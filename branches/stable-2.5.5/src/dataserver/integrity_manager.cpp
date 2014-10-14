/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 *
 */

#include "common/base_packet.h"
#include "common/file_opv2.h"
#include "message/message_factory.h"
#include "dataservice.h"
#include "integrity_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace message;
    using namespace std;

    IntegrityManager::IntegrityManager(DataService& service):
      service_(service)
    {
    }

    IntegrityManager::~IntegrityManager()
    {
    }

    inline BlockManager& IntegrityManager::get_block_manager()
    {
      return service_.get_block_manager();
    }

    inline TaskManager& IntegrityManager::get_task_manager()
    {
      return service_.get_task_manager();
    }

    inline DataHelper& IntegrityManager::get_data_helper()
    {
      return service_.get_data_helper();
    }

    bool IntegrityManager::check_one(const uint64_t block_id, const bool force)
    {
      bool checked = false;
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      int32_t now = time(NULL);
      int32_t last_check_time = 0;
      get_block_manager().get_last_check_time(last_check_time, block_id);
      if (force ||
          last_check_time + ds_info.check_integrity_interval_days_ * 86400 < now)
      {
        checked = true;
        TIMER_START();
        ret = get_data_helper().check_integrity(block_id);
        TIMER_END();
        if (TFS_SUCCESS == ret)
        {
          now = time(NULL);
          get_block_manager().set_last_check_time(now, block_id);
        }
        TBSYS_LOG(INFO, "check block %"PRI64_PREFIX"u integrity, cost: %ld, ret: %d",
            block_id, TIMER_DURATION(), ret);
      }
      return checked;
    }

    int IntegrityManager::get_dirty_expire_time()
    {
      int expire = 30;
      char buf[32];
      FileOperation op("/proc/sys/vm/dirty_expire_centisecs", O_RDONLY);
      int size = op.pread(buf, 32, 0);
      if (size > 0 && size < 32)  // has a valid value in file
      {
        buf[size] = '\0';
        expire = atoi(buf) / 100;
      }
      TBSYS_LOG(INFO, "dirty page expire time: %d seconds", expire);
      return expire;
    }

    void IntegrityManager::check_for_crash(const int32_t crash_time)
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      int32_t expire_time = get_dirty_expire_time();
      std::vector<uint64_t> blocks;
      std::vector<IndexHeaderV2> headers;
      get_block_manager().get_all_block_header(headers);

      std::vector<IndexHeaderV2>::iterator it = headers.begin();
      for ( ; it != headers.end(); it++)
      {
        if (it->throughput_.last_update_time_ + expire_time >= crash_time &&
            it->used_offset_ > 0)
        {
          blocks.push_back(it->info_.block_id_);
        }
      }

      std::vector<uint64_t>::iterator bit = blocks.begin();
      for ( ; bit != blocks.end() && !ds_info.is_destroyed(); bit++)
      {
        // force check these blocks, one per minute
        check_one(*bit, true);
        interruptable_usleep(60 * 1000000);
      }
    }

    void IntegrityManager::run_check()
    {
      if (service_.get_last_crash_time() > 0)
      {
        check_for_crash(service_.get_last_crash_time());
      }

      // ds usually has replicate or reinstate task after starup
      // so it begin check block after 30mins (time taken for upgrade a cluster)
      interruptable_usleep(30 * 60 * 1000000);

      int32_t MIN_SLEEP_TIME_US = 1000000;
      std::vector<uint64_t> blocks;
      SuperBlockInfo* info = NULL;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      get_block_manager().get_super_block_manager().get_super_block_info(info);
      assert(NULL != info);
      while (!ds_info.is_destroyed())
      {
        // check_integrity_interval_day: 0 means stop check
        while (ds_info.check_integrity_interval_days_ <= 0 && !ds_info.is_destroyed())
        {
          interruptable_usleep(MIN_SLEEP_TIME_US);
        }

        if (!ds_info.is_destroyed())
        {
          blocks.clear();
          get_block_manager().get_all_block_ids(blocks);
          std::vector<uint64_t>::iterator it = blocks.begin();
          for ( ; it != blocks.end() &&
              !ds_info.is_destroyed() &&
              ds_info.check_integrity_interval_days_ > 0; it++)
          {
            while (!should_check_now() && !ds_info.is_destroyed())
            {
              interruptable_usleep(MIN_SLEEP_TIME_US);
            }

            if (!ds_info.is_destroyed())
            {
              if(check_one(*it))
              {
                int64_t per_block_cost = static_cast<int64_t>(ds_info.check_integrity_interval_days_) * 86400 /
                  (info->used_main_block_count_ + 1);
                interruptable_usleep(per_block_cost * 1000000);
              }
              else
              {
                interruptable_usleep(MIN_SLEEP_TIME_US);
              }
            }
          }
        }
      }
    }

    bool IntegrityManager::should_check_now()
    {
      // TODO: limit check hour range
      return 0 == get_task_manager().size();
    }
  }
}
