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

    void IntegrityManager::run_check()
    {
      // ds usually has replicate or reinstate task after starup
      // so it begin check block after 30mins (time taken for upgrade a cluster)
      interruptable_usleep(30 * 60 * 1000000);

      int ret = TFS_SUCCESS;
      int32_t MIN_SLEEP_TIME_US = 1000000;
      std::vector<uint64_t> blocks;
      SuperBlockInfo* info = NULL;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      get_block_manager().get_super_block_manager().get_super_block_info(info);
      assert(NULL != info);
      while (!ds_info.is_destroyed())
      {
        // check_integrity_interval_day: 0 means stop check
        while (!ds_info.check_integrity_interval_days_ && !ds_info.is_destroyed())
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
              ds_info.check_integrity_interval_days_; it++)
          {
            // TODO, verify check block's raw data
            if (IS_VERFIFY_BLOCK(*it))
            {
              continue;
            }

            while (!should_check_now() && !ds_info.is_destroyed())
            {
              interruptable_usleep(MIN_SLEEP_TIME_US);
            }

            if (!ds_info.is_destroyed())
            {
              int32_t now = time(NULL);
              int32_t last_check_time = 0;
              get_block_manager().get_last_check_time(last_check_time, *it);
              if (last_check_time +
                  ds_info.check_integrity_interval_days_ * 86400 < now)
              {
                ret = get_data_helper().check_integrity(*it);
                if (TFS_SUCCESS == ret)
                {
                  now = time(NULL);
                  get_block_manager().set_last_check_time(now, *it);
                }
                else if (EXIT_NO_LOGICBLOCK_ERROR == ret)
                {
                  ret = TFS_SUCCESS;
                }
                //TODO: assert(EXIT_CHECK_CRC_ERROR != ret);
                TBSYS_LOG_DW(ret, "check block %"PRI64_PREFIX"u integrity, ret: %d", *it, ret);
                int32_t per_block_cost =
                  ds_info.check_integrity_interval_days_ * 86400 / info->total_main_block_count_;
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
