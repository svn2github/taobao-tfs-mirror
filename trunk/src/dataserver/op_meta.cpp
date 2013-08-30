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
 */

#include "tbsys.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/func.h"
#include "dataservice.h"
#include "op_meta.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    OpMeta::OpMeta(const OpId& oid, const VUINT64& servers):
      oid_(oid),
      file_size_(0),
      server_size_(0),
      done_server_size_(0),
      ref_count_(0)
    {
      start_time_ = Func::get_monotonic_time_us();
      last_update_time_ = Func::get_monotonic_time();
      set_members(servers);
   }

    OpMeta::~OpMeta()
    {
    }

    void OpMeta::set_members(const VUINT64& servers)
    {
      done_server_size_ = 0;
      server_size_ = servers.size();
      start_time_ = Func::get_monotonic_time_us();
      for (int32_t index = 0; index < server_size_; index++)
      {
        members_[index].server_ = servers[index];
        members_[index].info_.block_id_ = INVALID_BLOCK_ID;
        members_[index].info_.version_= INVALID_VERSION;
        members_[index].status_ = EXIT_TIMEOUT_ERROR;
      }
    }

    void OpMeta::reset_members()
    {
      done_server_size_ = 0;
      start_time_ = Func::get_monotonic_time_us();
      for (int32_t index = 0; index < server_size_; index++)
      {
        members_[index].info_.block_id_ = INVALID_BLOCK_ID;
        members_[index].info_.version_= INVALID_VERSION;
        members_[index].status_ = EXIT_TIMEOUT_ERROR;
      }
    }

    void OpMeta::update_member()
    {
      tbutil::Mutex::Lock lock(mutex_);
      done_server_size_++;
    }

    void OpMeta::update_member(const uint64_t server,
        const common::BlockInfoV2& info, const int32_t status)
    {
      tbutil::Mutex::Lock lock(mutex_);
      for (int index = 0; index < server_size_; index++)
      {
        if (server == members_[index].server_)
        {
          members_[index].info_ = info;
          members_[index].status_  = status;
          done_server_size_++;
          break;
        }
      }
    }

    int OpMeta::get_members(common::ArrayHelper< BlockReplicaInfo >& helper) const
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_SUCCESS;
      if (helper.get_array_size() < server_size_)
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        for (int index = 0; index < server_size_; index++)
        {
          helper.push_back(std::make_pair(members_[index].server_, members_[index].info_));
        }
      }
      return ret;
    }

    void OpMeta::get_servers(common::VUINT64& servers) const
    {
      servers.clear();
      for (int index = 0; index < server_size_; index++)
      {
        servers.push_back(members_[index].server_);
      }
    }

    bool OpMeta::check(OpStat& stat)
    {
      tbutil::Mutex::Lock lock(mutex_);
      bool all_finish = false;
      TBSYS_LOG(DEBUG, "op done server size: %d", done_server_size_);
      if (done_server_size_ >= server_size_)
      {
        all_finish = true;
        stat.status_ = TFS_SUCCESS; // default all success
        for (int index = 0; index < server_size_; index++)
        {
          if (TFS_SUCCESS != members_[index].status_)
          {
            stat.status_ = members_[index].status_;
          }

          if (EXIT_BLOCK_VERSION_CONFLICT_ERROR == members_[index].status_)
          {
            break; // found error conflict, break
          }
        }

        if (TFS_SUCCESS == stat.status_)
        {
          stat.size_ = file_size_;
          stat.cost_ = Func::get_monotonic_time_us() - start_time_;
        }
        else
        {
          strerror(stat.error_);
        }

        done_server_size_ = 0; // ensure only one thread can get all_finish true
      }
      return all_finish;
    }

    void OpMeta::strerror(std::stringstream& error)
    {
      for (int32_t index = 0; index < server_size_; index++)
      {
        error << " server: " << tbsys::CNetUtil::addrToString(members_[index].server_) <<
          " status: " << members_[index].status_;
      }
    }

    WriteOpMeta::WriteOpMeta(const OpId& oid, const common::VUINT64& servers):
      OpMeta(oid, servers),
      data_file_(oid.op_id_,
          dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir())
    {
    }

    WriteOpMeta::~WriteOpMeta()
    {
    }

  }/** end namespace dataserver**/
}/** end namespace tfs**/
