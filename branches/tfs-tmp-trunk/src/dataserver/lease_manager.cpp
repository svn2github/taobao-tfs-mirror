/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lease_manager.cpp 2014 2012-08-16 15:41:45Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "tbsys.h"
#include "lease_manager.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "dataservice.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    Lease::Lease(const LeaseId& lease_id, const int64_t now, const VUINT64& servers):
      lease_id_(lease_id),
      last_update_time_(now),
      req_begin_time_(now),
      ref_count_(0),
      server_size_(0),
      done_server_size_(0)
    {
      int32_t index = 0;
      memset(members_, 0, sizeof(members_));
      server_size_ = servers.size();
      VUINT64::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter, ++index)
      {
        members_[index].server_ = (*iter);
        members_[index].info_.version_= INVALID_VERSION;
        members_[index].status_ = TFS_ERROR;
      }
    }

    void Lease::reset_member_status()
    {
      for (int index = 0; index < MAX_REPLICATION_NUM; index++)
      {
        if (members_[index].server_ == INVALID_SERVER_ID)
        {
          break;
        }
        members_[index].info_.version_= INVALID_VERSION;
        members_[index].status_ = TFS_ERROR;
      }
      done_server_size_ = 0;
    }

    int Lease::get_member_info(std::pair<uint64_t, common::BlockInfoV2>* members, int32_t& size) const
    {
      int ret = (NULL != members) ? TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = 0;
        for (index = 0; index <MAX_REPLICATION_NUM; ++index)
        {
          if (members_[index].server_ == INVALID_SERVER_ID)
          {
            break;
          }
          members[index].first = members_[index].server_;
          members[index].second = members_[index].info_;
        }
        size = index;
      }
      return ret;
    }

    int Lease::get_block_info(common::BlockInfoV2& info)
    {
      int ret = TFS_ERROR;
      int32_t max_version = -1;
      for (int32_t index = 0; index < MAX_REPLICATION_NUM; ++index)
      {
        if (members_[index].server_ == INVALID_SERVER_ID)
        {
          break;
        }

        if ((TFS_SUCCESS == members_[index].status_) && (members_[index].info_.version_ > max_version))
        {
          ret = TFS_SUCCESS; // at least one success
          max_version = members_[index].info_.version_;
          info = members_[index].info_;
        }
      }

      return ret;
    }

    int Lease::update_member_info(const uint64_t server, const common::BlockInfoV2& info, const int32_t status)
    {
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = EXIT_DATASERVER_NOT_FOUND;
        for (int32_t index = 0; index < MAX_REPLICATION_NUM && TFS_SUCCESS != ret; ++index)
        {
          ret = server == members_[index].server_ ? TFS_SUCCESS : EXIT_DATASERVER_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            members_[index].info_ = info;
            members_[index].status_  = status;

            tbutil::Mutex::Lock lock(mutex_);
            done_server_size_++;
          }
        }
      }
      return ret;
    }

    int Lease::update_member_info()
    {
      tbutil::Mutex::Lock lock(mutex_);
      done_server_size_++;
      return TFS_SUCCESS;
    }

    void Lease::reset_member_info()
    {
      for (int32_t index = 0; index < MAX_REPLICATION_NUM; ++index)
      {
        members_[index].info_.version_= INVALID_VERSION;
        members_[index].status_ = TFS_ERROR;
      }
    }

    void Lease::dump(const int32_t level, const char* const format)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        std::stringstream str;
        for (int32_t index = 0; index < MAX_REPLICATION_NUM ; ++index)
        {
          if (members_[index].server_ != INVALID_SERVER_ID)
            str << " server: " << tbsys::CNetUtil::addrToString(members_[index].server_) << " version: " << members_[index].info_.version_ << " status: " << members_[index].status_;
        }
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__, "%s lease id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u,block: %u, info: %s",
            format == NULL ? "" : format, lease_id_.lease_id_, lease_id_.file_id_, lease_id_.block_, str.str().c_str());
      }
    }

    void Lease::dump(std::stringstream& desp)
    {
      for (int32_t index = 0; index < MAX_REPLICATION_NUM ; ++index)
      {
        if (members_[index].server_ != INVALID_SERVER_ID)
        {
          desp << " server: " << tbsys::CNetUtil::addrToString(members_[index].server_) <<
                  " status: " << members_[index].status_;
        }
      }
    }

    bool Lease::check_all_finish()
    {
      tbutil::Mutex::Lock lock(mutex_);
      bool all_finish = (done_server_size_ >= server_size_);
      if (all_finish)
      {
        done_server_size_ = 0; // so only one thread get all_finish true
      }
      return all_finish;
    }

    bool Lease::check_all_successful() const
    {
      int32_t count = 0;
      bool all_successful = true ;
      for (int32_t index = 0; index < MAX_REPLICATION_NUM && all_successful; ++index)
      {
        if (INVALID_SERVER_ID != members_[index].server_)
        {
          ++count;
          all_successful = members_[index].status_ == TFS_SUCCESS;
        }
      }
      return all_successful && count > 0;
    }

    bool Lease::check_has_version_conflict() const
    {
      bool has_version_error = false;
      for (int32_t index = 0; index < MAX_REPLICATION_NUM && !has_version_error; ++index)
      {
        if (INVALID_SERVER_ID != members_[index].server_)
        {
          has_version_error = members_[index].status_ == EXIT_VERSION_CONFLICT_ERROR;
        }
      }
      return has_version_error;
    }

    WriteLease::WriteLease(const LeaseId& lease_id, const int64_t now, const VUINT64& servers):
      Lease(lease_id, now, servers)
    {
      data_file_ = new (std::nothrow) DataFile(lease_id.lease_id_,
          dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir());
      assert(NULL != data_file_);
    }

    WriteLease::~WriteLease()
    {
      tbsys::gDelete(data_file_);
    }

    LeaseManager::LeaseManager():
      base_lease_id_(0)
    {

    }
    LeaseManager::~LeaseManager()
    {
      LEASE_MAP_ITER iter = leases_.begin();
      for (; iter != leases_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
    }

    void LeaseManager::generation(LeaseId& lease_id, const int64_t now, const int8_t type, const VUINT64& servers)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      Lease* lease = NULL;
      if (INVALID_LEASE_ID == lease_id.lease_id_)
        lease_id.lease_id_ = atomic_inc(&base_lease_id_);
      if (LEASE_TYPE_UNLINK == type)
        lease = new Lease(lease_id, now, servers);
      else
        lease = new WriteLease(lease_id, now, servers);
      assert(lease);
      std::pair<LEASE_MAP_ITER, bool> res = leases_.insert(LEASE_MAP::value_type(lease->lease_id_, lease));
      if (!res.second)
        tbsys::gDelete(lease);
    }

    Lease* LeaseManager::get(const LeaseId& lease_id, const int64_t now) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      LEASE_MAP_CONST_ITER iter = leases_.find(lease_id);
      Lease* lease = (leases_.end() != iter && !iter->second->timeout(now)) ? iter->second : NULL;
      if (NULL != lease)
      {
        lease->inc_ref();
      }
      return lease;
    }

    void LeaseManager::put(Lease* lease)
    {
      int32_t ret = (NULL != lease) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        lease->dec_ref();
      }
    }

    int LeaseManager::remove(const LeaseId& lease_id)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      LEASE_MAP_ITER iter = leases_.find(lease_id);
      if (leases_.end() != iter)
      {
        if (iter->second->ref_count_ <= 0)
        {
          tbsys::gDelete(iter->second);
          leases_.erase(iter);
        }
      }
      return TFS_SUCCESS;
    }

    int64_t LeaseManager::size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return leases_.size();
    }

    int LeaseManager::timeout(const time_t now)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      uint32_t total = leases_.size();
      LEASE_MAP_ITER iter = leases_.begin();
      for (; iter != leases_.end(); )
      {
        if (iter->second->ref_count_ <= 0 && iter->second->timeout(now))
        {
          tbsys::gDelete(iter->second);
          leases_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      TBSYS_LOG(INFO, "cleanup lease, old size: %u, current size: %zd", total, leases_.size());
      return total - leases_.size();
    }

    bool LeaseManager::has_out_of_limit() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return leases_.size() >= static_cast<uint32_t>(SYSPARAM_DATASERVER.max_datafile_nums_);
    }

    uint64_t LeaseManager::gen_lease_id()
    {
      return atomic_inc(&base_lease_id_);
    }
  }/** end namespace dataserver**/
}/** end namespace tfs**/
