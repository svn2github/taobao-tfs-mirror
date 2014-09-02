/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_manager.cpp 746 2013-08-28 07:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <string>
#include "ss_define.h"
#include "sync_manager.h"
#include "common/error_msg.h"
#include "common/array_helper.h"
#include "common/client_manager.h"
#include "message/sync_file_entry_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace syncserver
  {
    int SyncManager::WorkThreadHelper::insert(const common::SyncFileEntry& entry)
    {
      int32_t ret = SsRuntimeGlobalInformation::instance().is_destroyed() ? EXIT_SERVICE_SHUTDOWN : TFS_SUCCESS;
      if (TFS_SUCCESS== ret)
      {
        int64_t now = Func::get_monotonic_time();
        warn_(now);
        ret = out_of_limit_() ? EXIT_QUEUE_FULL_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        tbutil::Mutex::Lock lock(mutex_);
        ++queue_size_;
        queue_.push_back(entry);
      }
      return ret;
    }

    void SyncManager::WorkThreadHelper::run()
    {
      int32_t ret = TFS_SUCCESS;
      time_t now  = 0;
      SsRuntimeGlobalInformation& srgi = SsRuntimeGlobalInformation::instance();
      while (!srgi.is_destroyed())
      {
        if (!queue_.empty())
        {
          mutex_.lock();
          SyncFileEntry entry = queue_.front();
          --queue_size_;
          queue_.pop_front();
          mutex_.unlock();
          now = Func::get_monotonic_time_ms();
          bool complete = entry.check_need_sync(now);
          if (complete)
          {
            ret = do_sync_(entry);
            complete = TFS_SUCCESS == ret;
          }

          if (!complete)
          {
            tbutil::Mutex::Lock lock(mutex_);
            ++queue_size_;
            queue_.push_back(entry);
          }
        }
        usleep(5000);
      }
    }

    bool SyncManager::WorkThreadHelper::out_of_limit_() const
    {
      return queue_size_ >= queue_limit_;
    }

    void SyncManager::WorkThreadHelper::warn_(const time_t now)
    {
      if ((queue_size_ > queue_warn_limit_ && now - last_warn_time_ > 60))//1 minute
      {
        TBSYS_LOG(WARN, "%s sync queue will be full, dest ns addr: %s, queue size: %d, queue warn limit: %d, queue limit: %d, now: %s, last_warn_time: %s",
            tbsys::CNetUtil::addrToString(manager_.get_source_ds_addr()).c_str(),
            tbsys::CNetUtil::addrToString(dest_ns_addr_).c_str(), queue_size_, queue_warn_limit_, queue_limit_, tbutil::Time::seconds(now).toDateTime().c_str(),
            tbutil::Time::seconds(last_warn_time_).toDateTime().c_str());
        last_warn_time_ = now;
      }

      if (queue_size_ >= queue_limit_ && now - last_out_limit_time_ > 60)
      {
        TBSYS_LOG(WARN, "%s sync queue is full, dest ns addr: %s, queue size: %d, queue warn limit: %d, queue limit: %d, now: %s, last_out_limit_time: %s",
            tbsys::CNetUtil::addrToString(manager_.get_source_ds_addr()).c_str(),
            tbsys::CNetUtil::addrToString(dest_ns_addr_).c_str(), queue_size_, queue_warn_limit_, queue_limit_, tbutil::Time::seconds(now).toDateTime().c_str(),
            tbutil::Time::seconds(last_out_limit_time_).toDateTime().c_str());
        last_out_limit_time_ = now;
      }
    }

    int SyncManager::WorkThreadHelper::do_sync_(common::SyncFileEntry& entry)
    {
      assert(entry.block_id_ != INVALID_BLOCK_ID);
      assert(entry.file_id_ != INVALID_FILE_ID);
      assert(entry.source_ds_addr_ != INVALID_SERVER_ID);
      int32_t ret = func_(buffer_, THREAD_BUFFER_SIZE, entry);
      if (TFS_SUCCESS != ret)
      {
        ++entry.sync_fail_count_;
        entry.last_sync_time_ = common::Func::get_monotonic_time();
      }
      return ret;
    }

    int SyncManager::WorkThreadHelperManager::initialize(const common::ArrayHelper<std::pair<uint64_t, sync_func> >& sync_mirror_info,
        const int32_t queue_limit, const int32_t queue_warn_limit)
    {
      int32_t ret = sync_mirror_info.get_array_index() <= MAX_SYNC_CLUSTER_SIZE ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        for (int64_t index = 0; index < sync_mirror_info.get_array_index(); ++index)
        {
          std::pair<uint64_t, sync_func>* item = sync_mirror_info.at(index);
          assert(NULL != item);
          work_thread_[index] = new (std::nothrow) WorkThreadHelper(*this, item->second, item->first, queue_limit, queue_warn_limit);
          assert(work_thread_[index] != 0);
          ++work_thread_count_;
        }
      }
      return ret;
    }

    int SyncManager::WorkThreadHelperManager::destroy()
    {
      for (int32_t index = 0; index < work_thread_count_; ++index)
      {
        if (0 != work_thread_[index])
        {
          work_thread_[index]->join();
          work_thread_[index] = 0;
        }
      }
      return TFS_SUCCESS;
    }

    int SyncManager::WorkThreadHelperManager::insert(common::SyncFileEntry& entry)
    {
      int32_t ret = SsRuntimeGlobalInformation::instance().is_destroyed() ? EXIT_SERVICE_SHUTDOWN : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        if (INVALID_SERVER_ID == entry.dest_ns_addr_)
        {
          for (int32_t index = 0; index < work_thread_count_; ++index)
          {
            entry.dest_ns_addr_ = work_thread_[index]->get_dest_ns_addr();
            work_thread_[index]->insert(entry);
          }
        }
        else
        {
          WorkThreadHelperPtr thread= 0;
          for (int32_t index = 0; index < work_thread_count_ && 0 == thread; ++index)
          {
            if (work_thread_[index]->get_dest_ns_addr() == entry.dest_ns_addr_)
              thread = work_thread_[index];
          }
          if (0 != thread)
          {
            ret = thread->insert(entry);
          }
        }
      }
      return ret;
    }

    SyncManager::SyncManager(const int32_t limit, const float warn_ratio) :
      queue_limit_(limit),
      queue_warn_limit_(static_cast<int32_t>(static_cast<float>(limit) * warn_ratio))
    {

    }

    SyncManager::~SyncManager()
    {

    }

    int SyncManager::initialize(const common::ArrayHelper<std::pair<uint64_t, int32_t> >& sync_mirror_info)
    {
      int32_t ret = sync_mirror_info.get_array_index() <= MAX_SYNC_CLUSTER_SIZE ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int64_t index = 0;
        std::pair<uint64_t, sync_func> mirror[sync_mirror_info.get_array_index()];
        common::ArrayHelper<std::pair<uint64_t, sync_func > > helper(sync_mirror_info.get_array_index(), mirror);
        for (index = 0; index < sync_mirror_info.get_array_index(); ++index)
        {
          std::pair<uint64_t, int32_t>* item = sync_mirror_info.at(index);
          assert(NULL != item);
          if (SYNC_CLIENT_VERSION_V0 == item->second)
          {
            helper.push_back(std::make_pair(item->first, SyncManager::do_sync_whith_client_v0));
          }
          if (SYNC_CLIENT_VERSION_V1 == item->second)
          {
            helper.push_back(std::make_pair(item->first, SyncManager::do_sync_whith_client_v1));
          }
        }
        for (index = 0; index < MAX_WORK_THREAD_SIZE && TFS_SUCCESS == ret; ++index)
        {
          work_thread_manager_[index] = new (std::nothrow) WorkThreadHelperManager(*this, 0);
          ret = work_thread_manager_[index]->initialize(helper, queue_limit_, queue_warn_limit_);
          assert(work_thread_manager_[index] != 0);
        }
      }
      return ret;
    }

    int SyncManager::destroy()
    {
      for (int32_t index = 0; index < MAX_WORK_THREAD_SIZE; ++index)
      {
        if (work_thread_manager_[index]!= 0)
        {
          work_thread_manager_[index]->destroy();
          tbsys::gDelete(work_thread_manager_[index]);
        }
      }
      return TFS_SUCCESS;
    }

    int SyncManager::insert(common::SyncFileEntry& entry)
    {
      int32_t ret = SsRuntimeGlobalInformation::instance().is_destroyed() ? EXIT_SERVICE_SHUTDOWN : TFS_SUCCESS;
      if (TFS_SUCCESS== ret)
      {
        mutex_.lock();
        int32_t free_index = -1;
        WorkThreadHelperManager* manager = NULL;
        for (int32_t index = 0; index < MAX_WORK_THREAD_SIZE && NULL == manager; ++index)
        {
          if (work_thread_manager_[index]->get_source_ds_addr() == entry.source_ds_addr_)
          {
            manager = work_thread_manager_[index];
          }
          else if (work_thread_manager_[index]->get_source_ds_addr() == INVALID_SERVER_ID)
          {
            free_index = index;
          }
        }
        ret = (NULL == manager && free_index < 0) ? EXIT_SOURCE_DS_SYNC_THREAD_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (NULL == manager)
            manager = work_thread_manager_[free_index];
          assert(0 != manager);
          if (manager->get_source_ds_addr() == INVALID_SERVER_ID)
            manager->set_source_ds_addr(entry.source_ds_addr_);
        }
        mutex_.unlock();
        entry.sync_fail_count_ = 0;
        entry.last_sync_time_  = 0;
        ret = manager->insert(entry);
      }
      return ret;
    }

    int SyncManager::do_sync_whith_client_v0(char* buf, const int32_t length, const common::SyncFileEntry& entry)
    {
      UNUSED(entry);
      int32_t ret = (NULL != buf && length > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //TODO
      }
      return ret;
    }

    int SyncManager::do_sync_whith_client_v1(char* buf, const int32_t length, const common::SyncFileEntry& entry)
    {
      UNUSED(entry);
      int32_t ret = (NULL != buf && length > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //TODO
      }
      return ret;
    }
  }/** end namespace syncserver **/
}/** end namesapce tfs **/
