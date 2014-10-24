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
#include "common/error_msg.h"
#include "ds_define.h"
#include "sync_manager.h"
#include "common/array_helper.h"
#include "common/client_manager.h"
#include "message/sync_file_entry_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace dataserver
  {
    SyncManager::SyncManager(const uint64_t dest_addr,const uint64_t self_addr, const uint64_t self_ns_addr, const int32_t limit, const float warn_ratio ) :
      dest_addr_(dest_addr),
      self_addr_(self_addr),
      self_ns_addr_(self_ns_addr),
      last_warn_time_(0),
      last_out_limit_time_(0),
      queue_size_(0),
      queue_limit_(limit),
      queue_warn_limit_(static_cast<int32_t>(static_cast<float>(limit) * warn_ratio))
    {

    }

    SyncManager::~SyncManager()
    {

    }

    int SyncManager::initialize()
    {
      work_thread_ = new (std::nothrow) WorkThreadHelper(*this);
      assert(work_thread_ != 0);
      return TFS_SUCCESS;
    }

    int SyncManager::destroy()
    {
      if (work_thread_ != 0)
      {
        work_thread_->join();
        work_thread_ = 0;
      }
      return TFS_SUCCESS;
    }

    int SyncManager::insert(const uint64_t dest_ns_addr, const int64_t app_id, const uint64_t block_id, const uint64_t file_id, const int32_t type)
    {
      int32_t ret = DsRuntimeGlobalInformation::instance().is_destroyed() ? EXIT_SERVICE_SHUTDOWN : TFS_SUCCESS;
      if (TFS_SUCCESS== ret)
      {
        int64_t now = Func::get_monotonic_time();
        warn_(now);
        ret = out_of_limit_() ? EXIT_QUEUE_FULL_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret && INVALID_SERVER_ID != dest_addr_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        ++queue_size_;
        SyncFileEntry entry;
        entry.app_id_   = app_id;
        entry.block_id_ = block_id;
        entry.file_id_  = file_id;
        entry.dest_ns_addr_ = dest_ns_addr;
        entry.type_     = type;
        entry.source_ds_addr_ = self_addr_;
        entry.source_ns_addr_ = self_ns_addr_;
        entry.sync_fail_count_ = 0;
        entry.last_sync_time_  = 0;
        queue_.push_back(entry);
      }
      return ret;
    }

    void SyncManager::do_sync_(const int32_t timeout_ms, const bool print)
    {
      SyncFileEntryMessage req_msg;
      common::ArrayHelper<SyncFileEntry> helper(MAX_SYNC_FILE_ENTRY_COUNT, req_msg.get_entry());
      mutex_.lock();
      QUEUE_CONST_ITER iter = queue_.begin();
      while (iter != queue_.end() && helper.get_array_index() < MAX_SYNC_FILE_ENTRY_COUNT)
      {
        helper.push_back((*iter));
      }
      mutex_.unlock();

      if (!helper.empty())
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        int32_t ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          tbnet::Packet* result = NULL;
          ret = send_msg_to_server(dest_addr_, client, &req_msg, result, timeout_ms);
          if (TFS_SUCCESS == ret)
          {
            ret = RSP_SYNC_FILE_ENTRY_MESSAGE == result->getPCode() ? TFS_SUCCESS : EXIT_SEND_SYNC_FILE_ENTRY_MSG_ERROR;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
        if (TFS_SUCCESS == ret)
        {
          tbutil::Mutex::Lock lock(mutex_);
          queue_size_ -= helper.get_array_index();
          for (int64_t index = 0; index < helper.get_array_index(); ++index)
          {
            queue_.pop_front();
          }
        }

        if (TFS_SUCCESS != ret && print)
        {
          mutex_.lock();
          queue_size_ -= helper.get_array_index();
          mutex_.unlock();
          for (int64_t index = 0; index < helper.get_array_index(); ++index)
          {
            SyncFileEntry* entry = helper.at(index);
            entry->dump(TBSYS_LOG_LEVEL(INFO), "sync file entry failed");
          }
        }
      }
    }

    bool SyncManager::out_of_limit_() const
    {
      return queue_size_ >= queue_limit_;
    }

    void SyncManager::run_()
    {
      int64_t now = 0;
      const int32_t MAX_TIMEOUT_MS = 1500;
      DsRuntimeGlobalInformation& rgi = DsRuntimeGlobalInformation::instance();
      while (!rgi.is_destroyed() && !queue_.empty())
      {
        do_sync_(MAX_TIMEOUT_MS, false);

        now = Func::get_monotonic_time();

        warn_(now);

        usleep(5000);
      }

      const int32_t OVER_TIMEOUT_MS = 500;
      int32_t index = (queue_size_ % MAX_SYNC_FILE_ENTRY_COUNT) + 1;
      while (index-- > 0)
      {
        do_sync_(OVER_TIMEOUT_MS, true);
      }
    }

    void SyncManager::warn_(const time_t now)
    {
      if ((queue_size_ > queue_warn_limit_ && now - last_warn_time_ > 60))//1 minute
      {
        TBSYS_LOG(WARN, "sync queue will be full, queue size: %d, queue warn limit: %d, queue limit: %d, now: %s, last_warn_time: %s",
            queue_size_, queue_warn_limit_, queue_limit_, tbutil::Time::seconds(now).toDateTime().c_str(),
            tbutil::Time::seconds(last_warn_time_).toDateTime().c_str());
        last_warn_time_ = now;
      }

      if (queue_size_ >= queue_limit_ && now - last_out_limit_time_ >= 60)
      {
        TBSYS_LOG(WARN, "sync queue is full, queue size: %d, queue warn limit: %d, queue limit: %d, now: %s, last_out_limit_time: %s",
            queue_size_, queue_warn_limit_, queue_limit_, tbutil::Time::seconds(now).toDateTime().c_str(),
            tbutil::Time::seconds(last_out_limit_time_).toDateTime().c_str());
        last_out_limit_time_ = now;
      }
    }

    void SyncManager::WorkThreadHelper::run()
    {
      manager_.run_();
    }
  }/** end namespace dataserver **/
}/** end namesapce tfs **/
