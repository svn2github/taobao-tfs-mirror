/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <Memory.hpp>

#include "common/parameter.h"
#include "common/directory_op.h"
#include "sync_base.h"
#include "dataservice.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace tfs::common;

    SyncBase::SyncBase(DataService& service, const int32_t type, const int32_t index,
                       const char* src_addr, const char* dest_addr) :
      service_(service), backup_type_(type), src_addr_(src_addr), dest_addr_(dest_addr),
      is_master_(false), stop_(0), pause_(0), need_sync_(0), need_sleep_(0),
      retry_count_(0), retry_time_(0), file_queue_(NULL), backup_(NULL) 
    {
      if (src_addr != NULL && 
          strlen(src_addr) > 0 &&
          dest_addr != NULL && 
          strlen(dest_addr) > 0)
      {
        mirror_dir_ = dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir() + "/mirror";
        uint64_t dest_ns_id = Func::get_host_ip(dest_addr); 
        char queue_name[20];
        // the 1st one is the master
        if (index == 0)
        {
          is_master_ = true;
          sprintf(queue_name, "firstqueue");  // for compactibility
        }
        else
        {
          sprintf(queue_name, "queue_%"PRI64_PREFIX"u", dest_ns_id);
        }
        file_queue_ = new FileQueue(mirror_dir_, queue_name);
        file_queue_->load_queue_head();
        file_queue_->initialize();

        if (type == SYNC_TO_TFS_MIRROR)
        {
          backup_ = new TfsMirrorBackup(*this, src_addr, dest_addr);
        }
        TBSYS_LOG(INFO, "backup type: %d, construct result: %d", type, backup_ ? 1 : 0);
      }
      else
        TBSYS_LOG(ERROR, "sync mirror src addr or dest addr null!");
    }

    SyncBase::~SyncBase()
    {
      tbsys::gDelete(file_queue_);
      tbsys::gDelete(backup_);
    }

    int SyncBase::init()
    {
      int32_t ret = (!src_addr_.empty() && !dest_addr_.empty()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        need_sync_ = backup_ ? backup_->init() : false;
        if (!need_sync_)
        {
          ret = TFS_ERROR;
        }
        else
        {
          // master need to recover second queue for compatibility
          if (is_master_)
          {
            ret = recover_second_queue();
          }
        }
      }
      return ret;
    }

    void SyncBase::stop()
    {
      stop_ = 1;
      sync_mirror_monitor_.lock();
      sync_mirror_monitor_.notifyAll();
      sync_mirror_monitor_.unlock();
      retry_wait_monitor_.lock();
      retry_wait_monitor_.notifyAll();
      retry_wait_monitor_.unlock();

      // wait for thread join
      if (NULL != backup_)
        backup_->destroy();
    }

    int SyncBase::recover_second_queue()
    {
      // 1.check if secondqueue exist
      std::string queue_path = mirror_dir_ + "/secondqueue";
      if (!DirectoryOp::is_directory(queue_path.c_str()))
      {
        return TFS_SUCCESS;
      }

      // 2.init FileQueue 
      FileQueue* second_file_queue = new FileQueue(mirror_dir_, "secondqueue");
      second_file_queue->load_queue_head();
      second_file_queue->initialize();

      // 3.move QueueItems from the 2nd queue to the 1st queue
      int32_t item_count = 0;
      while (!second_file_queue->empty())
      {
        QueueItem* item = NULL;
        item = second_file_queue->pop(0);
        if (NULL != item)
        {
          file_queue_->push(&(item->data_[0]), item->length_);
          free(item);
          item = NULL;
          item_count++;
        }
      }
      TBSYS_LOG(DEBUG, "recover %d queue items from the second file queue success", item_count);

      // 4.delete FileQueue
      tbsys::gDelete(second_file_queue);

      // 5.remove secondqueue directory
      return DirectoryOp::delete_directory_recursively(queue_path.c_str(), true) ? TFS_SUCCESS : TFS_ERROR;
    }

    int SyncBase::run_sync_mirror()
    {
      int ret = TFS_SUCCESS;
      sync_mirror_monitor_.lock();
      while (0 == need_sync_ && 0 == stop_)
      {
        sync_mirror_monitor_.wait();
      }
      sync_mirror_monitor_.unlock();
      if (stop_)
      {
        return ret;
      }

      sync_mirror_monitor_.lock();
      need_sleep_ = 1;
      while (!stop_)
      {
        while (0 == stop_ && (pause_ || file_queue_->empty()))
        {
          sync_mirror_monitor_.wait();
        }
        if (stop_)
        {
          break;
        }
        QueueItem* item = file_queue_->pop(0);
        sync_mirror_monitor_.unlock();
        if (NULL != item)
        {
          if (is_master_ && !service_.sync_mirror_.empty())
          {
            std::vector<SyncBase*>::const_iterator iter = service_.sync_mirror_.begin();
            // skip itself 
            iter++;
            for (; iter != service_.sync_mirror_.end(); iter++)
            {
              SyncData* data = reinterpret_cast<SyncData*>(&item->data_[0]);
              int8_t retry_count = 0;
              do {
                ret = (*iter)->write_sync_log(data->cmd_, data->block_id_, data->file_id_,
                        data->old_file_id_);
              }while (TFS_SUCCESS != ret && ++retry_count < 3);
            }
          }
          if (TFS_SUCCESS == do_sync(&item->data_[0], item->length_))
          {
            file_queue_->finish(0);
          }
          free(item);
        }
        if (need_sleep_)
        {
          usleep(20000);
        }
        sync_mirror_monitor_.lock();
        if (need_sleep_ && file_queue_->empty())
        {
          need_sleep_ = 0;
        }
      }
      sync_mirror_monitor_.unlock();

      return TFS_SUCCESS;
    }

    int SyncBase::write_sync_log(const int32_t cmd, const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id)
    {
      int32_t ret = block_id > 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (stop_ || 0 == need_sync_)
        {
          TBSYS_LOG(INFO, " no need write sync log, blockid: %u, fileid: %" PRI64_PREFIX "u, need sync: %d", block_id,
              file_id, need_sync_);
          return TFS_SUCCESS;
        }

        SyncData data;
        memset(&data, 0, sizeof(SyncData));
        data.cmd_ = cmd;
        data.block_id_ = block_id;
        data.file_id_ = file_id;
        data.old_file_id_ = old_file_id;
        data.retry_time_ = time(NULL);

        sync_mirror_monitor_.lock();
        ret = file_queue_->push(reinterpret_cast<void*>(&data), sizeof(SyncData));
        sync_mirror_monitor_.notify();
        sync_mirror_monitor_.unlock();

      }
      return ret;
    }

    int SyncBase::reset_log()
    {
      if (stop_)
      {
        return TFS_SUCCESS;
      }
      sync_mirror_monitor_.lock();
      file_queue_->clear();
      need_sync_ = 1;
      sync_mirror_monitor_.notifyAll();
      sync_mirror_monitor_.unlock();
      TBSYS_LOG(INFO, "sync log reset.");
      return TFS_SUCCESS;
    }

    int SyncBase::disable_log()
    {
      if (stop_)
      {
        return TFS_SUCCESS;
      }
      sync_mirror_monitor_.lock();
      file_queue_->clear();
      need_sync_ = 0;
      sync_mirror_monitor_.notifyAll();
      sync_mirror_monitor_.unlock();
      TBSYS_LOG(INFO, "sync log disable.");
      return TFS_SUCCESS;
    }

    void SyncBase::set_pause(const int32_t v)
    {
      if (stop_)
      {
        return;
      }
      sync_mirror_monitor_.lock();
      if (pause_ != v && 0 == v)
      {
        need_sleep_ = 1;
      }
      pause_ = v;
      sync_mirror_monitor_.notifyAll();
      sync_mirror_monitor_.unlock();
      TBSYS_LOG(INFO, "sync setpause: %d", pause_);
    }

    int SyncBase::do_sync(const char* data, const int32_t len)
    {
      if (NULL == data || len != sizeof(SyncData))
      {
        TBSYS_LOG(WARN, "SYNC_ERROR: data null or len error, %d <> %d", len, sizeof(SyncData));
        return TFS_ERROR;
      }
      SyncData* sf = reinterpret_cast<SyncData*>(const_cast<char*>(data));
      
      int ret = TFS_ERROR;
      // endless retry if fail
      do 
      {
#if defined(TFS_DS_GTEST)
        ret = backup_->do_sync(sf, src_block_file_.c_str(), dest_block_file_.c_str());
#else
        ret = backup_->do_sync(sf);
#endif
        if (TFS_SUCCESS != ret)
        {
          // for invalid block, not retry 
          if (EXIT_BLOCKID_ZERO_ERROR == ret)
            break;
          TBSYS_LOG(WARN, "sync error! cmd: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d, retry_count: %d",
              sf->cmd_, sf->block_id_, sf->file_id_, sf->old_file_id_, ret, retry_count_);
          retry_count_++;
          int32_t wait_second = retry_count_ * retry_count_; // 1, 4, 9, ...
          wait_second -= (time(NULL) - retry_time_);
          if (wait_second > 0)
          {
            retry_wait_monitor_.lock();
            retry_wait_monitor_.timedWait(tbutil::Time::seconds(wait_second));
            retry_wait_monitor_.unlock();
          }
          if (stop_)
          {
            return TFS_ERROR;
          }
          retry_time_ = time(NULL);
        }
      }while (TFS_SUCCESS != ret);

      retry_count_ = 0;
      retry_time_ = 0;

      return TFS_SUCCESS;
    }
  }
}
