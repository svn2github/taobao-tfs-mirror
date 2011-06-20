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
#include "sync_base.h"
#include "dataservice.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace tfs::common;

    SyncBase::SyncBase(const int32_t type) :
      stop_(0), pause_(0), need_sync_(0), need_sleep_(0),
      file_queue_(NULL), second_file_queue_(NULL), second_file_queue_thread_(NULL), backup_(NULL)
    {
      mirror_dir_ = dynamic_cast<DataService*>(DataService::instance())->get_real_work_dir() + "/mirror";
      file_queue_ = new FileQueue(mirror_dir_.c_str(), "firstqueue");
      file_queue_->load_queue_head();
      file_queue_->initialize();

      if (type == SYNC_TO_TFS_MIRROR)
      {
        backup_ = new TfsMirrorBackup();
      }
      else if (type == SYNC_TO_NFS_MIRROR)
      {
        backup_ = new NfsMirrorBackup();
      }
      TBSYS_LOG(INFO, "backup type: %d, construct result: %d", type, backup_ ? 1 : 0);
      need_sync_ = backup_ ? backup_->init() : false;
    }

    SyncBase::~SyncBase()
    {
      tbsys::gDelete(file_queue_);
      tbsys::gDelete(second_file_queue_thread_);
      tbsys::gDelete(second_file_queue_);
      tbsys::gDelete(backup_);
    }

    void SyncBase::stop()
    {
      stop_ = 1;
      sync_mirror_monitor_.lock();
      sync_mirror_monitor_.notifyAll();
      sync_mirror_monitor_.unlock();
      if (NULL != second_file_queue_thread_)
      {
        second_file_queue_thread_->destroy();
        second_file_queue_thread_->wait();
      }
    }

    int SyncBase::do_second_sync(const void* data, const int64_t len, const int32_t, void* args)
    {
      SyncBase* rt = reinterpret_cast<SyncBase*>(args);
      return rt->do_sync(reinterpret_cast<const char*>(data), len, true);
    }

    int SyncBase::run_sync_mirror()
    {
      sync_mirror_monitor_.lock();
      while (0 == need_sync_ && 0 == stop_)
      {
        sync_mirror_monitor_.wait();
      }
      sync_mirror_monitor_.unlock();
      if (stop_)
      {
        return TFS_SUCCESS;
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
          if (TFS_SUCCESS == do_sync(&item->data_[0], item->length_, false))
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
      int ret = file_queue_->push(reinterpret_cast<void*>(&data), sizeof(SyncData));
      sync_mirror_monitor_.notify();
      sync_mirror_monitor_.unlock();

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

    int SyncBase::do_sync(const char* data, const int32_t len, const bool second)
    {
      if (len != sizeof(SyncData))
      {
        TBSYS_LOG(WARN, "SYNC_ERROR: len error, %d <> %d", len, sizeof(SyncData));
        return TFS_ERROR;
      }
      SyncData* sf = reinterpret_cast<SyncData*>(const_cast<char*>(data));
      if (sf->retry_count_)
      {
        int32_t wait_second = sf->retry_count_ * sf->retry_count_ * 10; // 10, 40, 90
        wait_second -= (time(NULL) - sf->retry_time_);
        if (wait_second > 0)
        {
          sleep(wait_second);
        }
        if (stop_)
        {
          return TFS_ERROR;
        }
      }
      int ret = TFS_ERROR;
      if (!second)
      {
        ret = backup_->do_sync(sf);
      }
      else
      {
        ret = backup_->do_second_sync(sf);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "sync error! cmd: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, old fileid: %" PRI64_PREFIX "u, ret: %d, sf->retry_count: %d",
            sf->cmd_, sf->block_id_, sf->file_id_, sf->old_file_id_, ret, sf->retry_count_);
      }

      if (TFS_SUCCESS != ret && sf->retry_count_ <= 3)
      {
        if (NULL == second_file_queue_ && NULL == second_file_queue_thread_)
        {
          second_file_queue_ = new FileQueue(mirror_dir_.c_str(), "secondqueue");
          second_file_queue_->load_queue_head();
          second_file_queue_->initialize();
          second_file_queue_thread_ = new FileQueueThread(second_file_queue_, this);
          second_file_queue_thread_->initialize(1, SyncBase::do_second_sync);
        }
        ++sf->retry_count_;
        sf->retry_time_ = time(NULL);
        second_file_queue_thread_->write(data, len);
      }

      return TFS_SUCCESS;
    }

    int SyncBase::reload_slave_ip()
    {
      sync_mirror_monitor_.lock();
      need_sync_ = backup_->init();
      sync_mirror_monitor_.unlock();
      TBSYS_LOG(INFO, "set slave ip: %s", (SYSPARAM_DATASERVER.slave_ns_ip_ ? SYSPARAM_DATASERVER.slave_ns_ip_ : "none"));

      return TFS_SUCCESS;
    }

  }
}
