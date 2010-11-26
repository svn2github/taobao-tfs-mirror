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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_OPERATION_LOG_SYNC_MANAGER_H_
#define TFS_NAMESERVER_OPERATION_LOG_SYNC_MANAGER_H_

#include <deque>
#include <map>
#include <Mutex.h>
#include <Monitor.h>
#include <Timer.h>
#include "oplog.h"
#include "message/message_factory.h"
#include "file_system_image.h"
#include "proactor_data_pipe.h"
#include "common/file_queue.h"
#include "common/file_queue_thread.h"

namespace tfs
{
  namespace nameserver
  {
    class MetaManager;
    class FlushOpLogTimerTask: public tbutil::TimerTask
    {
    public:
      FlushOpLogTimerTask(MetaManager* mm);
      virtual void runTimerTask();
    private:
      DISALLOW_COPY_AND_ASSIGN( FlushOpLogTimerTask);
      MetaManager* meta_mgr_;
    };
    typedef tbutil::Handle<FlushOpLogTimerTask> FlushOpLogTimerTaskPtr;

    class OpLogSyncManager: public ProactorDataPipe<std::deque<message::Message*>, OpLogSyncManager>
    {
      friend class FlushOpLogTimerTask;
      template<typename Container, typename Executor> friend class ProactorDataPipe;
    public:
      OpLogSyncManager(MetaManager* mm);
      virtual ~OpLogSyncManager();
      int initialize(LayoutManager& blkMaps);
      int wait_for_shut_down();
      int destroy();
      int register_slots(const char* const data, const int64_t length);
      int register_msg(const message::Message* msg);
      void notify_all();
      int rotate();
      int flush_oplog();
      int log(const common::BlockInfo* const block, int32_t type, const common::VUINT64& dsList);
      static std::string printDsList(const common::VUINT64& dsList);//only debug
    public:
      const FileSystemImage* get_ns_fs_image() const
      {
        return &ns_fs_image_;
      }

      FileSystemImage* get_ns_fs_image()
      {
        return &ns_fs_image_;
      }

      common::FileQueueThread* get_file_queue_thread() const
      {
        return file_queue_thread_;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN( OpLogSyncManager);
      static int do_sync_oplog(const void* const data, const int64_t len, const int32_t threadIndex, void *arg);
      int do_sync_oplog(const char* const data, const int64_t length);
      int execute(const message::Message *msg, const void* args);
      int do_master_msg(const message::Message* msg, const void* args);
      int do_slave_msg(const message::Message* msg, const void* args);
      int do_sync_oplog(const message::Message* msg, const void* args);
    private:
      bool is_destroy_;
      MetaManager* meta_mgr_;
      FileSystemImage ns_fs_image_;
      OpLog* oplog_;
      common::FileQueue* file_queue_;
      common::FileQueueThread* file_queue_thread_;
      tbutil::Mutex mutex_;
      tbutil::Monitor<tbutil::Mutex> monitor_;
    };
  }//end namespace nameserver
}//end namespace tfs
#endif
