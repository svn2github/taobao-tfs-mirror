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
#include "common/file_queue.h"
#include "common/file_queue_thread.h"

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    class FlushOpLogTimerTask: public tbutil::TimerTask
    {
    public:
      FlushOpLogTimerTask(LayoutManager& mm);
      virtual void runTimerTask();
    private:
      DISALLOW_COPY_AND_ASSIGN( FlushOpLogTimerTask);
      LayoutManager& meta_mgr_;
    };
    typedef tbutil::Handle<FlushOpLogTimerTask> FlushOpLogTimerTaskPtr;

    class OpLogSyncManager: public tbnet::IPacketQueueHandler
    {
      friend class FlushOpLogTimerTask;
    public:
      OpLogSyncManager(LayoutManager& mm);
      virtual ~OpLogSyncManager();
      int initialize();
      int wait_for_shut_down();
      int destroy();
      int register_slots(const char* const data, const int64_t length);
      void notify_all();
      void rotate();
      int flush_oplog();
      int log(uint8_t type, const char* const data, const int64_t length);
      int push(common::BasePacket* msg, int32_t max_queue_size = 0, bool block = false);
    public:
      common::FileQueueThread* get_file_queue_thread() const
      {
        return file_queue_thread_;
      }
      int replay_helper(const char* const data, int64_t& data_len, int64_t& pos, time_t now = time(NULL));
      int replay_helper_do_msg(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos);
      int replay_helper_do_oplog(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos, time_t now);
    private:
      DISALLOW_COPY_AND_ASSIGN( OpLogSyncManager);
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);
      static int do_sync_oplog(const void* const data, const int64_t len, const int32_t threadIndex, void *arg);
      int do_sync_oplog(const char* const data, const int64_t length);
      int execute(const common::BasePacket *msg, const void* args);
      int do_master_msg(const common::BasePacket* msg, const void* args);
      int do_slave_msg(const common::BasePacket* msg, const void* args);
      int do_sync_oplog(const common::BasePacket* msg, const void* args);
      int replay_all();
    private:
      bool is_destroy_;
      LayoutManager& meta_mgr_;
      OpLog* oplog_;
      common::FileQueue* file_queue_;
      common::FileQueueThread* file_queue_thread_;
      tbutil::Mutex mutex_;
      tbutil::Monitor<tbutil::Mutex> monitor_;
      tbnet::PacketQueueThread work_thread_;
    };
  }//end namespace nameserver
}//end namespace tfs
#endif
