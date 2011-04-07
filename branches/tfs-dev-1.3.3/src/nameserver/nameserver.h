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
#ifndef TFS_NAMESERVER_NAMESERVER_H_
#define TFS_NAMESERVER_NAMESERVER_H_

#include <string>
#include <ext/hash_map>
#include <stdarg.h>
#include <deque>
#include <Mutex.h>
#include <Monitor.h>
#include <Timer.h>
#include <Handle.h>

#include "ns_define.h"
#include "layout_manager.h"
#include "heart_manager.h"
#include "message/tfs_packet_streamer.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    class OwnerCheckTimerTask: public tbutil::TimerTask
    {
    public:
      OwnerCheckTimerTask(NameServer* server);
      virtual ~OwnerCheckTimerTask();
      virtual void runTimerTask();
    private:
      DISALLOW_COPY_AND_ASSIGN( OwnerCheckTimerTask);
      NameServer* server_;
      const int64_t MAX_LOOP_TIME;
      int64_t max_owner_check_time_;
      int64_t owner_check_time_;
      int32_t main_task_queue_size_;
    };

    typedef tbutil::Handle<OwnerCheckTimerTask> OwnerCheckTimerTaskPtr;

    class NameServer: public tbnet::IServerAdapter,
                      public tbnet::IPacketQueueHandler
    {
    public:
      NameServer();
      virtual ~NameServer();
      int start();
      int stop();
      void wait();
      const tbutil::TimerPtr& get_timer() const
      {
        return timer_;
      }
      tbnet::PacketQueueThread* get_packet_queue_thread()
      {
        return &main_task_queue_thread_;
      }

      // interface implementations
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      bool handlePacketQueue(tbnet::Packet *packet, void *args);

   private:
      DISALLOW_COPY_AND_ASSIGN(NameServer);
      LayoutManager meta_mgr_;
      tbutil::TimerPtr timer_;
      tbutil::TimerPtr owner_check_timer_;
      MasterHeartTimerTaskPtr master_heart_task_;
      SlaveHeartTimerTaskPtr slave_heart_task_;
      OwnerCheckTimerTaskPtr owner_check_task_;
      CheckOwnerIsMasterTimerTaskPtr check_owner_is_master_task_;
      MasterAndSlaveHeartManager master_slave_heart_mgr_;
      HeartManagement heart_mgr_;

      message::TfsPacketStreamer streamer_;
      tbnet::Transport transport_;
      message::MessageFactory msg_factory_;
      tbnet::PacketQueueThread main_task_queue_thread_;//main threads

      int32_t main_task_queue_size_;
    private:

      int open(message::Message* msg);
      int close(message::Message* msg);
      int batch_open(message::Message* msg);
      int update_block_info(message::Message* msg);
      int get_ds_status(message::Message* msg);
      int owner_check(message::Message* msg);

      int initialize_ns_global_info();
      int get_peer_role();
      void initialize_handle_task_and_heart_threads();
      int wait_for_ds_report();
    };
  }
} // end namespace tfs::nameserver
#endif
