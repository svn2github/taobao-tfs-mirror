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

#include "message/async_client.h"
#include "ns_define.h"
#include "meta_manager.h"
#include "replicate.h"
#include "compact.h"
#include "proactor_data_pipe.h"
#include "heart_manager.h"
#include "scanner_manager.h"
#include "redundant.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;

    // a timer class for itself
    class OwnerCheckTimerTask: public tbutil::TimerTask
    {
    public:
      OwnerCheckTimerTask(NameServer* fsnm);
      virtual ~OwnerCheckTimerTask();
      virtual void runTimerTask();
    private:
      DISALLOW_COPY_AND_ASSIGN( OwnerCheckTimerTask);
      NameServer* fs_name_system_;
      int main_task_queue_size_;

      tbutil::Time max_owner_check_time_;
      tbutil::Time owner_check_time_;
    };
    typedef tbutil::Handle<OwnerCheckTimerTask> OwnerCheckTimerTaskPtr;

    // the main class for run timer, start threads and handle packets.
    class NameServer: public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler, public tbsys::Runnable
    {
    public:
      NameServer();
      virtual ~NameServer();
      int start();
      int stop();
      int wait();

    public:
      // interface implementations
      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
      bool handlePacketQueue(tbnet::Packet *packet, void *args);

      static int rm_block_from_ds(const uint64_t server_id, const uint32_t block_id);
      static int rm_block_from_ds(const uint64_t server_id, const vector<uint32_t>& block_ids);

      MetaManager* get_meta_mgr()
      {
        return &meta_mgr_;
      }
      NsRuntimeGlobalInformation* get_ns_global_info()
      {
        return &ns_global_info_;
      }
      const tbutil::TimerPtr& get_timer() const
      {
        return timer_;
      }
      tbnet::PacketQueueThread* get_packet_queue_thread()
      {
        return &main_task_queue_thread_;
      }

    private:
      MetaManager meta_mgr_;
      NsRuntimeGlobalInformation ns_global_info_;
      tbutil::TimerPtr timer_;
      MasterHeartTimerTaskPtr master_heart_task_;
      SlaveHeartTimerTaskPtr slave_heart_task_;
      OwnerCheckTimerTaskPtr owner_check_task_;
      CheckOwnerIsMasterTimerTaskPtr check_owner_is_master_task_;
      MasterAndSlaveHeartManager master_slave_heart_mgr_;

      message::TfsPacketStreamer streamer_;
      tbnet::Transport transport_;
      message::MessageFactory msg_factory_;

      int32_t main_task_queue_size_;

      // threads
      tbsys::CThread check_ds_thread_; // check dataserver heartbeat
      tbsys::CThread check_block_thread_; // check replication count
      tbsys::CThread balance_thread_; // blocks balance
      tbsys::CThread time_out_thread_; // check timeout task
      ReplicateLauncher replicate_thread_; // replicate blocks thread
      CompactLauncher compact_thread_; // compact blocks thread
      RedundantLauncher redundant_thread_;
      tbnet::PacketQueueThread main_task_queue_thread_;
      HeartManagement heart_mgr_;
      ScannerManager pass_mgr_;

      // thread runner functions
      void run(tbsys::CThread *thread, void *arg);
      int do_check_ds();
      int do_check_blocks();
      int do_time_out();
      int do_balance();

      // message handler functions
      int get_block_info(message::Message* msg);
      int batch_get_block_info(message::Message* msg);
      int write_complete(message::Message* msg);
      int update_block_info(message::Message* msg);
      int do_control_cmd(message::Message* msg);
      int get_ds_status(message::Message* msg);
      int get_replicate_info_msg(message::Message* msg);
      int set_runtime_param(const uint32_t index, uint64_t value, char *retstr);
      int owner_check(message::Message* msg);
      int get_block_list(message::Message* msg);

      int check_ds(const time_t now);

    private:
      DISALLOW_COPY_AND_ASSIGN( NameServer);
      int initialize_ns_global_info();
      int get_peer_role();
      void initialize_handle_threads();
      void initialize_handle_task_and_heart_threads();
      int wait_for_ds_report();

    private:
      static const int8_t BATCH_GET_BLOCK_NUM_MAX;
    };

  }
} // end namespace tfs::nameserver
#endif
