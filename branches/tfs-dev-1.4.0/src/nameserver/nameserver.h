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

#include "ns_define.h"
#include "common/internal.h"
#include "common/base_service.h"
#include "layout_manager.h"
#include "heart_manager.h"

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

    class NameServer: public common::BaseService
    {
    public:
      NameServer();
      virtual ~NameServer();
      /** initialize application data*/
      virtual int initialize(int argc, char* argv[]);

      /** destroy application data*/
      virtual int destroy_service();

      /** create the packet streamer, this is used to create packet according to packet code */
      virtual tbnet::IPacketStreamer* create_packet_streamer()
      {
        return new common::BasePacketStreamer();
      }

      /** destroy the packet streamer*/
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
      {
        tbsys::gDelete(streamer);
      }

      /** create the packet streamer, this is used to create packet*/
      virtual common::BasePacketFactory* create_packet_factory()
      {
        return new message::MessageFactory();
      }

      /** destroy packet factory*/
      virtual void destroy_packet_factory(common::BasePacketFactory* factory)
      {
        tbsys::gDelete(factory);
      }

      /** handle single packet */
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      /** handle packet*/
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

      int callback(common::NewClient* client);

   private:
      DISALLOW_COPY_AND_ASSIGN(NameServer);
      LayoutManager meta_mgr_;
      MasterHeartTimerTaskPtr master_heart_task_;
      SlaveHeartTimerTaskPtr slave_heart_task_;
      OwnerCheckTimerTaskPtr owner_check_task_;
      CheckOwnerIsMasterTimerTaskPtr check_owner_is_master_task_;
      MasterAndSlaveHeartManager master_slave_heart_mgr_;
      HeartManagement heart_mgr_;

    private:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      int open(common::BasePacket* msg);
      int close(common::BasePacket* msg);
      int batch_open(common::BasePacket* msg);
      int update_block_info(common::BasePacket* msg);
      int show_server_information(common::BasePacket* msg);
      int owner_check(common::BasePacket* msg);
      int ping(common::BasePacket* msg);
      int do_master_msg_helper(common::BasePacket* packet);
      int do_slave_msg_helper(common::BasePacket* packet);

      int initialize_ns_global_info();
      int get_peer_role();
      int wait_for_ds_report();
    };
  }/** nameserver **/
}/** tfs **/
#endif
