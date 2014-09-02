/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: kvrootserver.h  $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#ifndef TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_H_
#define TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_H_

#include <map>
#include <deque>
#include <vector>
#include "message/message_factory.h"
#include "common/internal.h"
#include "common/base_service.h"
#include "exp_server_manager.h"
#include "handle_task_helper.h"

namespace tfs
{
  namespace exprootserver
  {
    class ExpRootServer: public common::BaseService
    {
    public:
      ExpRootServer();
      virtual ~ExpRootServer();
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

    private:
      DISALLOW_COPY_AND_ASSIGN(ExpRootServer);

    protected:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      /** get pid file path */
      virtual const char* get_pid_file_path() { return NULL;}

    private:
      class KeepAliveIPacketQueueHandlerHelper : public tbnet::IPacketQueueHandler
      {
      public:
        explicit KeepAliveIPacketQueueHandlerHelper(ExpRootServer& manager): manager_(manager){ }
        virtual ~KeepAliveIPacketQueueHandlerHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHandlerHelper);
        ExpRootServer& manager_;
      };

    private:
      int rt_es_keepalive(common::BasePacket* packet);
      int32_t handle_finish_task(message::ReqFinishTaskFromEsMessage *msg);
      int32_t query_task(message::ReqQueryTaskMessage *msg);

    private:
      tbnet::PacketQueueThread rt_es_heartbeat_workers_;

      KeepAliveIPacketQueueHandlerHelper rt_es_heartbeat_handler_;
      ExpServerManager manager_;

      HandleTaskHelper handle_task_helper_;
      common::KvEngineHelper *kv_engine_helper_;
    };
  }/** exprootserver **/
}/** tfs **/
#endif
