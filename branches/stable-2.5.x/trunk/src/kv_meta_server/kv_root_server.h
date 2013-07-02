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
#ifndef TFS_KVROOTSERVER_NAMESERVER_H_
#define TFS_KVROOTSERVER_NAMESERVER_H_

#include "message/message_factory.h"
#include "common/internal.h"
#include "common/base_service.h"

#include "kv_meta_server_manager.h"

namespace tfs
{
  namespace kvrootserver
  {
    class KvRootServer: public common::BaseService
    {
    public:
      KvRootServer();
      virtual ~KvRootServer();
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
      DISALLOW_COPY_AND_ASSIGN(KvRootServer);

    protected:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      /** get pid file path */
      virtual const char* get_pid_file_path() { return NULL;}

    private:
      class KeepAliveIPacketQueueHandlerHelper : public tbnet::IPacketQueueHandler
      {
      public:
        explicit KeepAliveIPacketQueueHandlerHelper(KvRootServer& manager): manager_(manager){ }
        virtual ~KeepAliveIPacketQueueHandlerHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHandlerHelper);
        KvRootServer& manager_;
      };

    private:
      int kv_ms_keepalive(common::BasePacket* packet);
      int get_tables(common::BasePacket* packet);

    private:
      tbnet::PacketQueueThread ms_rs_heartbeat_workers_;

      KeepAliveIPacketQueueHandlerHelper rt_ms_heartbeat_handler_;
      KvMetaServerManager manager_;
    };
  }/** kvrootserver **/
}/** tfs **/
#endif
