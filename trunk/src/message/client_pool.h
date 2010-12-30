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
 *
 */
#ifndef TFS_MESSAGE_CLIENT_MANAGER_H_
#define TFS_MESSAGE_CLIENT_MANAGER_H_

#include <tbsys.h>
#include <tbnet.h>
#include "message.h"
#include "message_factory.h"
#include "tfs_packet_streamer.h"
#include "async_client.h"
#include "client.h"

typedef __gnu_cxx::hash_map<int32_t, tfs::message::Callee*> CLIENTS_MAP;
namespace tfs
{
  namespace message
  {
    class ClientManager: public tbnet::IPacketHandler
    {
      public:
        ClientManager();
        ~ClientManager();

        void init();
        void init_with_transport(tbnet::Transport* transport);

        // IPacketHandler
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);
        AsyncClient* get_async_client(const common::VUINT64& ds, AsyncCallback* cb);
        SimpleAsyncClient* get_simple_async_client(const uint64_t server_id, SimpleAsyncCallback* cb);
        Client* get_client(const uint64_t server_id);
        void release_client(Callee* callee);

      public:
        MessageFactory factory_;
        TfsPacketStreamer streamer_;
        tbnet::Transport* transport_;
        tbnet::ConnectionManager* connmgr_;

        int32_t max_timeout_;
        CLIENTS_MAP clients_;
        tbsys::CThreadMutex mutex_;
        bool init_;
        bool own_transport_;

        static ClientManager gClientManager;

      private:
        void store_client(Callee* callee);
    };
  }
}
#endif
