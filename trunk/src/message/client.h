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
#ifndef TFS_MESSAGE_CLIENT_H_
#define TFS_MESSAGE_CLIENT_H_

#include <tbsys.h>
#include "message.h"
#include "async_client.h"

namespace tfs
{
  namespace message
  {
    class Client: public Callee
    {
      public:
        Client(const uint64_t ip);
        ~Client();
        virtual int connect();
        virtual int disconnect();
        virtual Message* call(Message* message);
        virtual int handlePacket(tbnet::Packet* packet, void* args);
        // server ip
        inline uint64_t get_mip()
        {
          return ip_;
        }

      private:
        uint64_t ip_;
        Message* ret_msg_;
        tbsys::CThreadCond cond_;
    };

    int send_message_to_server(const uint64_t server_id, Message* ds_message, Message** ret_message);
    int post_message_to_server(const uint64_t server_id, Message* message);
    int test_server_alive(const uint64_t server_id);
  }
}
#endif
