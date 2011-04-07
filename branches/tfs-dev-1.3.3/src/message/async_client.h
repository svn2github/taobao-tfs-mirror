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
#ifndef TFS_MESSAGE_ASYNC_CLIENT_H_
#define TFS_MESSAGE_ASYNC_CLIENT_H_

#include <tbsys.h>
#include <tbnet.h>

#include "message.h"
#include "tfs_packet_streamer.h"

namespace tfs
{
  namespace message
  {
    class Callee
    {
      public:
        enum CallStatus
        {
          CALL_FINISHED = 1,
          CALL_UNFINISH
        };

      public:
        Callee();
        virtual ~Callee();

      public:
        virtual int connect() = 0;
        virtual int disconnect() = 0;
        virtual int32_t get_call_id();
        virtual Message* call(Message* message) = 0;
        virtual int32_t handlePacket(tbnet::Packet* packet, void* args) = 0;

      protected:
        int32_t call_id_;
      private:
        static atomic_t global_call_id_;
    };

    class AsyncCallback
    {
      public:
        virtual ~AsyncCallback()
        {
        }
        virtual int32_t command_done(Message* send_message, bool status, const std::string& error) = 0;
    };

    class DefaultAsyncCallback: public AsyncCallback
    {
      public:
        virtual ~DefaultAsyncCallback()
        {
        }
        virtual int32_t command_done(Message* send_message, bool status, const std::string& error);
    };

    class AsyncClient: public Callee
    {
      public:
        AsyncClient(const common::VUINT64& ds, AsyncCallback* cb);
        virtual ~AsyncClient();

      public:
        virtual int connect();
        virtual int disconnect();
        virtual Message* call(Message* message);
        virtual int32_t handlePacket(tbnet::Packet* packet, void* args);

        inline bool all_message_posted()
        {
          return send_success_count_ >= ds_list_.size();
        }
        inline uint32_t send_success_count() const
        {
          return send_success_count_;
        }
        int32_t post(const Message* message);

      protected:
        bool save_message(const Message* message);
        static const int32_t WRITE_WAIT_TIME = 2000;

      protected:
        uint32_t handle_response_count_;
        uint32_t handle_success_count_;
        uint32_t send_success_count_;
        bool call_over_;

        common::VUINT64 ds_list_;
        // save message info
        Message* send_message_;
        AsyncCallback* callback_;
        tbsys::CThreadMutex mutex_;

    };

    class SimpleAsyncCallback
    {
      public:
        virtual int command_done(Message* send_message, Message* ret_message) = 0;
        virtual ~SimpleAsyncCallback()
        {
        }
    };

    class SimpleAsyncClient: public AsyncClient
    {
      public:
        SimpleAsyncClient(const common::VUINT64& ds, SimpleAsyncCallback* cb);
        virtual ~SimpleAsyncClient();
      public:
        virtual int32_t handlePacket(tbnet::Packet* packet, void* args);
      protected:
        SimpleAsyncCallback *simple_callback_;
    };

    int async_post_message_to_servers(const Message* message, common::VUINT64& ds_list, AsyncCallback* cb);
    int simple_async_post_message_to_server(const Message* message, const uint64_t server_id, SimpleAsyncCallback* cb);
  }
}
#endif
