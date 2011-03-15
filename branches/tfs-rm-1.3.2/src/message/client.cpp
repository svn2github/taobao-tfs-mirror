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
#include "client.h"
#include "client_pool.h"
#include "common/error_msg.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    Client::Client(const uint64_t ip) :
      ip_(ip), ret_msg_(NULL)
    {
    }

    Client::~Client()
    {
      if (ret_msg_)
      {
        delete ret_msg_;
        ret_msg_ = NULL;
      }
    }

    int Client::connect()
    {
      tbnet::Connection* conn = CLIENT_POOL.connmgr_->getConnection(ip_);
      if (conn)
      {
        return TFS_SUCCESS;
      }
      return EXIT_CONNECT_ERROR;
    }

    int Client::disconnect()
    {
      return TFS_SUCCESS;
    }

    Message* Client::call(Message* message)
    {
      if (ret_msg_ != NULL)
      {
        delete ret_msg_;
        ret_msg_ = NULL;
      }

      // must copy a message, client send version 2 packet, and no need to deserialize.
      // [sendmsg] object must be allocated on heap, life cycle take over by tbnet
      // framework after call CLIENT_SEND_PACKET
      Message* send_msg = ClientManager::gClientManager.factory_.clone_message(message, 2, false);
      if (send_msg == NULL)
      {
        TBSYS_LOG(ERROR, "clone message failure, pcode:%d", message->getPCode());
        return NULL;
      }
      send_msg->set_auto_free(true);
      // donot call the Message::buildMessage(), because cloneMessage already do that.
      if (!ClientManager::gClientManager.connmgr_->sendPacket(ip_, send_msg, NULL, reinterpret_cast<void*> (static_cast<long>(call_id_))))
      {
        TBSYS_LOG(ERROR, "client(%d) send message cid(%d) to (%s) failure, pcode:%d", call_id_,
            send_msg->getChannelId(), tbsys::CNetUtil::addrToString(ip_).c_str(), send_msg->getPCode());
        delete send_msg;
        return NULL;
      }
      cond_.lock();
      //no wait if message has returned
      if (ret_msg_)
      {
        TBSYS_LOG(INFO, "client call to server(%s) response has returned before cond_wait\n",
            tbsys::CNetUtil::addrToString(ip_).c_str());
      }
      else
      {
        cond_.wait(CLIENT_POOL.max_timeout_);
        if (!ret_msg_)
        {
          TBSYS_LOG(ERROR, "client call to server(%s) failed\n", tbsys::CNetUtil::addrToString(ip_).c_str());
        }
      }
      Message* ret_msg = ret_msg_;
      ret_msg_ = NULL;
      cond_.unlock();
      return ret_msg;
    }

    int32_t Client::handlePacket(tbnet::Packet* packet, void* )
    {
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(ERROR, "got a control packet cmd:%d\n", (dynamic_cast<tbnet::ControlPacket*> (packet)->getCommand()));
        cond_.lock();
        ret_msg_ = NULL;
        cond_.signal();
        cond_.unlock();
        return CALL_UNFINISH;
      }
      cond_.lock();
      ret_msg_ = dynamic_cast<Message*> (packet);
      cond_.signal();
      cond_.unlock();
      return CALL_UNFINISH;
    }

     // sendMessageToServer
     // @param server_id
     // @param message
     // @param retMessage
     // StatusMessage
     // @return TFS_SUCCESS
    int send_message_to_server(const uint64_t server_id, Message* ds_message, Message** ret_message)
    {
      Client* client = CLIENT_POOL.get_client(server_id);
      if (client->connect() != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "sendMessageToServer:connect server failed");
        CLIENT_POOL.release_client(client);
        return EXIT_CONNECT_ERROR;
      }
      int ret_status = EXIT_RECVMSG_ERROR;
      Message* message = client->call(ds_message);
      if (message != NULL)
      {
        if (ret_message == NULL)
        {
          if (message->get_message_type() == STATUS_MESSAGE)
          {
            StatusMessage* s_msg = dynamic_cast<StatusMessage*> (message);
            if (STATUS_MESSAGE_OK == s_msg->get_status())
            {
              ret_status = TFS_SUCCESS;
            }
            else
            {
              TBSYS_LOG(ERROR, "receive an error mesage , retcode(%d), error(%s)", s_msg->get_status(),
                  s_msg->get_error());
              ret_status = s_msg->get_status();
            }
          }
          delete message;
        }
        else
        {
          (*ret_message) = message;
          ret_status = TFS_SUCCESS;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "sendMessageToServer:receive message timeout");
      }
      CLIENT_POOL.release_client(client);

      return (ret_status);
    }

    // postMessageToServer post the message to nameserver 
    // @param server_id
    // @param message
    // @return TFS_SUCCESS
    int post_message_to_server(const uint64_t server_id, Message* message)
    {
      if (!CLIENT_POOL.init_ || !CLIENT_POOL.connmgr_)
      {
        TBSYS_LOG(ERROR, "ClientManager not inited yet!");
        return EXIT_GENERAL_ERROR;
      }
      // must copy a message, client send version 2 packet, and no need to deserialize.
      // [sendmsg] object must be allocated on heap, life cycle take over by tbnet
      // framework after call CLIENT_SEND_PACKET
      Message* send_message = ClientManager::gClientManager.factory_.clone_message(message, 2, false);
      if (send_message == NULL)
      {
        TBSYS_LOG(ERROR, "clone message failure, pcode:%d", message->getPCode());
        return EXIT_GENERAL_ERROR;
      }
      send_message->set_auto_free(true);
      // donot call the Message::buildMessage(), because cloneMessage already do that.
      if (!ClientManager::gClientManager.connmgr_->sendPacket(server_id, send_message, NULL, (void*) ((long) 0)))
      {
        TBSYS_LOG(ERROR, "client post message cid(%d) to (%s) failure, pcode:%d", send_message->getChannelId(),
            tbsys::CNetUtil::addrToString(server_id).c_str(), send_message->getPCode());
        delete send_message;
        return EXIT_SENDMSG_ERROR;
      }
      return TFS_SUCCESS;
    }

    // test whether the DataServerStatInfo is still alive.  
    // @param server_id
    // @return TFS_SUCCESS
    int test_server_alive(const uint64_t server_id)
    {
      Message* ret_msg = NULL;
      StatusMessage send_msg(STATUS_MESSAGE_PING);
      int ret = send_message_to_server(server_id, &send_msg, &ret_msg);
      if (ret != TFS_SUCCESS)
      {
        if (ret_msg != NULL)
        {
          delete ret_msg;
        }
        TBSYS_LOG(ERROR, "test server alive ping server %s error, server is down", tbsys::CNetUtil::addrToString(
            server_id).c_str());
        return ret;
      }
      if (ret_msg)
      {
        if (ret_msg->get_message_type() == STATUS_MESSAGE)
        {
          //TBSYS_LOG(DEBUG, "test server alive ping server %s got status msg ok",
           //   tbsys::CNetUtil::addrToString(server_id).c_str());
          ret = TFS_SUCCESS;
        }
        else
        {
          //TBSYS_LOG(ERROR, "test server alive ping server %s got wrong msg type",
          //    tbsys::CNetUtil::addrToString(server_id).c_str());
          ret = EXIT_RECVMSG_ERROR;
        }
        delete ret_msg;
        return ret;
      }

      return EXIT_GENERAL_ERROR;
    }
  }
}
