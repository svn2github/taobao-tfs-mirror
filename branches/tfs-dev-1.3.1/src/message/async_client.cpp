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
#include "common/error_msg.h"
#include "common/func.h"
#include "async_client.h"
#include "client_pool.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    atomic_t Callee::global_call_id_ = { 1 };
    Callee::Callee()
    {
      call_id_ = atomic_add_return(1, &global_call_id_);
    }

    Callee::~Callee()
    {
    }

    int32_t Callee::get_call_id()
    {
      return call_id_;
    }

    AsyncClient::AsyncClient(const VUINT64& ds, AsyncCallback* cb) :
      handle_response_count_(0), handle_success_count_(0), send_success_count_(0), call_over_(false), send_message_(
          NULL), callback_(cb)
    {
      assert(ds.size());
      ds_list_.assign(ds.begin(), ds.end());
    }

    AsyncClient::~AsyncClient()
    {
      if (send_message_)
      {
        delete send_message_;
      }
    }

    bool AsyncClient::save_message(const Message* message)
    {
      if (message != NULL)
      {
        send_message_ = ClientManager::gClientManager.factory_.clone_message(const_cast<Message*>(message), -1, true);
        return (send_message_ != NULL);
      }
      return false;
    }

    int AsyncClient::connect()
    {
      int32_t i = 0;
      int32_t list_size = ds_list_.size();
      for (i = 0; i < list_size; ++i)
      {
        uint64_t ds_ip = Func::addr_inc_port(ds_list_[i], 1);
        tbnet::Connection* conn = CLIENT_POOL.connmgr_->getConnection(ds_ip);
        if (conn == NULL)
        {
          TBSYS_LOG(ERROR, "connect %s failed\n", tbsys::CNetUtil::addrToString(ds_ip).c_str());
          return EXIT_CONNECT_ERROR;
        }
        else
        {
          conn->setQueueTimeout(WRITE_WAIT_TIME);
        }
      }
      return TFS_SUCCESS;
    }

    int AsyncClient::disconnect()
    {
      return TFS_SUCCESS;
    }

    int32_t AsyncClient::post(const Message* msg)
    {
      save_message(msg);
      int32_t i = 0;
      int32_t list_size = ds_list_.size();
      for (i = 0; i < list_size; ++i)
      {
        uint64_t dsip = Func::addr_inc_port(ds_list_[i], 1);
        Message* send_msg = ClientManager::gClientManager.factory_.clone_message(const_cast<Message*> (msg), 2, false);
        if (send_msg == NULL)
        {
          TBSYS_LOG(ERROR, "clone message failure, pcode:%d", const_cast<Message*>(msg)->getPCode());
          break;
        }
        send_msg->set_auto_free(true);

        mutex_.lock();
        if (!ClientManager::gClientManager.connmgr_->sendPacket(dsip, send_msg, NULL, (void*) (static_cast<long>(call_id_))))
        {
          TBSYS_LOG(ERROR, "client(%d) send message cid(%d) to server(%s) failure, pcode:%d", call_id_,
              send_msg->getChannelId(), tbsys::CNetUtil::addrToString(dsip).c_str(), send_msg->getPCode());
          delete send_msg;
          call_over_ = true;
          mutex_.unlock();
          break;
        }
        ++send_success_count_;
        TBSYS_LOG(DEBUG, "client(%d) post packet(%d) success, count:%d, dssize:%d, over:%d", call_id_,
            const_cast<Message*>(msg)->getChannelId(), send_success_count_, list_size, call_over_);
        if (send_success_count_ >= ds_list_.size())
        {
          call_over_ = true;
          i = ds_list_.size();
          mutex_.unlock();
          break;
        }
        mutex_.unlock();
      }
      return i;
    }

    Message* AsyncClient::call(Message* message)
    {
      post(const_cast<Message*>(message));
      return NULL;
    }

    int AsyncClient::handlePacket(tbnet::Packet* packet, void*)
    {
      bool response_result = false;
      bool error_occured = true;
      int response_status = CALL_UNFINISH;
      std::string err_msg;
      ++handle_response_count_;
      Message* message = NULL;
      StatusMessage* s_msg = NULL;
      if (!packet->isRegularPacket())
      { // failed
        tbnet::ControlPacket* ctrl = static_cast<tbnet::ControlPacket*> (packet);
        TBSYS_LOG(ERROR, "client(%d) handle error control packet:%d\n", call_id_, ctrl->getCommand());
        err_msg.assign("client handle controlpacket, maybe timeout or disconnect.");
        goto out;
      }

      message = static_cast<Message*> (packet);
      if (message->get_message_type() == STATUS_MESSAGE)
      {
        s_msg = (StatusMessage*) message;
        if (s_msg->get_status() != STATUS_MESSAGE_OK)
        {
          TBSYS_LOG(ERROR, "client(%d) handle response error %d,%s\n", call_id_, s_msg->get_status(),
              s_msg->get_error());
          err_msg.assign(s_msg->get_error());
          goto out;
        }
        else
        { // ok, check all server return..
          handle_success_count_++;
          error_occured = false;
          goto out;
        }
      }
      else
      {
        // get wired packet...
        TBSYS_LOG(ERROR, "get wired packet %s,%d\n", message->get_name(), message->getChannelId());
        goto out;
      }

      out:
      // all message posted & all response handled or handle a error response
      mutex_.lock();
      if (call_over_ && ((handle_response_count_ >= send_success_count_) || error_occured))
        response_status = CALL_FINISHED;
      // all message posted & all response handled successfully
      if (call_over_ && (handle_success_count_ >= ds_list_.size()))
      {
        response_result = true;
      }
      TBSYS_LOG(DEBUG, "client(%d) handle packet response_status:%d,response_result:%d, "
        "error_occured:%d, _callOver:%d, _handleResponseCount:%d,_handleSucessCount:%d, _sendSucessCount:%d\n",
          call_id_, response_status, response_result, error_occured, call_over_, handle_response_count_,
          handle_success_count_, send_success_count_);
      mutex_.unlock();

      if ((response_status == CALL_FINISHED) && all_message_posted())
      {
        callback_->command_done(send_message_, response_result, err_msg);
      }
      // only allMessagePosted need callback...
      if (packet->isRegularPacket())
      {
        delete packet;
      }
      return response_status;
    }

    SimpleAsyncClient::SimpleAsyncClient(const VUINT64& ds, SimpleAsyncCallback* cb) :
      AsyncClient(ds, NULL), simple_callback_(cb)
    {
    }

    SimpleAsyncClient::~SimpleAsyncClient()
    {
    }

    int SimpleAsyncClient::handlePacket(tbnet::Packet* packet, void*)
    {
      Message* message = NULL;

      if (!packet->isRegularPacket())
      { // failed
        tbnet::ControlPacket* ctrl = static_cast<tbnet::ControlPacket*> (packet);
        TBSYS_LOG(ERROR, "client(%d) handle error control packet:%d\n", call_id_, ctrl->getCommand());
        goto out;
      }

      message = static_cast<Message*> (packet);

      out:
        if (NULL != message)
        {
          TBSYS_LOG(DEBUG, "client(%d) handle packet %d ", call_id_, message->get_message_type());
          simple_callback_->command_done(send_message_, message);
        }
      // packet must
      return 0;
    }

    int DefaultAsyncCallback::command_done(Message* send_message, bool status, const string& error)
    {
      StatusMessage* response = new StatusMessage();
      response->set_message(status ? STATUS_MESSAGE_OK : STATUS_MESSAGE_ERROR, const_cast<char*>(error.c_str()));
      send_message->reply_message(response);
      return TFS_SUCCESS;
    }

    int async_post_message_to_servers(const Message* message, VUINT64& ds_list, AsyncCallback* cb)
    {
      if (!ds_list.size())
      {
        return EXIT_GENERAL_ERROR;
      }
      AsyncClient* client = CLIENT_POOL.get_async_client(ds_list, cb);
      if (client->connect() != TFS_SUCCESS)
      {
        CLIENT_POOL.release_client(client);
        return EXIT_CONNECT_ERROR;
      }
      uint32_t send = client->post(const_cast<Message*> (message));
      if (send < ds_list.size())
      {
        if (send == 0)
        {
          CLIENT_POOL.release_client(client);
        }
        return EXIT_GENERAL_ERROR;
      }
      // if post all message to server, dont release client object, wait for async repsonse message.
      return TFS_SUCCESS;
    }

    int simple_async_post_message_to_server(const Message* message, const uint64_t server_id, SimpleAsyncCallback* cb)
    {
      SimpleAsyncClient* client = CLIENT_POOL.get_simple_async_client(server_id, cb);
      if (client->connect() != TFS_SUCCESS)
      {
        CLIENT_POOL.release_client(client);
        return EXIT_CONNECT_ERROR;
      }
      uint32_t send = client->post(const_cast<Message*> (message));
      if (send < 1)
      {
        CLIENT_POOL.release_client(client);
        return EXIT_GENERAL_ERROR;
      }
      // if post all message to server, do not release client object, wait for async repsonse message.
      return TFS_SUCCESS;
    }
  }
}
