#include "client_manager.h"
#include "common/error_msg.h"
#include <Memory.hpp>

namespace tfs
{
  namespace message 
  {
    NewClientManager::NewClientManager()
      : transport_(NULL), connmgr_(NULL), seq_id_(0), initialize_(false), own_transport_(false)
    {
    }

    NewClientManager::~NewClientManager()
    {
      destroy();
    }

    void NewClientManager::destroy()
    {
      if (own_transport_ && transport_)
      {
        transport_->stop();
        transport_->wait();
        tbsys::gDelete(transport_);
      }

      tbsys::gDelete(connmgr_);

      initialize_ = false;
    }

    void NewClientManager::initialize()
    {
      if (!initialize_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        if (!initialize_)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = new tbnet::Transport();
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          transport_->start();
          own_transport_ = true;
          initialize_ = true;
          new_clients_.clear();
        }
      }
    }

    void NewClientManager::initialize_with_transport(tbnet::Transport* transport)
    {
      if (!initialize_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        if (!initialize_)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = transport;
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          own_transport_ = false;
          initialize_ = true;
          new_clients_.clear();
        }
      }
    }

    tbnet::IPacketHandler::HPRetCode NewClientManager::handlePacket(
        tbnet::Packet* packet, void* args)
    {
      bool call_wakeup = NULL != args;
      if (call_wakeup)
      {
        bool is_disconntion_packet = (NULL != packet)
                      && (!packet->isRegularPacket()) //disconntion packet
                      && (packet->getPCode() == tbnet::ControlPacket::CMD_DISCONN_PACKET);
        call_wakeup = !is_disconntion_packet;
        if (call_wakeup)
        {
          WaitId id = *(reinterpret_cast<WaitId*>(&args));
          handlePacket(id, packet);
        }
      }
      else
      {
        if (NULL != packet)
        {
          if (packet->isRegularPacket())//data packet
          {
            TBSYS_LOG(INFO, "no client waiting this packet.code: %d", packet->getPCode());
            packet->free();
          }
          else
          {
            TBSYS_LOG(DEBUG, "packet pcode: %d is not regular packet, command: %d, discard anyway. "
                "args is NULL, maybe post channel timeout packet ",
                packet->getPCode(), dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
          }
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    NewClient* NewClientManager::create_client()
    {
      initialize();
      tbutil::Mutex::Lock lock(mutex_);
      ++seq_id_;
      if (seq_id_ >= MAX_SEQ_ID)
      {
        seq_id_ = 0;
      }
      NewClient* client = NULL;
      NEWCLIENT_MAP_ITER iter = new_clients_.find(seq_id_);
      if (iter != new_clients_.end())
      {
        client = new NewClient(seq_id_);
        new_clients_.insert(std::make_pair(seq_id_, client));
      }
      else
      {
        TBSYS_LOG(ERROR, "client id(%u) was existed", seq_id_);
      }
      return client;
    }

    bool NewClientManager::destroy_client(NewClient* client)
    {
      bool bret =  NULL != client;
      if (bret)
      {
        const uint32_t& id = client->get_seq_id();
        tbutil::Mutex::Lock lock(mutex_);
        NEWCLIENT_MAP_ITER iter = new_clients_.find(id);
        if (iter != new_clients_.end())
        {
          tbsys::gDelete(client);
          new_clients_.erase(iter);
        }
        else
        {
          bret = false;
          TBSYS_LOG(ERROR, "client id(%u) not found", seq_id_);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "client object is null when call destroy_client function");
      }
      return bret;
    }

    bool NewClientManager::handlePacket(const WaitId& id, tbnet::Packet* response)
    {
      bool ret = true;
      tbutil::Mutex::Lock lock(mutex_);
      NEWCLIENT_MAP_ITER iter = new_clients_.find(id.seq_id_);
      if (iter == new_clients_.end())
      {
        TBSYS_LOG(INFO, "client not found, id: %u", id.seq_id_);
        ret = false;
      }
      else
      {
        // if got control packet or NULL, we will still add the done counter
        ret = iter->second->handlePacket(id, response);
      }
      
      if (!ret && response != NULL && response->isRegularPacket())
      {
        TBSYS_LOG(DEBUG, "delete response message client id: %u", id.seq_id_);
        tbsys::gDelete(response);
      }
      return ret;
    }

    // test whether the DataServerStatInfo is still alive.  
    int test_server_alive(const uint64_t server_id, const int64_t timeout)
    {
      int32_t ret = common::TFS_SUCCESS;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        TBSYS_LOG(ERROR, "%s", "create new client fail");
        ret = common::TFS_ERROR;
      }
      else
      {
        uint8_t send_id = 0;
        StatusMessage send_msg(STATUS_MESSAGE_PING);
        ret = client->post_request(server_id, &send_msg, send_id);
        if (common::TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "test server alive ping server(%s) error, server is down",
            tbsys::CNetUtil::addrToString(server_id).c_str());
        }
        else
        {
          bool bret = client->wait();
          if (!bret)
          {
            ret = common::TFS_ERROR;
            TBSYS_LOG(ERROR, "%s", "new client wait server(%s) response fail",
              tbsys::CNetUtil::addrToString(server_id).c_str());
          }
          else
          {
            NewClient::RESPONSE_MSG_MAP* response = client->get_success_response();
            if (NULL == response
                || response->empty())
            {
              ret = common::TFS_ERROR;
              TBSYS_LOG(ERROR, "test server alive ping server(%s) error, server mybe down",
                  tbsys::CNetUtil::addrToString(server_id).c_str());
            }
            else
            {
              ret = response->begin()->second.second->getPCode() == STATUS_MESSAGE
                  ? common::TFS_SUCCESS : common::TFS_ERROR;
            }
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    //send message to server
    /*int send_message_to_server(const uint64_t server_id, Message* msg, Message** response, const int64_t timeout)
    {
      Message* ret_msg = NULL;
      int iret = NewClientManager::get_instance().call(server_id, msg, timeout, ret_msg);
      if (TFS_SUCCESS == iret 
          && NULL != ret_msg)
      {
        if (NULL != response)
        {
          *response = ret_msg; 
        }
        else
        {
          if (ret_msg->getPCode() == STATUS_MESSAGE)
          {
            StatusMessage* s_msg = dynamic_cast<StatusMessage*>(ret_msg);
            iret = STATUS_MESSAGE_OK == s_msg->get_status()
                    ? TFS_SUCCESS :  s_msg->get_status();
          }
          else
          {
            iret = TFS_ERROR;
            TBSYS_LOG(ERROR, "send message to server(%s) error, message(%d:%d) is invalid",
                tbsys::CNetUtil::addrToString(server_id).c_str(), msg->getPCode(), ret_msg->getPCode());
          }
        }
      }
      else
      {
        iret = TFS_ERROR;
        TBSYS_LOG(ERROR, "send message to server(%s) error: receive message error or timeout",
          tbsys::CNetUtil::addrToString(server_id).c_str());
      }
      return iret;
    }*/
  }
}
