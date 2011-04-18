#include "new_client.h"
#include "common/error_msg.h"
#include "common/define.h"
#include "client_manager.h"
#include <Memory.hpp>
using namespace std;

namespace tfs
{
  namespace message 
  {
    NewClient::NewClient(const uint32_t& seq_id) 
    : callback_(NULL),
      seq_id_(seq_id),
      generate_send_id_(0),
      complete_(false),
      post_packet_complete_(false)
    {

    }

    NewClient::~NewClient()
    {
      RESPONSE_MSG_MAP::iterator iter = success_response_.begin();
      for (; iter != success_response_.end(); ++iter)
      {
        tbsys::gDelete(iter->second.second);
      }
    }

    bool NewClient::wait(int64_t timeout_in_ms)
    {
      bool ret = true;
      if (timeout_in_ms <= 0)
      {
        timeout_in_ms = common::DEFAULT_NETWORK_CALL_TIMEOUT;
        TBSYS_LOG(WARN, "timeout_in_ms equal 0, we'll use DEFAULT_NETWORK_CALL_TIMEOUT(%"PRI64_PREFIX"d)(ms)", 
          common::DEFAULT_NETWORK_CALL_TIMEOUT);
      }
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      post_packet_complete_ = true;//post packet complete, call timewait 
      uint32_t done_count = success_response_.size() + fail_response_.size();
      complete_ = done_count == send_id_sign_.size();
      if (!complete_)
        ret = monitor_.timedWait(tbutil::Time::milliSeconds(timeout_in_ms));
      complete_ = true;
      return ret;
    }

    bool NewClient::async_wait()
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        post_packet_complete_ = true;//post packet complete
        uint32_t done_count = success_response_.size() + fail_response_.size();
        complete_ = done_count == send_id_sign_.size();
      }
      if (complete_ && NULL != callback_)
      {
        callback_(this);
      }
      return true;
    }

    int NewClient::post_request(const uint64_t server, Message* packet, uint8_t& send_id)
    {
      int32_t ret = NULL != packet && server > 0 ? common::TFS_SUCCESS : common::EXIT_INVALID_ARGU;
      if (common::TFS_SUCCESS == ret)
      {
        send_id = create_send_id(server);
        if (send_id > MAX_SEND_ID)
        {
          ret = common::TFS_ERROR;
        }
        else
        {
          WaitId id;
          id.seq_id_ = seq_id_;
          id.send_id_= send_id;
          Message* send_msg = NewClientManager::get_instance().factory_.clone_message(packet, 2, false);
          if (NULL == send_msg)
          {
            TBSYS_LOG(ERROR, "clone message failure, pcode:%d", packet->getPCode());
            ret = common::TFS_ERROR;
            destroy_send_id(id);
          }
          else
          {
            send_msg->set_auto_free(true);
            bool send_ok = NewClientManager::get_instance().connmgr_->sendPacket(server, send_msg,
                NULL, reinterpret_cast<void*>(*(reinterpret_cast<int32_t*>(&id))));
            if (!send_ok)
            {
              ret = common::EXIT_SENDMSG_ERROR;
              TBSYS_LOG(INFO, "cannot post packet, maybe send queue is full or disconnect.");
              tbsys::gDelete(send_msg);
              destroy_send_id(id);
            }
          }
        }
      }
      return ret;
    }

    //post_packet is async version of send_packet. donot wait for response packet.
    int NewClient::async_post_request(const std::vector<uint64_t>& servers, Message* packet, callback_func func)
    {
      int32_t ret = !servers.empty() && NULL != packet &&  NULL != func ? common::TFS_SUCCESS : common::EXIT_INVALID_ARGU;
      if (common::TFS_SUCCESS == ret)
      {
        if (NULL == callback_)
          callback_ = func;

        std::vector<uint64_t>::const_iterator iter = servers.begin();
        for (; iter != servers.end(); ++iter)
        {
          uint8_t send_id = 0;
          ret = post_request((*iter), packet, send_id);
          if (common::TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "async post request fail, iret: %d", ret);
            break;
          }
        }
      }

      if (common::TFS_SUCCESS == ret)
      {
        async_wait();
      }
      return ret;
    }

    bool NewClient::handlePacket(const WaitId& id, tbnet::Packet* packet)
    {
      bool ret = true;
      monitor_.lock();
      if (complete_) //wait has return before wakeup. maybe timeout or has reach the wait count
      {
        ret = false;
        TBSYS_LOG(ERROR, "waitobject has returned before wakeup, waitid: %u, send id: %hhu", id.seq_id_, id.send_id_);
      }
      else
      {
        SEND_SIGN_PAIR* send_id = find_send_id(id);
        if (NULL == send_id)
        {
          ret = false;
          TBSYS_LOG(ERROR, "'WaitId' object not found by seq_id: %u send_id: %hhu", id.seq_id_, id.send_id_);
        }
        else
        {
          if (packet != NULL && packet->isRegularPacket())
          {
            ret = push_success_response(send_id->first, send_id->second, dynamic_cast<Message*>(packet));
          }
          else
          {
            ret = push_fail_response(send_id->first, send_id->second);
          }
          uint32_t done_count = success_response_.size() + fail_response_.size();
          complete_ = (done_count == send_id_sign_.size() && post_packet_complete_);
        }
      }
      if (ret && complete_ && NULL == callback_)
      {
        monitor_.notify();
      }
      bool is_callback = ret && callback_ != NULL && complete_;
      monitor_.unlock();

      if (is_callback)
      {
        callback_(this);
      }
      return ret;
    }

    uint8_t NewClient::create_send_id(const uint64_t server)
    {
      ++generate_send_id_;
      if (generate_send_id_ > MAX_SEND_ID)
      {
        TBSYS_LOG(ERROR, "send id: %hhu is invalid, MAX_END_ID: %hhu", generate_send_id_, MAX_SEND_ID);
      }
      else
      {
        send_id_sign_.insert(send_id_sign_.end(), SEND_SIGN_PAIR(generate_send_id_, server));
      }
      return generate_send_id_;
    }

    bool NewClient::destroy_send_id(const WaitId& id)
    {
      std::vector<SEND_SIGN_PAIR>::iterator iter = send_id_sign_.begin();
      for (; iter != send_id_sign_.end(); ++iter)
      {
        if ((*iter).first == id.send_id_)
        {
          send_id_sign_.erase(iter);
          break;
        }
      }
      return true;
    }

    NewClient::SEND_SIGN_PAIR* NewClient::find_send_id(const WaitId& id)
    {
      SEND_SIGN_PAIR* result = NULL;
      std::vector<SEND_SIGN_PAIR>::iterator iter = send_id_sign_.begin();
      for (; iter != send_id_sign_.end(); ++iter)
      {
        if ((*iter).first == id.send_id_)
        {
          result = &(*iter);
          break;
        }
      }
      return result;
    }

    bool NewClient::push_success_response(const uint8_t send_id, const uint64_t server, Message* packet)
    {
      bool bret = true;
      RESPONSE_MSG_MAP_ITER iter = fail_response_.find(send_id);
      if (iter != fail_response_.end())//erase controlpacket packet
      {
        fail_response_.erase(iter);
      }
      iter = success_response_.find(send_id);
      if (iter == success_response_.end())
      {
        success_response_.insert(std::pair<uint8_t, std::pair<uint64_t, Message*> >(send_id, std::pair<uint64_t, Message*>(server, packet)));
      }
      else
      {
        bret = false;
      }
      return bret;
    }

    bool NewClient::push_fail_response(const uint8_t send_id, const uint64_t server)
    {
      bool bret = true;
      RESPONSE_MSG_MAP_ITER iter = success_response_.find(send_id);
      if (iter == success_response_.end())//data packet not found
      {
        iter = fail_response_.find(send_id);
        if (iter == fail_response_.end())
        {
          fail_response_.insert(std::pair<uint8_t, std::pair<uint64_t, Message*> >(send_id, std::pair<uint64_t, Message*>(server, NULL)));
        }
      }
      return bret;
    }
  }
}
