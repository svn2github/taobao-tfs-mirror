#include "wait_object.h"
#include "common/define.h"
#include <Memory.hpp>
using namespace std;

namespace tfs
{
  namespace message 
  {
    using namespace common;

    WaitObject::WaitObject() : free_(false), wait_over_(false), done_count_(0)
    {
      responses_.clear();
    }

    WaitObject::~WaitObject()
    {
      if (free_)
      {
        std::map<uint16_t, Message*>::iterator it = responses_.begin();
        for ( ; it != responses_.end(); ++it)
        {
          tbsys::gDelete(it->second);
        }
      }
    }

    bool WaitObject::wait(const int64_t timeout_in_us)
    {
      return wait(1, timeout_in_us);
    }

    bool WaitObject::wait(const int32_t wait_count, const int64_t timeout_in_us)
    {
      bool ret = true;
      int64_t timeout_in_ms = timeout_in_us <= 0 ? 
        DEFAULT_NETWORK_CALL_TIMEOUT / 1000 : timeout_in_us / 1000;

      cond_.lock();
      while (done_count_ < wait_count)
      {
        if (cond_.wait(timeout_in_ms) == false)
        {
          ret = false;
          break;
        }
      }
      wait_over_ = true;
      cond_.unlock();
      return ret;
    }

    bool WaitObject::wakeup(const uint16_t send_id, tbnet::Packet* packet)
    {
      bool ret = true;
      cond_.lock();
      if (wait_over_) //wait has return before wakeup. maybe timeout or has reach the wait count
      {
        ret = false;
        TBSYS_LOG(ERROR, "waitobject has returned before wakeup, waitid: %hu, send id: %hu", wait_id_, send_id);
      }

      if (packet != NULL && packet->isRegularPacket())
      {
        push_response(send_id, dynamic_cast<Message*>(packet));
      }

      done_count_++;
      cond_.signal();
      cond_.unlock();

      return ret;
    }

    void WaitObject::set_free()
    {
      free_ = true;
    }

    uint16_t WaitObject::get_id() const
    {
      return wait_id_;
    }

    void WaitObject::set_id(const uint16_t id)
    {
      wait_id_ = id;
    }

    WaitId WaitObject::get_wait_key()
    {
      return wait_id_sign_.back();
    }

    void WaitObject::add_send_id()
    {
      uint16_t cur_send_id = static_cast<uint16_t>(wait_id_sign_.size());
      WaitId cur_wait_id; 
      cur_wait_id.seq_id_ = wait_id_;
      cur_wait_id.send_id_ = cur_send_id;
      wait_id_sign_.push_back(cur_wait_id);
    }

    void WaitObject::set_send_id(const uint16_t index_id)
    {
      uint16_t cur_send_id = index_id;
      WaitId cur_wait_id; 
      cur_wait_id.seq_id_ = wait_id_;
      cur_wait_id.send_id_ = cur_send_id;
      wait_id_sign_.push_back(cur_wait_id);
    }

    int64_t WaitObject::get_response_count()
    {
      return responses_.size();
    }

    void WaitObject::push_response(const uint16_t send_id, Message* packet)
    {
      if (!(responses_.insert(pair<uint16_t, Message*>(send_id, packet))).second) //insert false
      {
        TBSYS_LOG(ERROR, "insert responses fail, waitid: %hu, send id: %hu", wait_id_, send_id);
      }
      return;
    }

    std::map<uint16_t, Message*>& WaitObject::get_response()
    {
      return responses_;
    }

    Message* WaitObject::get_single_response()
    {
      Message* ret_pkt = NULL;
      if (!responses_.empty())
      {
        ret_pkt = responses_.begin()->second;
      }
      return ret_pkt;
    }

    // WaitObjectManager
    WaitObjectManager::WaitObjectManager() : seq_id_(1)
    {
    }

    WaitObjectManager::~WaitObjectManager()
    {
      // free
      tbsys::CThreadGuard guard(&mutex_);
      INT_WAITOBJ_MAP_ITER iter;
      for (iter = wait_objects_map_.begin(); iter != wait_objects_map_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
    }

    WaitObject* WaitObjectManager::create_wait_object()
    {
      WaitObject* wo = new WaitObject();
      insert_wait_object(wo);
      return wo;
    }

    WaitObject* WaitObjectManager::get_wait_object(const uint16_t wait_id)
    {
      tbsys::CThreadGuard guard(&mutex_);
      INT_WAITOBJ_MAP_ITER it = wait_objects_map_.find(wait_id);
      if (it == wait_objects_map_.end())
      {
        TBSYS_LOG(INFO, "wait object not found, id: %hu", wait_id);
        return NULL;
      }
      return it->second;
    }

    void WaitObjectManager::destroy_wait_object(WaitObject* wait_object)
    {
      if (wait_object != NULL)
      {
        tbsys::CThreadGuard guard(&mutex_);
        wait_objects_map_.erase(wait_object->get_id());
        tbsys::gDelete(wait_object);
      }
    }

    void WaitObjectManager::destroy_wait_object(const uint16_t wait_id)
    {
      WaitObject* wait_obj = get_wait_object(wait_id);
      destroy_wait_object(wait_obj);
    }

    void WaitObjectManager::wakeup_wait_object(const WaitId& id, tbnet::Packet* response)
    {
      tbsys::CThreadGuard guard(&mutex_);
      bool ret = true;
      INT_WAITOBJ_MAP_ITER it = wait_objects_map_.find(id.seq_id_);
      if (it == wait_objects_map_.end())
      {
        TBSYS_LOG(INFO, "wait object not found, id: %hu", id.seq_id_);
        ret = false;
      }
      else
      {
        // if got control packet or NULL, we will still add the done counter
        ret = it->second->wakeup(id.send_id_, response);
      }
      
      if (!ret && response != NULL && response->isRegularPacket())
      {
        TBSYS_LOG(DEBUG, "delete rsp wait object, id: %hu", id.seq_id_);
        tbsys::gDelete(response);
      }

      return;
    }

    void WaitObjectManager::insert_wait_object(WaitObject* wait_object)
    {
      if (wait_object != NULL)
      {
        tbsys::CThreadGuard guard(&mutex_);
        ++seq_id_;
        if (seq_id_ == UINT16_MAX) 
          seq_id_ = 1;
        wait_object->set_id(seq_id_);
        wait_objects_map_[seq_id_] = wait_object;
      }
      return;
    }

  }
}
