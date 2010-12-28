#include "wait_object.h"
#include "common/define.h"
#include <Memory.hpp>

namespace tfs
{
  namespace message 
  {
    using namespace common;
    const int64_t WaitObject::WAIT_RESPONSE_ARRAY_SIZE;

    WaitObject::WaitObject() : free_(false), done_count_(0)
    {
      responses_.clear();
    }

    WaitObject::~WaitObject()
    {
      if (free_)
      {
        std::map<int64_t, Message*>::iterator it = responses_.begin();
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
      int64_t timeout_in_ms = timeout_in_us / 1000;
      if (wait_count > WAIT_RESPONSE_ARRAY_SIZE)
      {
        ret = false;
        TBSYS_LOG(ERROR, "cannot wait more than %d responses, you request %d", WAIT_RESPONSE_ARRAY_SIZE, wait_count);
      }
      if (ret)
      {
        cond_.lock();
        while (done_count_ < wait_count)
        {
          if (cond_.wait(timeout_in_ms) == false)
          {
            ret = false;
            break;
          }
        }
        cond_.unlock();
      }
      return ret;
    }

    void WaitObject::wakeup()
    {
      cond_.lock();
      done_count_++;
      cond_.signal();
      cond_.unlock();
    }

    void WaitObject::set_free()
    {
      free_ = true;
    }

    int64_t WaitObject::get_id() const
    {
      return wait_key_.seq_id_;
    }

    void WaitObject::set_id(const int64_t id)
    {
      wait_key_.seq_id_ = id;
    }

    WaitId* WaitObject::get_wait_key()
    {
      return &wait_key_;
    }

    void WaitObject::add_send_id()
    {
      ++wait_key_.send_id_;
    }

    int64_t WaitObject::get_response_count()
    {
      return responses_.size();
    }

    void WaitObject::push_response(const int64_t send_id, Message* packet)
    {
      responses_[send_id] = packet;
      return;
    }

    std::map<int64_t, Message*>& WaitObject::get_response()
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

    WaitObject* WaitObjectManager::get_wait_object(const int64_t wait_id)
    {
      tbsys::CThreadGuard guard(&mutex_);
      INT_WAITOBJ_MAP_ITER it = wait_objects_map_.find(wait_id);
      if (it == wait_objects_map_.end())
      {
        TBSYS_LOG(INFO, "wait object not found, id: %"PRI64_PREFIX"d", wait_id);
        return NULL;
      }
      return it->second;
    }

    void WaitObjectManager::destroy_wait_object(WaitObject*& wait_object)
    {
      if (wait_object != NULL)
      {
        tbsys::CThreadGuard guard(&mutex_);
        wait_objects_map_.erase(wait_object->get_id());
        tbsys::gDelete(wait_object);
      }
    }

    void WaitObjectManager::destroy_wait_object(const int64_t wait_id)
    {
      WaitObject* wait_obj = get_wait_object(wait_id);
      destroy_wait_object(wait_obj);
    }

    void WaitObjectManager::wakeup_wait_object(const WaitId& id, tbnet::Packet* response)
    {
      tbsys::CThreadGuard guard(&mutex_);
      INT_WAITOBJ_MAP_ITER it = wait_objects_map_.find(id.seq_id_);
      if (it == wait_objects_map_.end())
      {
        TBSYS_LOG(INFO, "wait object not found, id: %"PRI64_PREFIX"d", id.seq_id_);
        if (response != NULL && response->isRegularPacket())
        {
          tbsys::gDelete(response);
        }
      }
      else
      {
        if (response != NULL && response->isRegularPacket())
        {
          it->second->push_response(id.send_id_, dynamic_cast<Message*>(response));
        }

        // if got control packet or NULL, we will still add the done counter
        it->second->wakeup();
      }

      return;
    }

    void WaitObjectManager::insert_wait_object(WaitObject* wait_object)
    {
      if (wait_object != NULL)
      {
        tbsys::CThreadGuard guard(&mutex_);
        ++seq_id_;
        if (seq_id_ <= 0)
          seq_id_ = 1;
        wait_object->set_id(seq_id_);
        wait_objects_map_[seq_id_] = wait_object;
      }
      return;
    }

  }
}
