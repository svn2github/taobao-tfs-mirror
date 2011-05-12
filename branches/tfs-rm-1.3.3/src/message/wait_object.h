#ifndef TFS_MESSAGE_WAIT_OBJECT_H_
#define TFS_MESSAGE_WAIT_OBJECT_H_
#include <tbsys.h>
#include <tbnet.h>
#include <map>
#include <ext/hash_map>
#include "message.h"

namespace tfs
{
  namespace message 
  {
#ifndef UINT16_MAX 
#define UINT16_MAX 65535U 
#endif

    struct WaitId
    {
      WaitId() : seq_id_(0), send_id_(0)
      {
      }

      WaitId(const uint16_t seq_id, const uint16_t send_id)
        : seq_id_(seq_id), send_id_(send_id)
      {
      }

      uint16_t seq_id_;
      uint16_t send_id_;
    };

    class WaitObject
    {
      friend class WaitObjectManager;
    public:
      static const int64_t WAIT_RESPONSE_ARRAY_SIZE = 100;
      WaitObject();
      virtual ~WaitObject();

      uint16_t get_id() const;
      void set_id(const uint16_t id);
      void add_send_id();
      void set_send_id(const uint16_t index_id);
      WaitId get_wait_key();
      void set_free();

      /** wait for response, timeout (us) */
      bool wait(const int32_t wait_count, const int64_t timeout_in_us);
      bool wait(const int64_t timeout_in_us = 0);
      bool wakeup(const uint16_t send_id, tbnet::Packet* packet);
      int64_t get_response_count();
      std::map<uint16_t, Message*>& get_response();
      Message* get_single_response();

    private:
      void push_response(const uint16_t send_id, Message* packet);

    private:
      bool free_;
      bool wait_over_;
      uint16_t wait_id_;
      std::vector<WaitId> wait_id_sign_;
      WaitId wait_key_;
      int32_t done_count_;
      tbsys::CThreadCond cond_;
      std::map<uint16_t, Message*> responses_;
    };

    class WaitObjectManager
    {
      struct hash_uint16
      {
        size_t operator()(uint16_t __x) const
        { 
          return __x;
        }
      };
      typedef __gnu_cxx::hash_map<uint16_t, WaitObject*, hash_uint16> INT_WAITOBJ_MAP;
      typedef __gnu_cxx::hash_map<uint16_t, WaitObject*, hash_uint16>::iterator INT_WAITOBJ_MAP_ITER;

      public:
        WaitObjectManager();
        virtual ~WaitObjectManager();

        WaitObject* create_wait_object();
        WaitObject* get_wait_object(const uint16_t wait_id);
        void destroy_wait_object(WaitObject* wait_object);
        void destroy_wait_object(const uint16_t wait_id);
        void wakeup_wait_object(const WaitId& id, tbnet::Packet* response);

      private:
        void insert_wait_object(WaitObject* wait_object);

      private:
        static const uint16_t MAX_SEQ_ID = static_cast<uint16_t>(-1);

      private:
        uint16_t seq_id_;
        tbsys::CThreadMutex mutex_;
        INT_WAITOBJ_MAP wait_objects_map_;
    };
  } /* message */
} /* tfs */

#endif /* TFS_MESSAGE_WAIT_OBJECT_H_ */
