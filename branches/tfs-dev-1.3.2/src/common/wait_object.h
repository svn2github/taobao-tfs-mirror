#ifndef TFS_COMMON_WAIT_OBJECT_H_
#define TFS_COMMON_WAIT_OBJECT_H_
#include <tbsys.h>
#include <tbnet.h>
#include <map>
#include <ext/hash_map>

namespace tfs
{
  namespace common
  {
    struct WaitId
    {
      WaitId() : seq_id_(0), send_id_(-1)
      {
      }

      int64_t seq_id_;
      int64_t send_id_;
    };

    class WaitObject
    {
      friend class WaitObjectManager;
    public:
      static const int64_t WAIT_RESPONSE_ARRAY_SIZE = 100;
      WaitObject();
      virtual ~WaitObject();

      int64_t get_id() const;
      void set_id(const int64_t id);
      void add_send_id();
      WaitId* get_wait_key();
      void set_no_free();

      /** wait for response, timeout (us) */
      bool wait(const int32_t wait_count, const int64_t timeout_in_us);
      bool wait(const int64_t timeout_in_us = 0);
      void wakeup();
      void push_response(const int64_t send_id, tbnet::Packet* packet);
      int64_t get_response_count();
      std::map<int64_t, tbnet::Packet*>& get_response();

    private:
      bool free_;
      WaitId wait_key_;
      int32_t done_count_;
      tbsys::CThreadCond cond_;
      std::map<int64_t, tbnet::Packet*> responses_;
    };

    class WaitObjectManager
    {
      struct hash_int64
      {
        size_t
        operator()(int64_t __x) const
          { return __x; }
      };
      typedef __gnu_cxx::hash_map<int64_t, WaitObject*, hash_int64> INT_WAITOBJ_MAP;
      typedef __gnu_cxx::hash_map<int64_t, WaitObject*, hash_int64>::iterator INT_WAITOBJ_MAP_ITER;

      public:
        WaitObjectManager();
        virtual ~WaitObjectManager();

        WaitObject* create_wait_object();
        WaitObject* get_wait_object(const int64_t wait_id);
        void destroy_wait_object(WaitObject*& wait_object);
        void wakeup_wait_object(const WaitId& id, tbnet::Packet* response);

      private:
        void insert_wait_object(WaitObject* wait_object);

      private:
        int64_t seq_id_;
        tbsys::CThreadMutex mutex_;
        INT_WAITOBJ_MAP wait_objects_map_;
    };
  } /* client */
} /* tfs */

#endif /* TFS_CLIENT_WAIT_OBJECT_H_ */
