/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_object.h 868 2013-08-12 00:09:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_OBJECT_H_
#define TFS_COMMON_BASE_OBJECT_H_

#include <set>
#include <list>
#include "define.h"
#include "error_msg.h"
#include "array_helper.h"
#include "tfs_vector.h"
#include "lock.h"
#include "internal.h"
#include <Mutex.h>

namespace tfs
{

  namespace common
  {
    typedef enum ServiceStatus_
    {
      SERVICE_STATUS_ONLINE = 0,
      SERVICE_STATUS_OFFLINE = 1
    }ServiceStatus;

    typedef enum ObjectWaitFreePhase_
    {
      OBJECT_WAIT_FREE_PHASE_NONE  = -1,
      OBJECT_WAIT_FREE_PHASE_CLEAR = 0,
      OBJECT_WAIT_FREE_PHASE_FREE  = 1
    }ObjectWaitFreePhase;

    template <typename T>
      class BaseObject
      {
        public:
          explicit BaseObject(const int64_t now):
            expire_time_(now) {}
          virtual ~BaseObject() {}
          inline void free() { delete this;}
          inline virtual void callback(void*, T&) {}
          inline void set(const int64_t now, const int32_t times) {expire_time_ = now + times;}
          inline int64_t get() const { return expire_time_;}
          inline bool expire(const int64_t now) const  { return now >= expire_time_;}
        protected:
          int64_t  expire_time_;/** object expire time or lease expire time(ms)**/
        private:
          DISALLOW_COPY_AND_ASSIGN(BaseObject<T>);
      };

    //lease
    template <typename T1, typename T2>
      class BaseLease : public BaseObject<T1>
    {
      public:
        BaseLease(const uint64_t lease_id, const int64_t now);
        virtual ~BaseLease();
        virtual bool has_valid_lease(const int64_t now) const;
        virtual bool renew(const T2& info, const int64_t now, const int32_t times);
        bool operator < (const BaseLease<T1, T2>& value) const;
        virtual int reset(const T2& info, const int64_t now, T1& manager) = 0;
        virtual int update(const T2& info, const int64_t now, const bool isnew) = 0;
        inline uint64_t id() const { return lease_id_;}
        inline int8_t get_status() const { return status_;}
        inline void set_status(const int8_t status) { status_ = status;}
        inline bool is_alive() const { return SERVICE_STATUS_ONLINE == status_;}
        inline int8_t get_wait_free_phase() const { return wait_free_phase_;}
        inline void set_wait_free_phase(const int8_t wait_free_phase) { wait_free_phase_ = wait_free_phase;}
      protected:
        common::RWLock rwmutex_;
        uint64_t lease_id_;/** lease id **/
        int8_t  status_:3;
        int8_t  wait_free_phase_:3;
    };

    template <typename T1, typename T2>
      BaseLease<T1,T2>::BaseLease(const uint64_t lease_id, const int64_t now):
        BaseObject<T1>(now),
        lease_id_(lease_id),
        status_(SERVICE_STATUS_OFFLINE),
        wait_free_phase_(OBJECT_WAIT_FREE_PHASE_NONE)
    {

    }

    template <typename T1, typename T2>
      BaseLease<T1,T2>::~BaseLease()
      {

      }

    template <typename T1, typename T2>
      bool BaseLease<T1, T2>::has_valid_lease(const int64_t now) const
      {
        return now < this->get();
      }

    template <typename T1, typename T2>
      bool BaseLease<T1, T2>::renew(const T2& info, const int64_t now, const int32_t times)
      {
        UNUSED(info);
        bool result = has_valid_lease(now);
        if (result)
          this->set(now, times);
        return  result;
      }

    template <typename T1, typename T2>
      bool BaseLease<T1, T2>::operator < (const BaseLease<T1, T2>& value) const
      {
        return lease_id_ < value.lease_id_;
      }

    template <typename T1, typename T2, template < typename X1, typename X2> class LeaseEntry >
      class LeaseManager
      {
        public:
          typedef LeaseEntry<T1, T2> Entry;
          struct EntryCompare
          {
            bool operator () (const Entry* left, const Entry* right) const
            {
              assert(NULL != left);
              assert(NULL != right);
              return left->id() < right->id();
            }
          };
          typedef TfsSortedVector<Entry*,EntryCompare> LEASE_ENTRY_SET;
          typedef typename LEASE_ENTRY_SET::iterator LEASE_ENTRY_SET_ITER;
        public:
          LeaseManager(T1& manager, const int32_t max_init_entry_num, const int32_t wait_free_wait_time_ms, const int32_t wait_clear_wait_time_ms);
          virtual ~LeaseManager();

          int apply(const T2& info, const int64_t now, const int32_t times);
          int giveup(const int64_t now, const uint64_t lease_id);
          int renew(const T2& info, const int64_t now, const int32_t times);
          bool has_valid_lease(const int64_t now, const uint64_t lease_id) const;

          Entry* get(const uint64_t lease_id) const;
          int64_t size() const;
          int64_t wait_free_size() const;

          int timeout(const int64_t now);
          int gc(const int64_t now);
          int traverse(const uint64_t start, ArrayHelper<Entry*>& leases, bool& all_over) const;
          virtual Entry* malloc(const T2& info, const int64_t now, T1& manager) = 0;
          virtual void free(Entry* entry) = 0;
        protected:
          Entry* get_(const uint64_t lease_id) const;
          int traverse_(const uint64_t start, ArrayHelper<Entry*>& leases, bool& all_over) const;

        protected:
          T1& manager_;
          mutable RWLock rwmutex_;
          LEASE_ENTRY_SET leases_;
          LEASE_ENTRY_SET wait_free_leases_;
          uint64_t last_traverse_lease_id_;
          int32_t wait_free_wait_time_ms_;
          int32_t wait_clear_wait_time_ms_;
      };

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      LeaseManager<T1, T2, LeaseEntry>::LeaseManager(T1& manager, const int32_t max_init_entry_num, const int32_t wait_free_wait_time_ms, const int32_t wait_clear_wait_time_ms):
        manager_(manager),
        leases_(max_init_entry_num, 256, 0.1),
        wait_free_leases_(max_init_entry_num, 256, 0.1),
        last_traverse_lease_id_(0),
        wait_free_wait_time_ms_(wait_free_wait_time_ms),
        wait_clear_wait_time_ms_(wait_clear_wait_time_ms)
    {

    }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      LeaseManager<T1, T2, LeaseEntry>::~LeaseManager()
      {
        RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
        LEASE_ENTRY_SET_ITER iter = leases_.begin();
        for (; iter != leases_.end(); ++iter)
        {
          (*iter)->free();
        }
        iter = wait_free_leases_.begin();
        for (; iter != wait_free_leases_.end(); ++iter)
        {
          (*iter)->free();
        }
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1, T2, LeaseEntry>::apply(const T2& info, const int64_t now, const int32_t times)
      {
        bool reset = false;
        Entry query(info.id_, 0);
        rwmutex_.wrlock();
        LEASE_ENTRY_SET_ITER iter = leases_.find(&query);
        Entry* lease = leases_.end() == iter ? NULL : (*iter);
        int32_t ret = (NULL == lease) ? TFS_SUCCESS : EXIT_APPLY_LEASE_ALREADY_ISSUED;
        if (TFS_SUCCESS == ret)
        {
          iter  = wait_free_leases_.find(&query);
          lease = wait_free_leases_.end() == iter ? NULL : (*iter);
          reset = (NULL != lease && (OBJECT_WAIT_FREE_PHASE_CLEAR == lease->get_wait_free_phase()));
          if (reset)
          {
            wait_free_leases_.erase(&query);
          }
          else
          {
            lease = this->malloc(info, now, manager_);
            assert(NULL != lease);
            Entry* result = NULL;
            ret = leases_.insert_unique(result, lease);
            assert(TFS_SUCCESS == ret);
            assert(NULL != result);
          }
        }
        rwmutex_.unlock();

        if (TFS_SUCCESS == ret)
        {
          assert(NULL != lease);
          lease->set(now, times);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = lease->has_valid_lease(now) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
        }

        if (TFS_SUCCESS == ret)
        {
          if (reset)
          {
            lease->reset(info, now, manager_);
          }
          lease->set_status(SERVICE_STATUS_ONLINE);
        }
        return ret;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1, T2, LeaseEntry>::giveup(const int64_t now, const uint64_t lease_id)
      {
        rwmutex_.wrlock();
        Entry query(lease_id, 0);
        Entry* lease= leases_.erase(&query);
        int32_t ret = (NULL == lease) ? EXIT_LEASE_NOT_EXIST : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          Entry* result = NULL;
          ret = wait_free_leases_.insert_unique(result, lease);
          assert(TFS_SUCCESS == ret);
          assert(NULL != result);
        }
        rwmutex_.unlock();
        if (TFS_SUCCESS == ret)
        {
          lease->set_status(SERVICE_STATUS_OFFLINE);
          lease->set_wait_free_phase(OBJECT_WAIT_FREE_PHASE_CLEAR);
          lease->set(now, wait_clear_wait_time_ms_);
          lease->callback(NULL, manager_);
        }
        return ret;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1, T2, LeaseEntry>::renew(const T2& info, const int64_t now, const int32_t times)
      {
        assert(times > 0);
        rwmutex_.rdlock();
        Entry* lease = NULL;
        Entry query(info.id_, 0);
        LEASE_ENTRY_SET_ITER iter = leases_.find(&query);
        int32_t ret = (leases_.end() == iter) ? EXIT_LEASE_NOT_EXIST : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          lease = (*iter);
          ret = lease->has_valid_lease(now) ? TFS_SUCCESS : EXIT_LEASE_EXPIRED;
        }
        rwmutex_.unlock();
        if (TFS_SUCCESS == ret)
        {
          lease->renew(info, now, times);
        }
        return ret;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      bool LeaseManager<T1, T2, LeaseEntry>::has_valid_lease(const int64_t now, const uint64_t lease_id) const
      {
        Entry query(lease_id, 0);
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        LEASE_ENTRY_SET_ITER iter = leases_.find(&query);
        Entry* lease = (iter == leases_.end()) ? NULL : (*iter);
        return (NULL == lease) ? false : lease->has_valid_lease(now);
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      LeaseEntry<T1, T2>* LeaseManager<T1, T2, LeaseEntry>::get(const uint64_t lease_id) const
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        return get_(lease_id);
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int64_t LeaseManager<T1, T2, LeaseEntry>::size() const
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        return leases_.size();
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int64_t LeaseManager<T1, T2, LeaseEntry>::wait_free_size() const
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        return wait_free_leases_.size();
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1, T2, LeaseEntry>::timeout(const int64_t now)
      {
        Entry* lease = NULL;
        int32_t expire_count = 0;
        uint64_t last_traverse_lease_id  = INVALID_LEASE_ID;
        const int32_t MAX_TRAVERSE_COUNT = 1024;
        Entry* entry[MAX_TRAVERSE_COUNT];
        ArrayHelper<Entry*> helper(MAX_TRAVERSE_COUNT, entry);
        bool all_over = false;
        while (!all_over)
        {
          traverse(last_traverse_lease_id, helper, all_over);
          for (int64_t index = 0; index < helper.get_array_index(); ++index)
          {
            lease = *helper.at(index);
            assert(NULL != lease);
            last_traverse_lease_id = lease->id();
            if (lease->expire(now))
            {
              ++expire_count;
              giveup(now, last_traverse_lease_id);
            }
          }
        }
        return expire_count;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1, T2, LeaseEntry>::gc(const int64_t now)
      {
        int32_t free_count = 0;
        const int32_t MAX_GC_COUNT = 32;
        Entry* lease= NULL;
        Entry* entry[MAX_GC_COUNT];
        ArrayHelper<Entry*> helper(MAX_GC_COUNT, entry);
        rwmutex_.rdlock();
        LEASE_ENTRY_SET_ITER iter = wait_free_leases_.begin();
        for (; iter != wait_free_leases_.end() && helper.get_array_index() < MAX_GC_COUNT; ++iter)
        {
          lease = (*iter);
          assert(NULL != lease);
          if (lease->expire(now))
            helper.push_back(lease);
        }
        rwmutex_.unlock();

        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          lease = *helper.at(index);
          assert(NULL != lease);
          if (OBJECT_WAIT_FREE_PHASE_CLEAR == lease->get_wait_free_phase())
          {
            lease->callback(NULL, manager_);
            lease->set_wait_free_phase(OBJECT_WAIT_FREE_PHASE_FREE);
            lease->set(now, wait_free_wait_time_ms_);
          }
          else
          {
            ++free_count;
            rwmutex_.wrlock();
            wait_free_leases_.erase(lease);
            rwmutex_.unlock();
            lease->free();
          }
        }
        return free_count;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1,T2, LeaseEntry>::traverse(const uint64_t start, ArrayHelper<Entry*>& leases, bool& all_over) const
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        return traverse_(start, leases, all_over);
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      LeaseEntry<T1, T2>* LeaseManager<T1, T2, LeaseEntry>::get_(const uint64_t lease_id) const
      {
        Entry query(lease_id, 0);
        Entry* lease= leases_.find(&query);
        return lease;
      }

    template <typename T1, typename T2, template <typename X1, typename X2> class LeaseEntry>
      int LeaseManager<T1,T2, LeaseEntry>::traverse_(const uint64_t start, ArrayHelper<Entry*>& leases, bool& all_over) const
      {
        Entry query(start, 0);
        LEASE_ENTRY_SET_ITER iter = INVALID_LEASE_ID == start ? leases_.begin() : leases_.upper_bound(&query);
        for (; iter != leases_.end() && leases.get_array_index() < leases.get_array_size(); ++iter)
        {
          leases.push_back((*iter));
        }
        all_over = leases.get_array_index() < leases.get_array_size();
        return leases.get_array_index();
      }

    //gc
    template <typename T1, template <typename X1> class Entry>
      class GCObjectManager
      {
        typedef typename std::set<Entry<T1>* > ENTRY_SET;
        typedef typename ENTRY_SET::iterator ENTRY_SET_ITER;
        typedef std::list<Entry<T1>* > ENTRY_LIST;
        typedef typename ENTRY_LIST::iterator ENTRY_LIST_ITER;
        public:
        GCObjectManager(T1& manager, const int32_t wait_free_wait_time_ms, const int32_t wait_clear_wait_time_ms):
          manager_(manager),
          wait_free_list_size_(0),
          wait_free_wait_time_ms_(wait_free_wait_time_ms),
          wait_clear_wait_time_ms_(wait_clear_wait_time_ms) {}
        int insert(Entry<T1>* entry, const int64_t now);
        int gc(const int64_t now);
        int64_t size() const;
        virtual ~GCObjectManager();
        private:
        T1& manager_;
        ENTRY_SET wait_clear_list_;
        ENTRY_LIST wait_free_list_;
        tbutil::Mutex mutex_;
        int64_t wait_free_list_size_;
        int32_t wait_free_wait_time_ms_;
        int32_t wait_clear_wait_time_ms_;
      };

    template <typename T1, template <typename X1> class Entry>
      GCObjectManager<T1, Entry>::~GCObjectManager()
      {
        Entry<T1>* entry = NULL;
        tbutil::Mutex::Lock lock(mutex_);
        ENTRY_SET_ITER iter = wait_clear_list_.begin();
        for (; iter != wait_clear_list_.end(); ++iter)
        {
          entry = (*iter);
          assert(NULL != entry);
          entry->free();
        }

        ENTRY_LIST_ITER it = wait_free_list_.begin();
        for (; it != wait_free_list_.end(); ++it)
        {
          entry = (*it);
          assert(NULL != entry);
          entry->free();
        }
      }

    template <typename T1, template <typename X1> class Entry>
      int GCObjectManager<T1, Entry>::insert(Entry<T1> * entry, const int64_t now)
      {
        int32_t ret = (NULL != entry) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          tbutil::Mutex::Lock lock(mutex_);
          std::pair<ENTRY_SET_ITER, bool> res = wait_clear_list_.insert(entry);
          if (!res.second)
            TBSYS_LOG(WARN, "object %p is exist", entry);
          else
            entry->set(now, wait_clear_wait_time_ms_);
        }
        return ret;
      }

    template <typename T1, template <typename X1> class Entry>
      int GCObjectManager<T1, Entry>::gc(const int64_t now)
      {
        Entry<T1>* entry = NULL;
        static const int32_t MAX_GC_COUNT = 1024;
        Entry<T1>* free_objects[MAX_GC_COUNT];
        Entry<T1>* clear_objects[MAX_GC_COUNT];
        ArrayHelper< Entry<T1>* > free_helper(MAX_GC_COUNT, free_objects);
        ArrayHelper< Entry<T1>* > clear_helper(MAX_GC_COUNT, clear_objects);
        ENTRY_SET_ITER iter = wait_clear_list_.begin();
        mutex_.lock();
        while (iter != wait_clear_list_.end() && clear_helper.get_array_index() < MAX_GC_COUNT)
        {
          entry = (*iter);
          assert(NULL != entry);
          if (entry->expire(now))
          {
            clear_helper.push_back(entry);
            wait_clear_list_.erase(iter++);
          }
          else
          {
            ++iter;
          }
        }

        ENTRY_LIST_ITER it = wait_free_list_.begin();
        while (it != wait_free_list_.end() && free_helper.get_array_index() < MAX_GC_COUNT)
        {
          entry = (*it);
          assert(NULL != entry);
          if (entry->expire(now))
          {
            free_helper.push_back(entry);
            wait_free_list_.erase(it++);
          }
          else
          {
            ++it;
          }
        }

        wait_free_list_size_ += clear_helper.get_array_index();
        wait_free_list_size_ -= free_helper.get_array_index();
        mutex_.unlock();

        int64_t index = 0;
        for (index = 0; index < clear_helper.get_array_index(); ++index)
        {
          entry = *clear_helper.at(index);
          assert(NULL != entry);
          entry->callback(NULL, manager_);
          mutex_.lock();
          entry->set(now, wait_free_wait_time_ms_);
          wait_free_list_.push_back(entry);
          mutex_.unlock();
        }

        for (index = 0; index < free_helper.get_array_index(); ++index)
        {
          entry = *free_helper.at(index);
          assert(NULL != entry);
          entry->free();
        }
        return free_helper.get_array_index();
      }

    template <typename T1, template <typename X1> class Entry>
      int64_t GCObjectManager<T1, Entry>::size() const
      {
        return  wait_free_list_size_ + wait_clear_list_.size();
      }
  }//end namesapce common
}//end namespace tfs

#endif
