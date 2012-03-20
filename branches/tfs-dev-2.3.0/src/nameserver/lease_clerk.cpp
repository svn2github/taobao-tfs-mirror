/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lease_clerk.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <stdint.h>
#include <limits.h>
#include <Handle.h>
#include <Memory.hpp>
#include "common/func.h"
#include "lease_clerk.h"
#include "global_factory.h"
#include "common/parameter.h"
#include "common/config_item.h"

#ifndef UINT32_MAX
#define UINT32_MAX 4294967295U
#endif

using namespace std;
using namespace tfs::common;
using namespace tbutil;

namespace tfs
{
  namespace nameserver
  {
    volatile uint32_t LeaseFactory::lease_id_factory_ = 0;
    volatile uint16_t LeaseFactory::gwait_count_ = 0;
    int16_t LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS = 3000;//ms
    int32_t LeaseEntry::LEASE_EXPIRE_TIME_MS = LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS;
    int64_t LeaseEntry::LEASE_EXPIRE_REMOVE_TIME_MS = 1 * LeaseEntry::LEASE_EXPIRE_TIME_MS * 3600;
    LeaseFactory LeaseFactory::instance_;

    LeaseEntry::LeaseEntry(LeaseClerkPtr clerk, uint32_t lease_id, int64_t client, LeaseType type):
      clerk_(clerk),
      last_update_time_(tbutil::Time::now(tbutil::Time::Monotonic)),
      expire_time_(last_update_time_ + tbutil::Time::milliSeconds(LEASE_EXPIRE_TIME_MS)),
      client_(client),
      lease_id_(lease_id),
      type_(type),
      status_(LEASE_STATUS_RUNNING)
    {

    }

    LeaseEntry::~LeaseEntry()
    {

    }

    uint32_t LeaseEntry::id() const
    {
      return lease_id_;
    }

    int64_t LeaseEntry::client() const
    {
      return client_;
    }

    LeaseType LeaseEntry::type() const
    {
      return static_cast<LeaseType>(type_);
    }

    LeaseStatus LeaseEntry::status() const
    {
      return static_cast<LeaseStatus>(status_);
    }

    void LeaseEntry::runTimerTask()
    {
      TBSYS_LOG(DEBUG, "lease id: %u client id: %"PRI64_PREFIX"d exipired", lease_id_, client_);
      clerk_->expire(client_);
    }

    void LeaseEntry::reset(uint32_t lease_id, LeaseType type)
    {
      lease_id_ = lease_id;
      last_update_time_ = tbutil::Time::now(tbutil::Time::Monotonic);
      expire_time_ = last_update_time_ + tbutil::Time::milliSeconds(LEASE_EXPIRE_TIME_MS);
      type_ = type;
      status_ = LEASE_STATUS_RUNNING;
    }

    bool LeaseEntry::is_valid_lease() const
    {
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      TBSYS_LOG(DEBUG, "is valid lease status %d:%s time: %s" , status_, status_ == LEASE_STATUS_RUNNING ? "true" : "false",
         now < expire_time_ ? "true" : "false");
      return (status_ == LEASE_STATUS_RUNNING
          && now < expire_time_);
    }

    bool LeaseEntry::wait_for_expire() const
    {
      if (status_ > LEASE_STATUS_RUNNING)
        return true;
      Time time_out = expire_time_ - tbutil::Time::now(tbutil::Time::Monotonic);
      if (time_out >= 0 )
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(*this);
        return timedWait(time_out);
      }
      else
      {
        return true;
      }
    }

    void LeaseEntry::change(LeaseStatus status)
    {
      status_ = status;
    }

    bool LeaseEntry::is_remove(tbutil::Time& now, bool check_time) const
    {
      if (check_time)
      {
        TBSYS_LOG(DEBUG, "%"PRI64_PREFIX"d, %d %d", LEASE_EXPIRE_REMOVE_TIME_MS, LEASE_EXPIRE_DEFAULT_TIME_MS, LEASE_EXPIRE_TIME_MS);
        return ((status_ >= LEASE_STATUS_FINISH)
            && (now.toMilliSeconds() >= (expire_time_.toMilliSeconds() + LEASE_EXPIRE_REMOVE_TIME_MS)));
      }
      return (status_ >= LEASE_STATUS_FINISH);
    }

    void LeaseEntry::dump(int64_t id, bool is_valid, int64_t current_wait_count, int64_t max_wait_count) const
    {
      TBSYS_LOG(DEBUG, "id: %"PRI64_PREFIX"d,last update time: %"PRI64_PREFIX"d, expire time: %"PRI64_PREFIX"d,"
          "now time: %"PRI64_PREFIX"d, lease id: %u, is valid: %s, current wait count: %"PRI64_PREFIX"d,"
          "max wait count: %"PRI64_PREFIX"d status: %s",
          id, last_update_time_.toMicroSeconds(), expire_time_.toMicroSeconds(),
          tbutil::Time::now(tbutil::Time::Monotonic).toMicroSeconds(), lease_id_, is_valid ? "true" : "false",
          current_wait_count, max_wait_count,
          status_ == LEASE_STATUS_RUNNING? "runing" : status_ == LEASE_STATUS_FINISH? "finsih"
          : status_ == LEASE_STATUS_FAILED ? "failed" : status_ == LEASE_STATUS_EXPIRED ? "expired"
          : status_ == LEASE_STATUS_CANCELED ? "canceled" : status_ == LEASE_STATUS_OBSOLETE ? "obsolet" : "unkown" );
    }

    LeaseClerk::LeaseClerk(int32_t remove_threshold) :
      remove_threshold_(remove_threshold)
    {

    }

    LeaseClerk::~LeaseClerk()
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      LEASE_MAP_ITER iter = leases_.begin();
      while (iter != leases_.end())
      {
        iter->second->notify();
        leases_.erase(iter++);
      }
      leases_.clear();
    }

    void LeaseClerk::clear(bool check_time, bool force)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      int32_t size = static_cast<int32_t>(leases_.size());
      TBSYS_LOG(INFO, "prepare to cleanup lease, current lease map size: %u", size);
      tbutil::Time now = tbutil::Time::now(tbutil::Time::Monotonic);
      bool remove = true;
      while (remove)
      {
        remove = ((size >= remove_threshold_) || (force && size > 0));
        if (remove)
        {
          LEASE_MAP_ITER iter = leases_.begin();
          while (iter != leases_.end())
          {
            if ((iter->second->is_remove(now, check_time))
                || (force))
            {
              iter->second->notify();
              leases_.erase(iter++);
            }
            else
            {
              ++iter;
            }
          }
        }
        size = leases_.size();
     }
      tbutil::Time end = tbutil::Time::now(tbutil::Time::Monotonic);
      TBSYS_LOG(INFO, "cleanup lease complete, current lease map size: %zd, consume: %" PRI64_PREFIX "d",
          leases_.size(), (end - now).toMicroSeconds());
    }

    bool LeaseClerk::has_valid_lease(int64_t id)
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      LeaseEntryPtr lease = find(id);
      if (lease == 0)
      {
        //TBSYS_LOG(WARN, "lease not found by id: %"PRI64_PREFIX"d", id);
        return false;
      }
      return lease->is_valid_lease();
    }

    bool LeaseClerk::exist(int64_t id)
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      LeaseEntryPtr lease = find(id);
      return lease != 0;
    }

    uint32_t LeaseClerk::add(int64_t id)
    {
      TBSYS_LOG(DEBUG, "client: %"PRI64_PREFIX"d register lease", id);
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      {
        TBSYS_LOG(WARN, "%s", "add lease fail because nameserver destroyed");
        return INVALID_LEASE_ID;
      }

      mutex_.rdlock();
      uint32_t lease_id = INVALID_LEASE_ID;
      LeaseEntryPtr lease = 0;
      LEASE_MAP_ITER iter = leases_.find(id);
      if ((iter == leases_.end())
          || ( iter != leases_.end() && iter->second == 0))
      {
        mutex_.unlock();
        goto Next;
      }
      lease = iter->second;
      mutex_.unlock();

      if (!lease->is_valid_lease())
      {
        goto Renew;
      }

      if (LeaseFactory::gwait_count_ > SYSPARAM_NAMESERVER.max_wait_write_lease_)
      {
        TBSYS_LOG(WARN, "lease: %u, current wait thread: %d beyond max_wait: %d", lease->id(), LeaseFactory::gwait_count_,
            SYSPARAM_NAMESERVER.max_wait_write_lease_);
        return INVALID_LEASE_ID;
      }

      atomic_inc(&LeaseFactory::gwait_count_);

      //lease was found by id, we must be wait
      lease->wait_for_expire();

      atomic_dec(&LeaseFactory::gwait_count_);

      if (!lease->is_valid_lease())
      {
        goto Renew;
      }
      return INVALID_LEASE_ID;
  Next:
      {
        if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_YES)
        {
          TBSYS_LOG(WARN, "%s", "add lease fail because nameserver destroyed");
          return INVALID_LEASE_ID;
        }

        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        lease_id = LeaseFactory::new_lease_id();
        lease = new LeaseEntry(tbutil::Handle<LeaseClerk>::dynamicCast(this), lease_id, id);
        std::pair<LEASE_MAP_ITER, bool> res =
          leases_.insert(LEASE_MAP_ITER::value_type(id, lease));
        if (!res.second)
        {
          lease = 0;
          TBSYS_LOG(WARN, "id: %"PRI64_PREFIX"d has been lease" , id);
          return INVALID_LEASE_ID;
        }
        GFactory::get_timer()->schedule(lease, tbutil::Time::milliSeconds(LeaseEntry::LEASE_EXPIRE_TIME_MS));
        return lease_id;
      }
  Renew:
      {
        if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_YES)
        {
          TBSYS_LOG(WARN, "%s", "add lease fail because nameserver destroyed");
          return INVALID_LEASE_ID;
        }

        tbutil::Monitor<tbutil::Mutex>::Lock lock(*lease);
        if (lease->is_valid_lease())
        {
          TBSYS_LOG(WARN, "id: %"PRI64_PREFIX"d has been lease" , id);
          return INVALID_LEASE_ID;
        }
        lease_id = LeaseFactory::new_lease_id();
        lease->reset(lease_id);
        lease->notifyAll();
        GFactory::get_timer()->cancel(lease);
        GFactory::get_timer()->schedule(lease, tbutil::Time::milliSeconds(LeaseEntry::LEASE_EXPIRE_TIME_MS));
        return lease_id;
      }
      return INVALID_LEASE_ID;
    }

    bool LeaseClerk::remove(int64_t id)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      LEASE_MAP_ITER iter = leases_.find(id);
      if (iter != leases_.end())
      {
        GFactory::get_timer()->cancel(iter->second);
        iter->second = 0;
        leases_.erase(iter);
      }
      return true;
    }

    bool LeaseClerk::obsolete(int64_t id)
    {
      return change(id, LEASE_STATUS_OBSOLETE);
    }

    bool LeaseClerk::finish(int64_t id)
    {
      return change(id, LEASE_STATUS_FINISH);
    }

    bool LeaseClerk::failed(int64_t id)
    {
      return change(id, LEASE_STATUS_FAILED);
    }

    bool LeaseClerk::cancel(int64_t id)
    {
      return change(id, LEASE_STATUS_CANCELED);
    }

    bool LeaseClerk::expire(int64_t id)
    {
      return change(id, LEASE_STATUS_EXPIRED);
    }

    bool LeaseClerk::change(int64_t id, LeaseStatus status)
    {
      RWLock::Lock r_lock(mutex_, READ_LOCKER);
      LeaseEntryPtr lease = find(id);
      if (lease == 0)
      {
        TBSYS_LOG(ERROR, "lease not found by block id: %"PRI64_PREFIX"d", id);
        return false;
      }
      tbutil::Monitor<tbutil::Mutex>::Lock lock(*lease);
      lease->change(status);
      lease->notifyAll();
      GFactory::get_timer()->cancel(lease);
      return true;
    }

    LeaseEntryPtr LeaseClerk::find(int64_t id) const
    {
      LEASE_MAP_CONST_ITER iter = leases_.find(id);
      if (iter != leases_.end())
        return iter->second;
      return 0;
    }

    bool LeaseClerk::commit(int64_t id, uint32_t lease_id, LeaseStatus status)
    {
      LeaseEntryPtr lease = 0;
      {
        RWLock::Lock r_lock(mutex_, READ_LOCKER);
        lease = find(id);
        if (lease == 0)
        {
          TBSYS_LOG(WARN, "lease not found by id: %"PRI64_PREFIX"d", id);
          return false;
        }
      }

      TBSYS_LOG(DEBUG,"commit : %"PRI64_PREFIX"d, status: %d", id, status);

      Monitor<Mutex>::Lock lock(*lease);
      if (lease->id() != lease_id)
      {
        lease->notifyAll();
        TBSYS_LOG(ERROR, "id: %"PRI64_PREFIX"d lease id not match %u:%u", id, lease->id(), lease_id);
        return false;
      }

      if (!lease->is_valid_lease())
      {
        TBSYS_LOG(WARN, "id: %"PRI64_PREFIX"d has lease, but it: %u was invalid", id, lease_id);
        lease->change(LEASE_STATUS_EXPIRED);
        lease->notifyAll();
        return false;
      }

      lease->change(status);
      lease->notifyAll();
      TBSYS_LOG(DEBUG, "cancel ---------%u", lease->id());
      GFactory::get_timer()->cancel(lease);
      return true;
    }

    LeaseFactory::LeaseFactory():
      clerk_num_(32),
      clerk_(NULL)
    {

    }

    LeaseFactory::~LeaseFactory()
    {

    }

    int LeaseFactory::initialize(int32_t clerk_num)
    {
      clerk_num_ =  clerk_num <= 0 ? 32 : clerk_num > 1024 ? 1024 : clerk_num;
      int32_t total = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_CLEANUP_LEASE_THRESHOLD, 0xFA000);//102400
      int32_t remove_threshold = total / clerk_num_;
      tbsys::gDeleteA(clerk_);
      clerk_ = new LeaseClerkPtr[clerk_num_];
      for (int32_t i = 0; i < clerk_num_; i++)
      {
        clerk_[i] = new LeaseClerk(remove_threshold);
      }
      LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_MAX_LEASE_TIMEOUT, LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS);
      LeaseEntry::LEASE_EXPIRE_TIME_MS = LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS;
      int32_t expired = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_LEASE_EXPIRED_TIME, 1);
      LeaseEntry::LEASE_EXPIRE_REMOVE_TIME_MS = expired * 3600 * 1000 + LeaseEntry::LEASE_EXPIRE_DEFAULT_TIME_MS;
      return TFS_SUCCESS;
    }

    int LeaseFactory::wait_for_shut_down()
    {
      if (NULL != clerk_)
      {
        for (int32_t i = 0; i < clerk_num_; i++)
        {
          clerk_[i] = 0;
        }
        tbsys::gDeleteA(clerk_);
      }
      return TFS_SUCCESS;
    }

    void LeaseFactory::destroy()
    {
      if (NULL != clerk_)
      {
        bool check_time = false;
        bool force = true;
        for (int32_t i = 0; i < clerk_num_; i++)
        {
          clerk_[i]->clear(check_time, force);
        }
      }
    }

    uint32_t LeaseFactory::add(int64_t id)
    {
      return clerk_[id % clerk_num_]->add(id);
    }

    bool LeaseFactory::remove(int64_t id)
    {
      return clerk_[id % clerk_num_]->remove(id);
    }

    bool LeaseFactory::obsolete(int64_t id)
    {
      return clerk_[id % clerk_num_]->obsolete(id);
    }

    bool LeaseFactory::finish(int64_t id)
    {
      return clerk_[id % clerk_num_]->finish(id);
    }

    bool LeaseFactory::failed(int64_t id)
    {
      return clerk_[id % clerk_num_]->failed(id);
    }

    bool LeaseFactory::cancel(int64_t id)
    {
      return clerk_[id % clerk_num_]->cancel(id);
    }

    bool LeaseFactory::expire(int64_t id)
    {
      return clerk_[id % clerk_num_]->expire(id);
    }

    bool LeaseFactory::commit(int64_t id, uint32_t lease_id, LeaseStatus status)
    {
      return clerk_[id % clerk_num_]->commit(id, lease_id, status);
    }

    bool LeaseFactory::has_valid_lease(int64_t id) const
    {
      return clerk_[id % clerk_num_]->has_valid_lease(id);
    }

    bool LeaseFactory::exist(int64_t id) const
    {
      return clerk_[id % clerk_num_]->exist(id);
    }

    void LeaseFactory::clear()
    {
      bool check_time = false;
      bool force = true;
      for (int32_t i = 0; i < clerk_num_; ++i)
      {
        clerk_[i]->clear(check_time, force);
      }
    }

    uint32_t LeaseFactory::new_lease_id()
    {
      uint32_t lease_id = atomic_inc(&lease_id_factory_);
      if (lease_id > UINT32_MAX - 1)
      {
        lease_id = atomic_exchange(&lease_id_factory_, 1);
      }
      return lease_id;
    }
  }
}

