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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <limits.h>
#include <Memory.hpp>
#include "common/func.h"
#include "lease_clerk.h"
#include "block_collect.h"

using namespace tfs::common;
using namespace tbutil;
namespace tfs
{
  namespace nameserver
  {

    atomic_t WriteLease::global_lease_id_ =
      {
      1
      };

    uint32_t WriteLease::get_new_lease_id()
    {
      uint32_t lease_id = atomic_add_return(1, &global_lease_id_);
      /*if (lease_id > UINT32_MAX - 1)
       {
       atomic_set(global_lease_id_, 1);
       lease_id = atomic_add_return(1, &global_lease_id_);
       }*/
      return lease_id;
    }

    LeaseClerk::LeaseClerk() :
      wait_count_(0)
    {

    }

    LeaseClerk::~LeaseClerk()
    {
      destroy();
    }

    void LeaseClerk::destroy()
    {
      tbutil::Mutex::Lock lock(mutex_);
      LEASE_MAP_ITER it = lease_map_.begin();
      while (it != lease_map_.end())
      {
        tbsys::gDelete(it->second);
        ++it;
      }
      lease_map_.clear();
    }

    bool LeaseClerk::if_need_clear() const
    {
      return ((Func::hour_range(SYSPARAM_NAMESERVER.cleanup_lease_time_lower_,
          SYSPARAM_NAMESERVER.cleanup_lease_time_upper_)) && (lease_map_.size()
          > static_cast<uint32_t> (SYSPARAM_NAMESERVER.cleanup_lease_threshold_)));
    }

    void LeaseClerk::clear()
    {
      vector < uint32_t > blocks;
      time_t now = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "prepare to cleanup lease, current lease map size(%u)", lease_map_.size());

      tbutil::Mutex::Lock lock(mutex_);
      LEASE_MAP_ITER iter = lease_map_.begin();
      while (iter != lease_map_.end())
      {
        if (iter->second != NULL)
        {
          if ((iter->second->status_ >= WriteLease::DONE) && (now - iter->second->expire_time_
              > static_cast<time_t> (WriteLease::LEASE_EXPIRE_TIME * 1000 * 3600)))
          {
            blocks.push_back(iter->first);
          }
        }
        if (static_cast<int32_t> (blocks.size()) > SYSPARAM_NAMESERVER.cleanup_lease_count_)
          break;

        ++iter;
      }

      TBSYS_LOG(INFO, "found obselete lease size(%u)", blocks.size());

      for (uint32_t i = 0; i < blocks.size(); ++i)
      {
        iter = lease_map_.find(blocks[i]);
        if (iter != lease_map_.end())
        {
          tbsys::gDelete(iter->second);
          lease_map_.erase(iter);
        }
      }

      time_t end = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "cleanup lease complete, current lease map size(%u), consume(%" PRI64_PREFIX "u)", lease_map_.size(), end - now);
    }

    WriteLease* LeaseClerk::get_lease(const uint32_t block_id) const
    {
      LEASE_MAP_CONST_ITER iter = lease_map_.find(block_id);
      if (iter != lease_map_.end())
        return iter->second;
      return NULL;
    }

    bool LeaseClerk::is_valid_lease(const WriteLease* lease) const
    {
      if (lease == NULL)
      {
        TBSYS_LOG(ERROR, "lease not found");
        return false;
      }
      return (lease->status_ == WriteLease::WRITING && tbsys::CTimeUtil::getTime() < lease->expire_time_);
    }

    bool LeaseClerk::has_valid_lease(const uint32_t block_id) const
    {
      const WriteLease* lease = get_lease(block_id);
      if (lease == NULL)
      {
        TBSYS_LOG(WARN, "lease not found by block id(%u)", block_id);
        return false;
      }
      return is_valid_lease(lease);
    }

    bool LeaseClerk::reset(WriteLease& lease)
    {
      lease.lease_id_ = WriteLease::get_new_lease_id();
      lease.last_write_time_ = tbsys::CTimeUtil::getTime();
      lease.expire_time_ = lease.last_write_time_ + WriteLease::LEASE_EXPIRE_TIME * 1000;
      lease.status_ = WriteLease::WRITING;
      return true;
    }

    void LeaseClerk::inc_wait_count()
    {
      tbutil::Mutex::Lock lock(mutex_);
      ++wait_count_;
    }

    void LeaseClerk::dec_wait_count()
    {
      tbutil::Mutex::Lock lock(mutex_);
      --wait_count_;
    }

    uint32_t LeaseClerk::register_lease(const uint32_t block_id)
    {
      mutex_.lock();
      int64_t wait_count = wait_count_;
      WriteLease* lease = get_lease(block_id);
      if (lease == NULL)
      {
        lease = new WriteLease();
        reset(*lease);
        lease_map_.insert(LEASE_MAP::value_type(block_id, lease));
        mutex_.unlock();
        return lease->lease_id_;
      }

      mutex_.unlock();
      Monitor<Mutex>::Lock lock(lease->monitor_);
      lease->dump(block_id, is_valid_lease(lease), wait_count, SYSPARAM_NAMESERVER.max_wait_write_lease_);
      if (!is_valid_lease(lease))
      {
        reset(*lease);
        lease->monitor_.notifyAll();
        return lease->lease_id_;
      }

      if (wait_count > SYSPARAM_NAMESERVER.max_wait_write_lease_)
      {
        TBSYS_LOG(WARN, "lease(%u), current wait thread(%"PRI64_PREFIX"d) beyond max_wait(%d)", lease->lease_id_, wait_count,
            SYSPARAM_NAMESERVER.max_wait_write_lease_);
        return WriteLease::INVALID_LEASE;
      }

      inc_wait_count();

      Time time_out = Time::milliSeconds(WriteLease::LEASE_EXPIRE_TIME);
      bool ret = lease->monitor_.timedWait(time_out);

      dec_wait_count();

      TBSYS_LOG(DEBUG, "register_lease block(%u) wait end ret(%d), valid(%d)", block_id, ret, is_valid_lease(lease));

      if (!is_valid_lease(lease))
      {
        reset(*lease);
        lease->monitor_.notifyAll();
        return lease->lease_id_;
      }
      return WriteLease::INVALID_LEASE;
    }

    bool LeaseClerk::unregister_lease(const uint32_t block_id)
    {
      return change(block_id, WriteLease::OBSOLETE);
    }

    bool LeaseClerk::cancel_lease(const uint32_t block_id)
    {
      return change(block_id, WriteLease::CANCELED);
    }

    bool LeaseClerk::change(const uint32_t block_id, const WriteLease::LeaseStatus status)
    {
      Mutex::Lock lock(mutex_);
      WriteLease* lease = get_lease(block_id);
      if (lease == NULL)
      {
        TBSYS_LOG(ERROR, "lease not found by block id(%u)", block_id);
        return false;
      }

      Monitor<Mutex>::Lock lease_lock(lease->monitor_);
      lease->status_ = status;
      lease->monitor_.notifyAll();
      return true;
    }

    bool LeaseClerk::write_commit(uint32_t block_id, uint32_t lease_id, WriteLease::LeaseStatus status)
    {
      mutex_.lock();
      WriteLease* lease = get_lease(block_id);
      mutex_.unlock();

      if (lease == NULL)
      {
        TBSYS_LOG(ERROR, "lease not found by block id(%u)", block_id);
        return false;
      }

      Monitor<Mutex>::Lock lock(lease->monitor_);
      if (lease->lease_id_ != lease_id)
      {
        lease->monitor_.notifyAll();
        TBSYS_LOG(ERROR, "block(%u) lease id not match (%u,%u)", block_id, lease->lease_id_, lease_id);
        return false;
      }

      if (!is_valid_lease(lease))
      {
        TBSYS_LOG(ERROR, "block(%u) lease(%u) expired", block_id, lease->lease_id_);
        lease->dump(block_id, is_valid_lease(lease), wait_count_, SYSPARAM_NAMESERVER.max_wait_write_lease_);
        lease->status_ = WriteLease::EXPIRED;
        lease->monitor_.notifyAll();
        return false;
      }
      lease->status_ = status;
      lease->monitor_.notifyAll();
      return true;
    }

    bool LeaseClerk::write_commit(BlockInfo* block_info, BlockInfo* new_block_info, const uint32_t lease_id)
    {
      if ((block_info == NULL) || (new_block_info == NULL) || (lease_id == WriteLease::INVALID_LEASE)
          || (block_info->block_id_ != new_block_info->block_id_))
      {
        return false;
      }

      mutex_.lock();
      WriteLease* lease = get_lease(block_info->block_id_);
      mutex_.unlock();

      if (lease == NULL)
      {
        TBSYS_LOG(ERROR, "lease not found by block id(%u)", block_info->block_id_);
        return false;
      }

      Monitor<Mutex>::Lock lock(lease->monitor_);

      bool ret = false;
      if ((lease->lease_id_ == lease_id) && (block_info->version_ + 1 == new_block_info->version_))
      {
        if (is_valid_lease(lease))
        {
          memcpy(block_info, new_block_info, BLOCKINFO_SIZE);
          lease->status_ = WriteLease::DONE;
          if (BlockCollect::is_full(new_block_info->size_))
          {
            lease->status_ = WriteLease::OBSOLETE;
          }
          ret = true;
        }
        else
        {
          TBSYS_LOG(ERROR, "block(%u) lease(%u) expired", block_info->block_id_, lease->lease_id_);
          lease->dump(block_info->block_id_, is_valid_lease(lease), wait_count_,
              SYSPARAM_NAMESERVER.max_wait_write_lease_);
          lease->status_ = WriteLease::EXPIRED;
        }
        lease->monitor_.notifyAll();
      }
      else
      {
        TBSYS_LOG(ERROR, "block(%u) lease id not match(%u, %u) or version not match(%d, %d)", lease->lease_id_,
            lease_id, block_info->version_, new_block_info->version_);
        lease->monitor_.notifyAll();
      }
      return ret;
    }
  }
}

