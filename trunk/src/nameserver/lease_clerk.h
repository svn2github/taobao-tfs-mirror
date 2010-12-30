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
#ifndef TFS_NAMESERVER_LEASE_CLERK_H_
#define TFS_NAMESERVER_LEASE_CLERK_H_

#include <tbsys.h>
#include <Monitor.h>
#include <Mutex.h>
#include "ns_define.h"

namespace tfs
{
  namespace nameserver
  {
    struct WriteLease
    {
      enum LeaseStatus
      {
        WRITING = 0x00,
        DONE,
        FAILED,
        EXPIRED,
        CANCELED,
        OBSOLETE
      };
      int64_t last_write_time_;
      int64_t expire_time_;
      uint32_t lease_id_;
      LeaseStatus status_;

      tbutil::Monitor<tbutil::Mutex> monitor_;

      void dump(const uint32_t block_id, const bool is_valid, const int64_t current_wait_count,
          const int64_t max_wait_count) const
      {
TBSYS_LOG      (DEBUG, "block id(%u) last wirte time(%"PRI64_PREFIX"d), expire time(%"PRI64_PREFIX"d),"
          "now time(%"PRI64_PREFIX"d), lease id(%u), is valid(%s), current wait count(%"PRI64_PREFIX"d),"
          "max wait count(%"PRI64_PREFIX"d) status(%s)",
          block_id, last_write_time_, expire_time_, tbsys::CTimeUtil::getTime(), lease_id_, is_valid ? "true" : "false", current_wait_count, max_wait_count,
          status_ == WRITING ? "writing" : status_ == DONE ? "done" : status_ == FAILED ? "failed" :
          status_ == EXPIRED ? "expired" : status_ == CANCELED ? "canceled" : status_ == OBSOLETE ? "obsolet" : "unkown" );
    }
    static atomic_t global_lease_id_;
    static uint32_t get_new_lease_id();
    static const uint32_t LEASE_EXPIRE_TIME = 3000;
    static const uint32_t INVALID_LEASE = 0;
  };

  class LeaseClerk
  {
    typedef __gnu_cxx ::hash_map<uint32_t, WriteLease*, __gnu_cxx::hash<uint32_t> > LEASE_MAP;
    typedef LEASE_MAP::iterator LEASE_MAP_ITER;
    typedef LEASE_MAP::const_iterator LEASE_MAP_CONST_ITER;
  public:
    LeaseClerk();
    virtual ~LeaseClerk();

  public:
    uint32_t register_lease(const uint32_t block_id);
    bool unregister_lease(const uint32_t block_id);
    bool cancel_lease(const uint32_t block_id);
    bool write_commit(const uint32_t block_id, const uint32_t lease_id, const WriteLease::LeaseStatus status);
    bool write_commit(common::BlockInfo* block_info, common::BlockInfo* new_block_info, const uint32_t lease_id);
    bool has_valid_lease(const uint32_t block_id) const;
    bool if_need_clear() const;
    void clear();

  private:
    bool is_valid_lease(const WriteLease* lease) const;
    bool reset(WriteLease& lease);
    bool change(const uint32_t block_id, const WriteLease::LeaseStatus status);
    void destroy();
    void inc_wait_count();
    void dec_wait_count();

    WriteLease* get_lease(const uint32_t block_id) const;

  private:
    DISALLOW_COPY_AND_ASSIGN( LeaseClerk);
    LEASE_MAP lease_map_;
    tbutil::Mutex mutex_;
    int64_t wait_count_;
  };
}
}

#endif 

