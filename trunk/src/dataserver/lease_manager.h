/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: lease_manager.h 2140 2012-08-16 15:42:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_LEASE_MANAGER_H_
#define TFS_DATASERVER_LEASE_MANAGER_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "data_file.h"
#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {
    typedef enum _LeaseType
    {
      LEASE_TYPE_UNLINK = 0,
      LEASE_TYPE_WRITE
    }LeaseType;

    struct LeaseMemberInfo
    {
      uint64_t server_;
      int32_t  status_;
      common::BlockInfo info_;
    };

    struct LeaseId
    {
      uint64_t lease_id_;
      uint64_t file_id_;
      uint32_t block_;
      LeaseId(const uint64_t lease_id, const uint64_t file_id, const uint32_t block) :
        lease_id_(lease_id), file_id_(file_id), block_(block) {}
      bool operator < (const LeaseId& lease) const
      {
        if (lease_id_ < lease.lease_id_)
          return true;
        if (lease_id_ > lease.lease_id_)
          return false;
        if (file_id_ < lease.file_id_)
          return true;
        if (file_id_ > lease.file_id_)
          return false;
        return block_< lease.block_;
      }
    };

    class Lease
    {
    public:
      Lease(const LeaseId& lease_id, const int64_t now, const std::vector<uint64_t>& servers);
      virtual ~Lease() {}

      inline uint32_t inc_ref() { return common::atomic_inc(&ref_count_);}
      inline uint32_t dec_ref() { return common::atomic_dec(&ref_count_);}
      inline void upate_last_time(const int64_t now) { last_update_time_ = now;}
      inline bool timeout(const int64_t now) { return now > last_update_time_ + common::SYSPARAM_DATASERVER.expire_data_file_time_;}
      bool check_all_successful() const;
      bool check_has_version_conflict() const;
      int get_member_info(std::vector<std::pair<uint64_t, common::BlockInfo> >& members) const;
      int update_member_info(const uint64_t server, const common::BlockInfo& info, const int32_t status);
      void reset_member_info();
      void dump(const int32_t level, const char* const format = NULL);

      LeaseId lease_id_;
      int64_t  last_update_time_;
      uint32_t ref_count_;
      LeaseMemberInfo members_[common::MAX_REPLICATION_NUM];
    private:
      DISALLOW_COPY_AND_ASSIGN(Lease);
    };

    class WriteLease: public Lease
    {
    public:
      WriteLease(const LeaseId& lease_id, const int64_t now, const std::vector<uint64_t>& servers);
      virtual ~WriteLease() {}
      DataFile& get_data_file() { return data_file_;}
    private:
      DataFile data_file_;
      DISALLOW_COPY_AND_ASSIGN(WriteLease);
    };

    class LeaseManager
    {
      typedef std::map<LeaseId, Lease*> LEASE_MAP;
      typedef LEASE_MAP::iterator LEASE_MAP_ITER;
      typedef LEASE_MAP::const_iterator LEASE_MAP_CONST_ITER;
    public:
      LeaseManager();
      virtual ~LeaseManager();

      Lease* generation(LeaseId& lease_id, const int64_t now, const int8_t type, const std::vector<uint64_t>& servers);
      Lease* get(const LeaseId& lease_id, const int64_t now) const;
      void put(Lease* lease);
      int remove(const LeaseId& lease_id);
      int timeout(const int64_t now);
      int64_t size() const;
      bool has_out_of_limit() const;
      uint64_t gen_lease_id();//这个函数的作用是: 为了向2.4以下的版本赚容，只能被create_file_number调用
    private:
      uint64_t base_lease_id_;
      LEASE_MAP leases_;
      mutable common::RWLock rwmutex_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/

#endif
