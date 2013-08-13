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
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "data_file.h"
#include "ds_define.h"

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
      common::BlockInfoV2 info_;
    };

    struct LeaseId
    {
      uint64_t lease_id_;
      uint64_t file_id_;
      uint64_t block_;
      LeaseId(const uint64_t lease_id, const uint64_t file_id, const uint64_t block) :
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
      Lease(const LeaseId& lease_id, const int64_t now_us, const common::VUINT64& servers);
      virtual ~Lease() {}

      int32_t inc_ref() { tbutil::Mutex::Lock lock(mutex_); return ++ref_count_;}
      int32_t dec_ref() { tbutil::Mutex::Lock lock(mutex_); return --ref_count_;}

      // we need to stat every request's real process time
      inline int64_t get_req_cost_time_us(const time_t now_us = common::Func::get_monotonic_time_us()) {return now_us - req_begin_time_; }

      // client call write, but never close, lease need to be expired
      inline void update_last_time_us(const int64_t now_us) { last_update_time_ = now_us;}
      inline bool timeout(const int64_t now_us)
      {
        return now_us > last_update_time_ +
          common::SYSPARAM_DATASERVER.expire_data_file_time_ * 1000000;
      }

      // unlink need return file_size to client for nginx log statistics
      inline int64_t get_file_size() { return file_size_; }
      inline void set_file_size(const int64_t file_size) { file_size_ = file_size; }

      bool check_all_finish();
      int check_all_successful() const;
      bool check_has_version_conflict() const;
      int get_member_info(std::pair<uint64_t, common::BlockInfoV2>* members, int32_t& size) const;
      int update_member_info(const uint64_t server, const common::BlockInfoV2& info, const int32_t status);
      int update_member_info();   // when received a error packet, use this interface
      void reset_member_info(const time_t now_us);
      bool get_highest_version_block(common::BlockInfoV2& info);
      void dump(const int32_t level, const char* const format = NULL);
      void dump(std::stringstream& desp);

      LeaseId lease_id_;
      int64_t last_update_time_;  // micro seconds
      int64_t req_begin_time_;    // micro seconds
      int64_t file_size_;
      int32_t ref_count_;
      int8_t server_size_;
      int8_t done_server_size_;
      tbutil::Mutex mutex_;
      LeaseMemberInfo members_[common::MAX_REPLICATION_NUM];
    private:
      DISALLOW_COPY_AND_ASSIGN(Lease);
    };

    class WriteLease: public Lease
    {
    public:
      WriteLease(const LeaseId& lease_id, const int64_t now_us, const common::VUINT64& servers);
      virtual ~WriteLease();
      DataFile& get_data_file() { return data_file_;}
    private:
      DataFile data_file_;
      DISALLOW_COPY_AND_ASSIGN(WriteLease);
    };

    class LeaseManager
    {
      #ifdef TFS_GTEST
      friend class TestLeaseManager;
      FRIEND_TEST(TestLeaseManager, process_apply_response);
      RIEND_TEST(TestLeaseManager, process_renew_response);
      #endif

      typedef std::map<LeaseId, Lease*> LEASE_MAP;
      typedef LEASE_MAP::iterator LEASE_MAP_ITER;
      typedef LEASE_MAP::const_iterator LEASE_MAP_CONST_ITER;
    public:
      LeaseManager();
      virtual ~LeaseManager();

      void generation(LeaseId& lease_id, const int64_t now_us, const int8_t type, const common::VUINT64& servers);
      Lease* get(const LeaseId& lease_id, const int64_t now_us) const;
      void put(Lease* lease);
      int remove(const LeaseId& lease_id);
      int timeout(const int64_t now_us);
      int64_t size() const;
      bool has_out_of_limit() const;
      uint64_t gen_lease_id(); // in old version, it's create_file_number
    private:
      uint64_t base_lease_id_;
      LEASE_MAP leases_;
      mutable common::RWLock rwmutex_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/

#endif
