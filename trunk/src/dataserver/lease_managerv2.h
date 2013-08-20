/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_LEASE_MANAGERV2_H_
#define TFS_DATASERVER_LEASE_MANAGERV2_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "message/ds_lease_message.h"
#include "data_file.h"
#include "ds_define.h"

namespace tfs
{
  namespace dataserver
  {
    enum LeaseStatus
    {
      LEASE_APPLY,
      LEASE_RENEW
    };

    class DataService;
    class WritableBlock;
    class WritableBlockManager;
    class LeaseManager
    {
      #ifdef TFS_GTEST
      friend class TestLeaseManager;
      FRIEND_TEST(TestLeaseManager, process_apply_response);
      FRIEND_TEST(TestLeaseManager, process_renew_response);
      #endif


      public:
        LeaseManager(DataService& service, const std::vector<uint64_t>& ns_ip_port);
        ~LeaseManager();

        int initialize();
        void destroy();

        WritableBlockManager& get_writable_block_manager();

        // interface for upper layer
        bool has_valid_lease(const time_t now) const;
        int alloc_writable_block(WritableBlock*& block);
        int alloc_update_block(const uint64_t block_id, WritableBlock*& block);
        void free_writable_block(const uint64_t block_id);
        void expire_block(const uint64_t block_id);

        // cleanup expired leases
        int timeout(const time_t now);

        // lease thread
        void run_lease(const int32_t who);

        // apply & giveup block thread
        void run_apply_and_giveup();

      private:
        /*
         * ds apply lease after startup
         * if apply fail, ds will keep retry
         */
        int apply(const int32_t who);

        /*
         * ds renew lease every few seconds
         * if renew fail, ds will retry lease_retry_times
         */
        int renew(const int32_t who);

        /*
         * ds giveup lease when exit
         */
        int giveup(const int32_t who);

        bool need_renew(const time_t now, const int32_t who) const
        {
          return now >= last_renew_time_[who] + lease_meta_[who].lease_renew_time_;
        }

        bool is_expired(const time_t now, const int32_t who) const
        {
          return now >= last_renew_time_[who] + lease_meta_[who].lease_expire_time_;
        }

        bool is_master(const int32_t who) const
        {
          return lease_meta_[who].ns_role_ != 0; // TODO
        }

      private:
        void process_apply_response(message::DsApplyLeaseResponseMessage* response, const int32_t who);
        void process_renew_response(message::DsRenewLeaseResponseMessage* response, const int32_t who);

      private:
      class RunLeaseThreadHelper: public tbutil::Thread
      {
        public:
          explicit RunLeaseThreadHelper(LeaseManager& manager, const int32_t who):
            manager_(manager), who_(who)
          {
            start();
          }
          virtual ~RunLeaseThreadHelper(){}
          void run();
        private:
          LeaseManager& manager_;
          int32_t who_;
        private:
          DISALLOW_COPY_AND_ASSIGN(RunLeaseThreadHelper);
      };
      typedef tbutil::Handle<RunLeaseThreadHelper> RunLeaseThreadHelperPtr;

      class RunApplyBlockThreadHelper: public tbutil::Thread
      {
        public:
          explicit RunApplyBlockThreadHelper(LeaseManager& manager):
            manager_(manager)
          {
            start();
          }
          virtual ~RunApplyBlockThreadHelper(){}
          void run();
        private:
          LeaseManager& manager_;
        private:
          DISALLOW_COPY_AND_ASSIGN(RunApplyBlockThreadHelper);
      };
      typedef tbutil::Handle<RunApplyBlockThreadHelper> RunApplyBlockThreadHelperPtr;

      private:
        DataService& service_;
        uint64_t ns_ip_port_[common::MAX_SINGLE_CLUSTER_NS_NUM];
        common::LeaseMeta lease_meta_[common::MAX_SINGLE_CLUSTER_NS_NUM];
        LeaseStatus lease_status_[common::MAX_SINGLE_CLUSTER_NS_NUM];
        time_t last_renew_time_[common::MAX_SINGLE_CLUSTER_NS_NUM];
        RunLeaseThreadHelperPtr lease_thread_[common::MAX_SINGLE_CLUSTER_NS_NUM];
        RunApplyBlockThreadHelperPtr apply_block_thread_;

      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseManager);

    };

  }/** end namespace dataserver**/
}/** end namespace tfs **/

#endif
