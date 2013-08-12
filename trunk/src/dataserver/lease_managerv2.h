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
    class WritableBlockManager;
    class LeaseManager
    {
      public:
        LeaseManager(DataService& service);
        ~LeaseManager();

        WritableBlockManager& get_writable_block_manager();

        /*
         * ds apply lease after startup
         * if apply fail, ds will keep retry
         */
        int apply();

        /*
         * ds renew lease every few seconds
         * if renew fail, ds will retry lease_retry_times
         */
        int renew();

        /*
         * ds giveup lease when exit
         */
        int giveup();

        // cleanup expired leases
        int timeout(const time_t now);

        // renew thread work
        void run_lease();

        bool need_renew(const time_t now) const
        {
          return now >= last_renew_time_ + lease_meta_.lease_renew_time_;
        }

        bool is_expired(const time_t now) const
        {
          return now >= last_renew_time_ + lease_meta_.lease_expire_time_;
        }

        const common::LeaseMeta& get_lease_meta() const
        {
          return lease_meta_;
        }

      private:
        DataService& service_;
        common::LeaseMeta lease_meta_;
        LeaseStatus lease_status_;
        time_t last_renew_time_;

      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseManager);

    };

  }/** end namespace dataserver**/
}/** end namespace tfs **/

#endif
