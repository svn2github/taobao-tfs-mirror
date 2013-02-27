/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#ifndef TFS_KVROOTSERVER_META_SERVER_MANAGER_H_
#define TFS_KVROOTSERVER_META_SERVER_MANAGER_H_

#include <Time.h>
#include <Mutex.h>
#include <Monitor.h>
#include <TbThread.h>

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

#include "common/lock.h"
#include "common/buffer.h"
#include "common/kv_rts_define.h"

namespace tfs
{
  namespace kvrootserver
  {
    class KvMetaServerManager
    {
      public:
        KvMetaServerManager();
        virtual ~KvMetaServerManager();
        int initialize();
        void destroy();
        int keepalive(common::KvMetaServerBaseInformation& base_info);
        int check_ms_lease_expired(void);
        int get_table(common::KvMetaTable& meta_table);


      private:
        uint64_t new_lease_id(void);
        void check_ms_lease_expired_helper(const tbutil::Time& now);
        void move_table(void);

      private:
        class CheckKvMetaServerLeaseThreadHelper: public tbutil::Thread
      {
        public:
          explicit CheckKvMetaServerLeaseThreadHelper(KvMetaServerManager& manager) : manager_(manager){start();}
          virtual ~CheckKvMetaServerLeaseThreadHelper(){}
          void run();
        private:
          KvMetaServerManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(CheckKvMetaServerLeaseThreadHelper);
      };
        typedef tbutil::Handle<CheckKvMetaServerLeaseThreadHelper> CheckKvMetaServerLeaseThreadHelperPtr;
      private:
        common::KV_META_SERVER_MAPS servers_;
        common::KvMetaTable meta_table_;
        volatile uint64_t lease_id_factory_;

        CheckKvMetaServerLeaseThreadHelperPtr check_ms_lease_thread_;

        bool initialize_;
        bool destroy_;
        bool need_move_;
        int32_t wait_time_check_;
        int64_t root_start_time_;
        tbutil::Mutex mutex_;
        tbutil::Mutex mutex_for_get_;
      private:
        DISALLOW_COPY_AND_ASSIGN(KvMetaServerManager);
    };
  } /** kvrootserver **/
}/** tfs **/
#endif

