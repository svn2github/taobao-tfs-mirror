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
 *   xueya.yy <xueya.yy@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_EXP_SERVER_MANAGER_H_
#define TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_EXP_SERVER_MANAGER_H_

#include <Time.h>
#include <Mutex.h>
#include <Monitor.h>
#include <TbThread.h>

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

#include "common/lock.h"
#include "common/buffer.h"
#include "common/expire_define.h"

namespace tfs
{
  namespace exprootserver
  {
    class HandleTaskHelper;
    class ExpServerManager
    {
      public:
        ExpServerManager(HandleTaskHelper &handle_task_helper);
        virtual ~ExpServerManager();
        int initialize();
        void destroy();
        int keepalive(common::ExpServerBaseInformation& base_info);
        int check_ms_lease_expired(void);
        void get_available_expire_server(common::VUINT64& v_available_servers);
        int64_t get_start_time(){return root_start_time_;}

      private:
        uint64_t new_lease_id(void);
        void check_es_lease(const tbutil::Time& now);

      private:
        class CheckExpServerLeaseThreadHelper: public tbutil::Thread
      {
        public:
          explicit CheckExpServerLeaseThreadHelper(ExpServerManager& manager)
            :server_manager_(manager){start();}
          virtual ~CheckExpServerLeaseThreadHelper(){}
          void run();
        private:
          ExpServerManager& server_manager_;
          DISALLOW_COPY_AND_ASSIGN(CheckExpServerLeaseThreadHelper);
      };
        typedef tbutil::Handle<CheckExpServerLeaseThreadHelper> CheckExpServerLeaseThreadHelperPtr;

      private:
        common::EXP_SERVER_MAPS servers_;
        volatile uint64_t lease_id_factory_;

        CheckExpServerLeaseThreadHelperPtr check_es_lease_thread_;

        bool initialize_;
        bool destroy_;
        int wait_time_check_;
        int64_t root_start_time_;
        tbutil::Mutex mutex_; //lock for servers_
        HandleTaskHelper &handle_task_helper_;
      private:
        DISALLOW_COPY_AND_ASSIGN(ExpServerManager);
    };
  } /** exprootserver **/
}/** tfs **/
#endif

