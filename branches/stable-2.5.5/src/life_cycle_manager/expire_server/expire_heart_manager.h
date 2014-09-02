/* * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs<qixiao.zs@alibaba-inc.com>
 *      - initial release
 */
#ifndef TFS_LIFE_CYCLE_MANAGER_EXPIRE_SERVER_HEART_MANAGER_H_
#define TFS_LIFE_CYCLE_MANAGER_EXPIRE_SERVER_HEART_MANAGER_H_

#include <tbsys.h>
#include <Monitor.h>
#include <Mutex.h>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/kv_rts_define.h"
#include "common/error_msg.h"
#include "clean_task_helper.h"

namespace tfs
{
  namespace expireserver
  {
    class CleanTaskHelper;
    class ExpireHeartManager
    {
      public:
        ExpireHeartManager(CleanTaskHelper &cleantask);
        virtual ~ExpireHeartManager();

        int initialize(const uint64_t rs_ipport_id,
            const uint64_t ms_ipport_id, const int64_t start_time,
            const int64_t available_thread_count);
        void destroy();

      private:
        void run_heart(void);
        int keepalive(void);
      private:
      class ESHeartBeatThreadHelper: public tbutil::Thread
      {
        public:
          explicit ESHeartBeatThreadHelper(ExpireHeartManager& manager): manager_(manager){}
          virtual ~ESHeartBeatThreadHelper(){};
          void run();
        private:
          ExpireHeartManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(ESHeartBeatThreadHelper);
      };
        typedef tbutil::Handle<ESHeartBeatThreadHelper> ESHeartBeatThreadHelperPtr;
      private:
        DISALLOW_COPY_AND_ASSIGN(ExpireHeartManager);
        static const int8_t MAX_RETRY_COUNT;
        static const int16_t MAX_TIMEOUT_MS;
        ESHeartBeatThreadHelperPtr heart_thread_;

        bool destroy_;
        uint64_t ers_ipport_id_;
        uint64_t es_ipport_id_;
        int64_t start_time_;
        int64_t heart_inter_;
        int64_t available_thread_count_;
        CleanTaskHelper &ref_clean_helper_;

    };
  }/** namemetaserver **/
}/** tfs **/

#endif

