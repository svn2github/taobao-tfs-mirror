/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#ifndef TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_HANDLE_TASK_HELPER_H_
#define TFS_LIFECYCLE_ROOTMANAGER_EXPROOTSERVER_HANDLE_TASK_HELPER_H_
#include <set>
#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kvengine_helper.h"
#include "common/expire_define.h"
#include "message/message_factory.h"
#include "exp_server_manager.h"

namespace tfs
{
  namespace exprootserver
  {
    class HandleTaskHelper
    {
      public:
        HandleTaskHelper(ExpServerManager &manager);
        virtual ~HandleTaskHelper();
        int init(tfs::common::KvEngineHelper* kv_engine_helper);
        int destroy();

        int handle_finish_task(const uint64_t es_id, const common::ExpireTaskInfo&);
        int handle_fail_servers(const common::VUINT64 &down_servers);
        int query_task(const uint64_t es_id, std::vector<common::ServerExpireTask>* p_running_tasks);
        int assign(const uint64_t es_id, const common::ExpireTaskInfo &del_task);
        void assign_task(void);

      protected:
        common::KvEngineHelper* kv_engine_helper_;
        int32_t lifecycle_area_;

      private:
        class AssignTaskThreadHelper : public tbutil::Thread
      {
        public:
          explicit AssignTaskThreadHelper(HandleTaskHelper &handle_task_helper)
            :handle_task_helper_(handle_task_helper){start();}
          virtual ~AssignTaskThreadHelper(){}
          void run();
        private:
          HandleTaskHelper &handle_task_helper_;
          DISALLOW_COPY_AND_ASSIGN(AssignTaskThreadHelper);
      };
        typedef tbutil::Handle<AssignTaskThreadHelper> AssignTaskThreadHelperPtr;

      private:
        AssignTaskThreadHelperPtr assign_task_thread_;

        tbutil::Mutex mutex_running_;

        tbutil::Mutex mutex_waitint_;

        std::map<uint64_t, std::set<common::ExpireTaskInfo> > m_s_running_tasks_;
        std::deque<common::ExpireTaskInfo> dq_waiting_tasks_;

        int32_t task_period_;
        int32_t note_interval_;

      private:
        bool destroy_;
        ExpServerManager &manager_;
        DISALLOW_COPY_AND_ASSIGN(HandleTaskHelper);
    };
  }
}

#endif
