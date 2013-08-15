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

        int query_progress(const uint64_t es_id, const int32_t num_es, const int32_t task_time,
            const int32_t hash_bucket_id, const common::ExpireTaskType type,
            int64_t *sum_file_num, int32_t *current_percent);

        int handle_finish_task(const uint64_t es_id);
        int handle_fail_servers(const common::VUINT64 &down_servers);
        int assign(const uint64_t es_id, const common::ExpireDeleteTask &del_task);
        int assign_task(void);

        void get_task_info(std::map<uint64_t, common::ExpireDeleteTask> &m_task_info);
        void put_task_info(const std::map<uint64_t, common::ExpireDeleteTask> &m_task_info);
        void get_task_wait(std::deque<common::ExpireDeleteTask> &task_wait);

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

        tbutil::Mutex mutex_task_; //lock for m_task_info_

        tbutil::Mutex mutex_task_wait_;   //lock for task_wait_

        std::map<uint64_t, common::ExpireDeleteTask> m_task_info_;
        typedef std::map<uint64_t, common::ExpireDeleteTask>::iterator TASK_INFO_ITER;

        std::deque<common::ExpireDeleteTask> task_wait_;

      private:
        ExpServerManager &manager_;
        DISALLOW_COPY_AND_ASSIGN(HandleTaskHelper);
    };
  }
}

#endif
