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

#ifndef TFS_LIFE_CYCLE_MANAGER_EXPIRE_CLEAN_TASK_HELPER_H_
#define TFS_LIFE_CYCLE_MANAGER_EXPIRE_CLEAN_TASK_HELPER_H_
#include <tbsys.h>
#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kvengine_helper.h"
#include "common/expire_define.h"
#include "message/message_factory.h"
#include "tfs_client_api.h"


namespace tfs
{
  namespace expireserver
  {
    class CleanTaskHelper
    {
      public:
        CleanTaskHelper();
        virtual ~CleanTaskHelper();
        int init(tfs::common::KvEngineHelper* kv_engine_helper);

        int do_clean_task(const uint64_t local_ipport, const int32_t total_es, const int32_t num_es,
                       const int32_t note_interval, const int32_t task_time);

        int32_t get_running_threads_count();

      private:
        int stat_to_kvstore(const uint64_t local_ipport, const int32_t num_es,
            const int32_t task_time,
            const int32_t hash_bucket_num, const int64_t sum_file_num);

      protected:
        common::KvEngineHelper* kv_engine_helper_;
        int32_t tair_lifecycle_area_;

      private:
        volatile uint64_t running_task_count_;
        client::RestClient restful_client_;
        DISALLOW_COPY_AND_ASSIGN(CleanTaskHelper);
    };
  }
}

#endif
