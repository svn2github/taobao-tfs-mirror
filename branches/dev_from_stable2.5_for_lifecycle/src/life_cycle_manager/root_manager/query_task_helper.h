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

#ifndef TFS_ROOT_CLEAN_TASK_HELPER_H_
#define TFS_ROOT_CLEAN_TASK_HELPER_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kvengine_helper.h"
#include "common/expire_define.h"
#include "message/message_factory.h"


namespace tfs
{
  namespace exprootserver
  {
    class QueryTaskHelper
    {
      public:
        QueryTaskHelper();
        virtual ~QueryTaskHelper();
        int init(tfs::common::KvEngineHelper* kv_engine_helper);

        int query_progress(const uint64_t es_id, const int32_t num_es, const int32_t task_time,
            const int32_t hash_bucket_id, const common::ExpireTaskType type,
            int64_t *sum_file_num, int32_t *current_percent);

      protected:
        common::KvEngineHelper* kv_engine_helper_;
        int32_t tair_lifecycle_area_;
      private:
        DISALLOW_COPY_AND_ASSIGN(QueryTaskHelper);
    };
  }
}

#endif
