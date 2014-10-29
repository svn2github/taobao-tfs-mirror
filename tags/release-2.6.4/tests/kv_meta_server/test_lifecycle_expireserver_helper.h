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
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#ifndef TFS_TESTS_TESTEXPIRESERVER_HELPER_H_
#define TFS_TESTS_TESTEXPIRESERVER_HELPER_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/kv_meta_define.h"
#include "common/kvengine_helper.h"
#include "message/message_factory.h"
#include "life_cycle_manager/expire_server/clean_task_helper.h"
#include "kv_meta_server/life_cycle_helper.h"

namespace tfs
{
  namespace expireserver
  {
    using namespace common;
    using namespace tfs::kvmetaserver;

    class TestCleanTaskHelper :public CleanTaskHelper
    {
    public:
      TestCleanTaskHelper()
      {
      }

      int init(common::KvEngineHelper*){
        return TFS_SUCCESS;
      }
      void set_kv_engine(KvEngineHelper *r)
      {
        kv_engine_helper_ = r;
      }
      int get_note(const uint64_t locak_ip, const int32_t num_es, const int32_t task_time,
                   const int32_t hash_bucket_num, const int64_t sum_file_num);
    };

    class TestLifeCycleHelper :public LifeCycleHelper
    {
    public:
      TestLifeCycleHelper()
      {
      }
      int init(common::KvEngineHelper*){
        return TFS_SUCCESS;
      }
      void set_kv_engine(KvEngineHelper *r)
      {
        kv_engine_helper_ = r;
      }

    };
  }
}

#endif
