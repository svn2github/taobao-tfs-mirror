/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */


#include "query_task_helper.h"
#include <malloc.h>
#include "common/func.h"
#include "common/tairengine_helper.h"

using namespace std;
namespace tfs
{
  using namespace common;
  namespace exprootserver
  {
    const int32_t KEY_BUFF_SIZE = 512 + 8 + 8;
    const int32_t VALUE_BUFF_SIZE = 512;
    const int32_t SCAN_LIMIT = 1000;

    enum
    {
      CMD_RANGE_ALL = 1,
      CMD_RANGE_VALUE_ONLY,
      CMD_RANGE_KEY_ONLY,
    };

    QueryTaskHelper::QueryTaskHelper()
    {
      kv_engine_helper_ = NULL;
      tair_lifecycle_area_ = 0;
    }

    QueryTaskHelper::~QueryTaskHelper()
    {
      kv_engine_helper_ = NULL;
    }

    int QueryTaskHelper::init(common::KvEngineHelper* kv_engine_helper)
    {
      int ret = TFS_SUCCESS;

      if (NULL == kv_engine_helper)
      {
        ret = TFS_ERROR;
      }
      else
      {
        kv_engine_helper_ = kv_engine_helper;
      }

      tair_lifecycle_area_ = SYSPARAM_EXPIRESERVER.tair_lifecycle_area_;
      return ret;
    }


    int QueryTaskHelper::query_progress(const uint64_t es_id, const int32_t num_es, const int32_t task_time,
                                   const int32_t hash_bucket_num, const ExpireTaskType type,
                                   int64_t *sum_file_num, int32_t *current_percent)
    {
      UNUSED(es_id);
      UNUSED(num_es);
      UNUSED(task_time);
      UNUSED(hash_bucket_num);
      UNUSED(type);
      UNUSED(sum_file_num);
      UNUSED(current_percent);
      //op key
      int ret = TFS_SUCCESS;
      /*char *key_buff = (char*)malloc(KEY_BUFF_SIZE);
      char *value_buff = (char*)malloc(VALUE_BUFF_SIZE);

      if(NULL == key_buff || NULL == value_buff)
      {
        ret = TFS_ERROR;
      }
      KvKey key;

      if (TFS_SUCCESS == ret)
      {
        ret = ExpireDefine::serialize_es_stat_key(local_ipport_, num_es,
                                task_time, hash_bucket_num,
                                sum_file_num, &key,
                                key_buff, KEY_BUFF_SIZE);
      }

      //op value
      int64_t pos = 0;
      int64_t lock_version = 0;
      KvMemValue kv_value;
      if (TFS_SUCCESS == ret)
      {
        ret = kv_engine_helper_->get_key(tair_lifecycle_area_, key, &kv_value, lock_version);

        if (TFS_SUCCESS == ret)
        {
          ;
        }
      }

      if (NULL != value_buff)
      {
        free(value_buff);
        value_buff = NULL;
      }

      if (NULL != key_buff)
      {
        free(key_buff);
        key_buff = NULL;
      }*/

      return ret;
    }

  }// end for exprootserver
}// end for tfs

