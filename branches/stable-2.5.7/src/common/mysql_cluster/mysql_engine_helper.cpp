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
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */

#include "mysql_engine_helper.h"

#include "Memory.hpp"
#include "common/parameter.h"
#include "common/error_msg.h"
#include "common/serialization.h"
#include "mysql_database_helper.h"
using namespace std;
namespace tfs
{
  namespace common
  {

    const int MysqlEngineHelper::MYSQL_RETRY_COUNT = 1;

    MysqlEngineHelper::MysqlEngineHelper(const std::string &conn_str, const std::string &user, const std::string &pass)
      :data_base_pool_(NULL), conn_str_(conn_str),
      user_(user), pass_(pass), pool_size_(20)
    {

    }

    MysqlEngineHelper::~MysqlEngineHelper()
    {
      delete data_base_pool_;
    }

    int MysqlEngineHelper::init()
    {
      int ret = TFS_SUCCESS;
      if (NULL != data_base_pool_)
      {
        TBSYS_LOG(ERROR, "MysqlEngineHelper have been inited");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        data_base_pool_ = new DataBasePool();
        assert(NULL !=data_base_pool_);

        if (!data_base_pool_->init_pool(pool_size_, conn_str_.c_str(), user_.c_str(), pass_.c_str()))
        {
          TBSYS_LOG(ERROR, "MysqlEngineHelper fail. ");
          ret = TFS_ERROR;
        }
      }
      return ret;
    }

    int MysqlEngineHelper::put_key(const int32_t name_area, const KvKey& key, const KvMemValue &value, const int64_t version)
    {
      int ret = TFS_ERROR;
      MysqlDatabaseHelper* database_ = data_base_pool_->get();
      if (NULL != database_)
      {
        ret = TFS_SUCCESS;
        if (KvDefine::MAX_VERSION == version)
        {
          ret = database_->insert_kv(name_area, key, value);
        }
        else if (0 == version)
        {
          ret = database_->replace_kv(name_area, key, value);
        }
        else
        {
          ret = database_->update_kv(name_area, key, value, version);
        }
        data_base_pool_->release(database_);
      }
      return ret;
    }
    int MysqlEngineHelper::get_key(const int32_t name_area, const KvKey& key, KvValue **pp_value, int64_t *version)
    {
      int ret = TFS_ERROR;
      int64_t my_version = 0;
      MysqlDatabaseHelper* database_ = data_base_pool_->get();
      if (NULL != database_)
      {
        ret = database_->get_v(name_area, key, pp_value, &my_version);
        if (NULL != version) *version = my_version;
        data_base_pool_->release(database_);
      }
      return ret;
    }
    int MysqlEngineHelper::scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key, const int32_t limit,
        const int32_t offset, vector<KvValue*> *keys, vector<KvValue*> *values, int32_t* result_size, short scan_type)
    {
      int ret = TFS_ERROR;
      assert(offset <2);
      UNUSED(scan_type);

      MysqlDatabaseHelper* database_ = data_base_pool_->get();
      if (NULL != database_)
      {
        ret = database_->scan_v(name_area, start_key, end_key, limit, offset!=0, keys, values, result_size);
        data_base_pool_->release(database_);
      }
      return ret;
    }
    int MysqlEngineHelper::delete_key(const int32_t name_area, const KvKey& key)
    {
      int ret = TFS_ERROR;
      MysqlDatabaseHelper* database_ = data_base_pool_->get();
      if (NULL != database_)
      {
        ret = database_->rm_kv(name_area, key);
        data_base_pool_->release(database_);
      }
      return ret;
    }


    int MysqlEngineHelper::delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys)
    {
      int ret = TFS_SUCCESS;
      for(size_t i = 0; i < vec_keys.size() && TFS_SUCCESS == ret; i++)
      {
        ret = delete_key(name_area, vec_keys[i]);
      }
      return ret;
    }

  }
}
