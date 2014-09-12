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

#ifndef TFS_COMMON_MYSQL_CLUSTER_MYSQL_ENGINE_HELPER_H_
#define TFS_COMMON_MYSQL_CLUSTER_MYSQL_ENGINE_HELPER_H_

#include "common/kv_meta_define.h"
#include "common/kvengine_helper.h"
#include "mysql_database_helper.h"
#include "database_pool.h"

namespace tfs
{
  namespace common
  {
    class MysqlEngineHelper : public KvEngineHelper
    {
    public:
      MysqlEngineHelper(const std::string &conn_str, const std::string &user, const std::string &pass);
      virtual ~MysqlEngineHelper();
      virtual int init();
      virtual int scan_keys(const int32_t name_area, const KvKey& start_key, const KvKey& end_key, const int32_t limit, const int32_t offset,
                           std::vector<KvValue*> *keys, std::vector<KvValue*> *values, int32_t* result_size, short scan_type);
      virtual int get_key(const int32_t name_area, const KvKey& key, KvValue **pp_value, int64_t *version);
      virtual int put_key(const int32_t name_area, const KvKey& key, const KvMemValue &value, const int64_t version);

      virtual int delete_key(const int32_t name_area, const KvKey& key);
      virtual int delete_keys(const int32_t name_area, const std::vector<KvKey>& vec_keys);

    public:

    private:
      DISALLOW_COPY_AND_ASSIGN(MysqlEngineHelper);
      DataBasePool* data_base_pool_;
      std::string conn_str_;
      std::string user_;
      std::string pass_;
      int pool_size_;
      static const int MYSQL_RETRY_COUNT;
    };
  }
}
#endif
