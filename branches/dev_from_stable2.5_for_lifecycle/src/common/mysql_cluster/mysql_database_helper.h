/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#ifndef TFS_COMMON_MYSQL_CLUSTER_MYSQL_DATABASE_HELPER_H_
#define TFS_COMMON_MYSQL_CLUSTER_MYSQL_DATABASE_HELPER_H_
#include <mysql.h>
#include <tbsys.h>
#include <Mutex.h>
#include <vector>

#include "common/kvengine_helper.h"

namespace tfs
{
  namespace common
  {
    class MysqlDatabaseHelper
    {
      public:
        MysqlDatabaseHelper();
        virtual ~MysqlDatabaseHelper();
        int insert_kv(const int32_t area, const KvKey& key, const KvMemValue &value);
        int replace_kv(const int32_t area, const KvKey& key, const KvMemValue &value);
        int update_kv(const int area, const KvKey& key, const KvMemValue &value,
            const int32_t version);
        int rm_kv(const int32_t area, const KvKey& key);

        int get_v(const int area, const KvKey& key, KvValue **pp_value, int64_t *version);
        // minus limit means pre scan
        int scan_v(const int area,
            const KvKey& start_key, const KvKey& end_key,
            const int32_t limit, const bool skip_first,
            std::vector<KvValue*> *keys, std::vector<KvValue*> *values, int32_t* result_size);
      public:
        bool is_connected() const;
        int set_conn_param(const char* conn_str, const char* user_name, const char* passwd);
        virtual int connect();
        virtual int close();


      private:
        MYSQL_STMT *stmt_;
        MYSQL_BIND ps_params_[5];  /* input parameter buffers */
      private:
        struct mysql_ex {
          std::string host;
          int port;
          std::string user;
          std::string pass;
          std::string database;
          bool   isopen;
          bool   inited;
          MYSQL  mysql;
        };

        bool init_mysql(const char* mysqlconn, const char* user_name, const char* passwd);
        bool open_mysql();
        int close_mysql();
        bool excute_stmt(MYSQL_STMT *stmt, int64_t& mysql_proc_ret);

        enum
        {
          MAX_KEY_VAR_SIZE = 749,
          MAX_VALUE_VAR_SIZE = 1024*1024,
        };

      private:
        mysql_ex  mysql_;
        tbutil::Mutex mutex_;
        char* key_buff_;
        char* value_buff_;
        char* sql_str_;
        int32_t max_sql_size_;
        int32_t key_buff_size_;
        int32_t value_buff_size_;
        int32_t retry_count_;

      private:
        enum
        {
          CONN_STR_LEN = 256,
        };
        char conn_str_[CONN_STR_LEN];
        char user_name_[CONN_STR_LEN];
        char passwd_[CONN_STR_LEN];
        bool is_connected_;

    };

  }
}
#endif
