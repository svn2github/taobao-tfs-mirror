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
#include "mysql_database_helper.h"

#include <errmsg.h>
#include <vector>
#include <errmsg.h>
#include <mysqld_error.h>

#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"

#include "common/expire_define.h"

using namespace std;
namespace tfs
{
  namespace common
  {

    MysqlDatabaseHelper::MysqlDatabaseHelper()
    {
      stmt_ = NULL;
      key_buff_size_ = MAX_KEY_VAR_SIZE + 10;
      key_buff_ = (char*)malloc(key_buff_size_);

      value_buff_size_ = MAX_VALUE_VAR_SIZE + 10;
      value_buff_ = (char*)malloc(value_buff_size_);
      max_sql_size_ = (value_buff_size_ * 2 + 1) * 2 + (key_buff_size_ * 2 + 1) + 1024;
      sql_str_ = (char*)malloc(max_sql_size_);
      conn_str_[0] = '\0';
      user_name_[0] = '\0';
      passwd_[0] = '\0';
      retry_count_ = 3;
      is_connected_ = false;
    }
    MysqlDatabaseHelper::~MysqlDatabaseHelper()
    {
      free(sql_str_);
      free(value_buff_);
      free(key_buff_);
      close();
      mysql_server_end();
    }
    int MysqlDatabaseHelper::set_conn_param(const char* conn_str, const char* user_name, const char* passwd)
    {
      int ret = TFS_SUCCESS;
      if (NULL == conn_str || NULL == user_name || NULL == passwd)
      {
        ret = TFS_ERROR;
      }
      else
      {
        snprintf(conn_str_, CONN_STR_LEN, "%s", conn_str);
        snprintf(user_name_, CONN_STR_LEN, "%s", user_name);
        snprintf(passwd_, CONN_STR_LEN, "%s", passwd);
      }
      return ret;
    }
    int MysqlDatabaseHelper::connect()
    {
      int ret = TFS_SUCCESS;
      if (is_connected_)
      {
        close();
      }

      if (!init_mysql(conn_str_, user_name_, passwd_))
      {
        ret = TFS_ERROR;
      }
      else if (!open_mysql())
      {
        ret = TFS_ERROR;
      }
      is_connected_ = TFS_SUCCESS == ret;
      return ret;
    }

    int MysqlDatabaseHelper::close()
    {
      close_mysql();
      is_connected_ = false;
      return TFS_SUCCESS;
    }

    int MysqlDatabaseHelper::insert_kv(const int32_t area, const KvKey& key, const KvMemValue &value)
    {
      int ret = TFS_SUCCESS;
      int64_t mysql_proc_ret = 0;
      int retry_time = 0;
      if (key.key_size_ > MAX_KEY_VAR_SIZE || value.get_size() > MAX_VALUE_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "size error");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_, "insert into tfsmeta_%d "
            "(meta_key, meta_value, version) values "
            "('", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                key.key_, key.key_size_);
            pos += sprintf(sql_str_ + pos, "%s", "','");
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                value.get_data(), value.get_size());
            pos += sprintf(sql_str_ + pos, "%s", "', 1)");
          }

          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);

          TBSYS_LOG(DEBUG, "query str: %s", sql_str_);

          if (0 != ret)
          {
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
            if (ER_DUP_ENTRY == ret)
            {
              mysql_proc_ret = EXIT_KV_RETURN_VERSION_ERROR;
              ret = TFS_SUCCESS;
            }
            else
            {
              TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            }
          }
        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      if (TFS_SUCCESS == ret && EXIT_KV_RETURN_VERSION_ERROR == mysql_proc_ret)
      {
        TBSYS_LOG(DEBUG, "insert affect row 0");
        ret = EXIT_KV_RETURN_VERSION_ERROR;
      }
      return ret;
    }
    int MysqlDatabaseHelper::replace_kv(const int32_t area, const KvKey& key, const KvMemValue &value)
    {
      int ret = TFS_SUCCESS;
      int retry_time = 0;
      if (key.key_size_ > MAX_KEY_VAR_SIZE || value.get_size() > MAX_VALUE_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "size error");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_, "insert into tfsmeta_%d "
            "(meta_key, meta_value, version) "
            "values ('", area);


        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                key.key_, key.key_size_);
            pos += sprintf(sql_str_ + pos, "%s", "','");
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                value.get_data(), value.get_size());
            pos += sprintf(sql_str_ + pos, "%s", "', 1)  on duplicate key "
                "update meta_value='");
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                value.get_data(), value.get_size());
            pos += sprintf(sql_str_ + pos, "%s", "',  version=mod(version,1024)+1");
          }

          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
            //if (ER_DUP_ENTRY == ret)
            //{
            //  mysql_proc_ret = EXIT_KV_RETURN_VERSION_ERROR;
            //  ret = TFS_SUCCESS;
            //}
          }
        }
      }
      if (TFS_SUCCESS != ret)
      {
        close();
      }
      return ret;
    }
    int MysqlDatabaseHelper::update_kv(const int area, const KvKey& key, const KvMemValue
        &value, const int32_t version)
    {
      int ret = TFS_SUCCESS;
      int64_t mysql_proc_ret = 0;
      int retry_time = 0;
      if (key.key_size_ > MAX_KEY_VAR_SIZE || value.get_size() > MAX_VALUE_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "size error");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_, "update tfsmeta_%d set meta_value='", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                value.get_data(), value.get_size());

            pos += sprintf(sql_str_ + pos, "%s", "', version=mod(version,1024)+1 where meta_key='");
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                key.key_, key.key_size_);
            pos += sprintf(sql_str_ + pos, "' and version = %d", version);
          }
          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
          }
          else
          {
            mysql_proc_ret = mysql_affected_rows(&mysql_.mysql);
          }
        }
        if (TFS_SUCCESS != ret)
        {
          close();
        }
        if (TFS_SUCCESS == ret && 1 != mysql_proc_ret)
        {
          TBSYS_LOG(DEBUG, "insert affect row 0");
          ret = EXIT_KV_RETURN_VERSION_ERROR;
        }
      }
      return ret;
    }
    int MysqlDatabaseHelper::rm_kv(const int32_t area, const KvKey& key)
    {
      int ret = TFS_SUCCESS;
      if (key.key_size_ > MAX_KEY_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "input error");
        ret = TFS_ERROR;
      }
      int retry_time = 0;
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_, "delete from tfsmeta_%d where meta_key='", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                key.key_, key.key_size_);
            pos += sprintf(sql_str_ + pos, "%s", "'");
          }

          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
          }
        }
        if (TFS_SUCCESS != ret)
        {
          close();
        }
      }
      return ret;
    }

    int MysqlDatabaseHelper::get_v(const int area, const KvKey& key, KvValue **pp_value, int64_t *version)
    {
      int ret = TFS_SUCCESS;
      int get_count = 0;

      if (key.key_size_ > MAX_KEY_VAR_SIZE || NULL == pp_value || NULL == version)
      {
        TBSYS_LOG(ERROR, "input error %d %p %p", key.key_size_, pp_value, version);

        ret = TFS_ERROR;
      }
      int retry_time = 0;
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_, "select meta_value, "
            "version from tfsmeta_%d where meta_key='", area);
        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                key.key_, key.key_size_);
            pos += sprintf(sql_str_ + pos, "%s", "'");
          }
          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);

          TBSYS_LOG(DEBUG, "query str %s", sql_str_);

          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
            TBSYS_LOG(INFO, "RET: %d", ret);
          }
          else
          {
            MYSQL_ROW row;
            MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
            if (mysql_ret == NULL)
            {
              TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
              ret = TFS_ERROR;
            }
            else
            {
              while(NULL != (row = mysql_fetch_row(mysql_ret)))
              {
                unsigned long *lengths;
                lengths = mysql_fetch_lengths(mysql_ret);
                int value_size = lengths[0];
                if (value_size > value_buff_size_)
                {
                  TBSYS_LOG(ERROR, "value size error %d truncated it", value_size);
                  value_size = value_buff_size_;
                }
                *version = atoi(row[1]);

                KvMemValue* p_value = new KvMemValue();
                char* data_ = p_value->malloc_data(value_size);
                memcpy(data_, row[0], value_size);
                *pp_value = p_value;

                get_count++;
                break;
              }
              mysql_free_result(mysql_ret);
            }
          }
        }
        if (TFS_SUCCESS != ret)
        {
          close();
        }
        if (TFS_SUCCESS == ret)
        {
          if (get_count <= 0)
          {
            ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
          }
        }
      }
      return ret;
    }
    int MysqlDatabaseHelper::scan_v(const int area,
        const KvKey& start_key, const KvKey& end_key,
        const int32_t limit, const bool skip_first,
        vector<KvValue*> *keys, vector<KvValue*> *values, int32_t* result_size)
    {
      int ret = TFS_SUCCESS;
      int get_count = 0;

      //todo: for debug
      /*int32_t t_days_secs;
      int32_t t_hours_secs;
      int32_t t_hash_num;
      int32_t t_file_type;
      string t_file_name;
      ExpireDefine::deserialize_exptime_app_key(start_key.key_, start_key.key_size_,
          &t_days_secs, &t_hours_secs, &t_hash_num, &t_file_type, &t_file_name);

      TBSYS_LOG(INFO, "start, t_hash_num: %d, t_days_secs: %d, t_hours_secs: %d, file_type: %d, file_name: %s", t_hash_num, t_days_secs, t_hours_secs, t_file_type, t_file_name.c_str());

      ExpireDefine::deserialize_exptime_app_key(end_key.key_, end_key.key_size_,
          &t_days_secs, &t_hours_secs, &t_hash_num, &t_file_type, &t_file_name);

      TBSYS_LOG(INFO, "end, t_hash_num: %d, t_days_secs: %d, t_hours_secs: %d, file_type: %d, file_name: %s", t_hash_num, t_days_secs, t_hours_secs, t_file_type, t_file_name.c_str());*/

      if (start_key.key_size_ > MAX_KEY_VAR_SIZE ||
          end_key.key_size_ > MAX_KEY_VAR_SIZE ||
          NULL == keys || NULL == values || NULL == result_size)
      {
        TBSYS_LOG(ERROR, "input error");
        ret = TFS_ERROR;
      }
      int retry_time = 0;
      if (TFS_SUCCESS == ret)
      {
        int limit_=limit;
        int64_t pos = 0;
        pos = snprintf(sql_str_, max_sql_size_,
            "select meta_key, meta_value, version from tfsmeta_%d where ", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          if (retry_time == 0)
          {
            if (limit < 0)
            {
              limit_ = -limit;
              if (skip_first)
              {
                pos += sprintf(sql_str_ + pos, "%s", "meta_key<'");
              }
              else
              {
                pos += sprintf(sql_str_ + pos, "%s", "meta_key<='");
              }
              pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                  start_key.key_, start_key.key_size_);
              pos += sprintf(sql_str_ + pos, "' and meta_key >= '");
              pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                  end_key.key_, end_key.key_size_);
              pos += sprintf(sql_str_ + pos, "' order by meta_key desc limit %d ", limit_);
            }
            else
            {
              if (skip_first)
              {
                pos += sprintf(sql_str_ + pos, "%s", "meta_key>'");
              }
              else
              {
                pos += sprintf(sql_str_ + pos, "%s", "meta_key>='");
              }
              pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                  start_key.key_, start_key.key_size_);
              pos += sprintf(sql_str_ + pos, "' and meta_key <= '");
              pos += mysql_real_escape_string(&mysql_.mysql, sql_str_ + pos,
                  end_key.key_, end_key.key_size_);
              pos += sprintf(sql_str_ + pos, "' order by meta_key asc limit %d ", limit_);
            }
          }
          ret= mysql_real_query(&mysql_.mysql, sql_str_, pos);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "mysql_real_query error %d", ret);
            ret = mysql_errno(&mysql_.mysql);
            if (CR_SERVER_GONE_ERROR == ret && retry_time++ < retry_count_)
            {
              TBSYS_LOG(ERROR, "CR_SERVER_GONE_ERROR error");
              close();
              goto retry;
            }
          }
          else
          {
            //TBSYS_LOG(INFO, "mysql_real_query str %s", sql_str_);
            MYSQL_ROW row;
            MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
            if (mysql_ret == NULL)
            {
              TBSYS_LOG(ERROR, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
              ret = TFS_ERROR;
            }
            else
            {
              while(NULL != (row = mysql_fetch_row(mysql_ret)))
              {
                unsigned long *lengths;
                lengths = mysql_fetch_lengths(mysql_ret);
                int key_size = lengths[0];
                int value_size = lengths[1];
                if (key_size > key_buff_size_)
                {
                  TBSYS_LOG(ERROR, "key size error %d truncated it", key_size);
                  key_size = key_buff_size_;
                }
                if (value_size > value_buff_size_ )
                {
                  TBSYS_LOG(ERROR, "value size error %d truncated it", value_size);
                  value_size = value_buff_size_;
                }
                KvMemValue* p_key = new KvMemValue();
                char* data_ =(p_key)->malloc_data(key_size);
                memcpy(data_, row[0], key_size);
                keys->push_back(p_key);

                KvMemValue* p_value = new KvMemValue();
                data_ =(p_value)->malloc_data(value_size);
                memcpy(data_, row[1], value_size);
                values->push_back(p_value);

                get_count++;

                /*int32_t size = 0;
                  char key[p_key->get_size() * 2 + 1];
                  size = mysql_real_escape_string(&mysql_.mysql, key, p_key->get_data(), p_key->get_size());
                  TBSYS_LOG(INFO, "find key: %s", key);
                  ExpireDefine::deserialize_exptime_app_key(p_key->get_data(), p_key->get_size(),
                  &t_days_secs, &t_hours_secs, &t_hash_num, &t_file_type, &t_file_name);
                  TBSYS_LOG(INFO, "find, t_hash_num: %d, t_days_secs: %d, t_hours_secs: %d, file_type: %d, file_name: %s", t_hash_num, t_days_secs, t_hours_secs, t_file_type, t_file_name.c_str());*/
              }
              mysql_free_result(mysql_ret);
            }
          }
        }
        if (TFS_SUCCESS != ret)
        {
          close();
        }
        if (TFS_SUCCESS == ret)
        {
          if (get_count <= 0)
          {
            ret = EXIT_KV_RETURN_DATA_NOT_EXIST;
          }
        }
      }
      *result_size=get_count;
      return ret;
    }

    ////////////////////////////////////

    bool MysqlDatabaseHelper::init_mysql(const char* mysqlconn, const char* user_name, const char* passwd)
    {
      vector<string> fields;
      tfs::common::Func::split_string(mysqlconn, ':', fields);
      mysql_.isopen = false;
      if (fields.size() < 3 || NULL == user_name || NULL == passwd)
        return false;
      mysql_.host = fields[0];
      mysql_.port = atoi(fields[1].c_str());
      mysql_.user = user_name;
      mysql_.pass = passwd;
      mysql_.database = fields[2];
      mysql_.inited = true;

      int v = 5;
      mysql_init(&mysql_.mysql);
      mysql_options(&mysql_.mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&v);
      mysql_options(&mysql_.mysql, MYSQL_OPT_READ_TIMEOUT, (const char *)&v);
      mysql_options(&mysql_.mysql, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&v);
      return true;
    }

    bool MysqlDatabaseHelper::open_mysql()
    {
      if (!mysql_.inited) return false;
      if (mysql_.isopen) return true;
      MYSQL *conn = mysql_real_connect(
          &mysql_.mysql,
          mysql_.host.c_str(),
          mysql_.user.c_str(),
          mysql_.pass.c_str(),
          mysql_.database.c_str(),
          mysql_.port, NULL, CLIENT_MULTI_STATEMENTS);
      if (!conn)
      {
        TBSYS_LOG(ERROR, "connect mysql database (%s:%d:%s:%s:%s) error(%s)",
            mysql_.host.c_str(), mysql_.port, mysql_.user.c_str(), mysql_.database.c_str(), mysql_.pass.c_str(),
            mysql_error(&mysql_.mysql));
        return false;
      }
      mysql_.isopen = true;
      return true;
    }

    int MysqlDatabaseHelper::close_mysql()
    {
      if (mysql_.isopen)
      {
        mysql_close(&mysql_.mysql);
      }
      return 0;
    }

    bool MysqlDatabaseHelper::excute_stmt(MYSQL_STMT *stmt, int64_t& mysql_proc_ret)
    {
      bool ret = true;
      int status;
      status = mysql_stmt_execute(stmt);
      if (status)
      {
        if (ER_DUP_ENTRY == mysql_stmt_errno(stmt))
        {
          mysql_proc_ret = EXIT_KV_RETURN_VERSION_ERROR;
        }
        else
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
        }
        ret = false;
      }
      if (ret)
      {
        mysql_proc_ret = mysql_stmt_affected_rows(stmt);

      }
      mysql_stmt_free_result(stmt);
      mysql_stmt_close(stmt);
      return ret;
    }
  }
}

