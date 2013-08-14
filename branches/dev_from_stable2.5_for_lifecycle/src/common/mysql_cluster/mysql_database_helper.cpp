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
#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"

using namespace std;
namespace tfs
{
  namespace common
  {

    MysqlDatabaseHelper::MysqlDatabaseHelper()
    {
      stmt_ = NULL;
      key_buff_size_ = 750;
      key_buff_ = (char*)malloc(key_buff_size_);

      value_buff_size_ = 1024*1024;
      value_buff_ = (char*)malloc(value_buff_size_);
      conn_str_[0] = '\0';
      user_name_[0] = '\0';
      passwd_[0] = '\0';
      is_connected_ = false;
    }
    MysqlDatabaseHelper::~MysqlDatabaseHelper()
    {
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
      int status;
      int64_t mysql_proc_ret = 0;
      int retry_time = 0;
      if (key.key_size_ > MAX_KEY_VAR_SIZE || value.get_size() > MAX_VALUE_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "size error");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[2];  /* input parameter buffers */
        char str[1024];
        snprintf(str, 1024, "insert into tfsmeta_%d (meta_key, meta_value, version) values (?, ?, 1)",
            area);


        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type = MYSQL_TYPE_VAR_STRING;
            ps_params[0].buffer = (char *) key.key_;
            unsigned long key_size_ =key.key_size_;
            ps_params[0].length = &key_size_;
            ps_params[0].is_null = 0;

            ps_params[1].buffer_type = MYSQL_TYPE_BLOB;
            ps_params[1].buffer = (char *) value.get_data();
            unsigned long value_size_ = value.get_size();
            ps_params[1].length = &value_size_;
            ps_params[1].is_null = 0;


            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              if (!excute_stmt(stmt, mysql_proc_ret))
              {
                if (EXIT_KV_RETURN_VERSION_ERROR != mysql_proc_ret)
                {
                  ret = TFS_ERROR;
                }
              }
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
    int MysqlDatabaseHelper::update_kv(const int area, const KvKey& key, const KvMemValue
        &value, const int32_t version)
    {
      int ret = TFS_SUCCESS;
      int status;
      int64_t mysql_proc_ret = 0;
      int retry_time = 0;
      if (key.key_size_ > MAX_KEY_VAR_SIZE || value.get_size() > MAX_VALUE_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "size error");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[3];  /* input parameter buffers */
        char str[1024];
        snprintf(str, 1024, "update tfsmeta_%d set meta_value=?, version=version+1 where meta_key=? and version=%d", area, version);


        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type = MYSQL_TYPE_BLOB;
            ps_params[0].buffer = (char *) value.get_data();
            unsigned long value_size_ = value.get_size();
            ps_params[0].length = &value_size_;
            ps_params[0].is_null = 0;

            ps_params[1].buffer_type = MYSQL_TYPE_VAR_STRING;
            ps_params[1].buffer = (char *) key.key_;
            unsigned long key_size_ = key.key_size_;
            ps_params[1].length = &key_size_;
            ps_params[1].is_null = 0;

            ps_params[2].buffer_type = MYSQL_TYPE_LONG;
            ps_params[2].buffer = (char *)version;
            ps_params[2].length = 0;
            ps_params[2].is_null = 0;



            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              if (!excute_stmt(stmt, mysql_proc_ret))
              {
                ret = TFS_ERROR;
              }
            }

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
      int status;
      int64_t mysql_proc_ret=0;

      if (key.key_size_ > MAX_KEY_VAR_SIZE)
      {
        TBSYS_LOG(ERROR, "input error");
        ret = TFS_ERROR;
      }
      int retry_time = 0;
      if (TFS_SUCCESS == ret)
      {
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[1];  /* input parameter buffers */
        char str[1024];
        snprintf(str, 1024, "delete from tfsmeta_%d where meta_key=?", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type =MYSQL_TYPE_VAR_STRING;
            ps_params[0].buffer = (char *)key.key_;
            unsigned long key_size_ = key.key_size_;
            ps_params[0].length = &key_size_;
            ps_params[0].is_null = 0;

            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              if (!excute_stmt(stmt, mysql_proc_ret))
              {
                ret = TFS_ERROR;
              }
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
      int status;
      int get_count = 0;

      if (key.key_size_ > MAX_KEY_VAR_SIZE || NULL == pp_value || NULL == version)
      {
        TBSYS_LOG(ERROR, "input error %d %p %p", key.key_size_, pp_value, version);

        ret = TFS_ERROR;
      }
      int retry_time = 0;
      if (TFS_SUCCESS == ret)
      {
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[1];  /* input parameter buffers */
        char str[1024];
        snprintf(str, 1024, "select meta_value, version from tfsmeta_%d where meta_key=?", area);

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type =MYSQL_TYPE_VAR_STRING;
            ps_params[0].buffer = (char *)key.key_;
            unsigned long key_size_ = key.key_size_;
            ps_params[0].length = &key_size_;
            ps_params[0].is_null = 0;

            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              status = mysql_stmt_execute(stmt);
              if (status)
              {
                if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
                {
                  close();
                  goto retry;
                }
                TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                    mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                ret = TFS_ERROR;
              }
              if (TFS_SUCCESS == ret)
              {
                MYSQL_BIND rs_bind[2];  /* for output buffers */
                my_bool    is_null[2];

                memset(rs_bind, 0, sizeof (rs_bind) );
                unsigned long value_len_ = 0;
                int32_t version_ = 0;

                /* set up and bind result set output buffers */
                rs_bind[0].buffer_type = MYSQL_TYPE_BLOB;
                rs_bind[0].is_null = &is_null[0];
                rs_bind[0].buffer_length = value_buff_size_;
                rs_bind[0].buffer = (char *) value_buff_;
                rs_bind[0].length= &value_len_;

                rs_bind[1].buffer_type = MYSQL_TYPE_LONG;
                rs_bind[1].is_null = &is_null[1];
                rs_bind[1].buffer = (char *) &version_;

                status = mysql_stmt_bind_result(stmt, rs_bind);
                if (status)
                {
                  TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                      mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                  ret = TFS_ERROR;
                }
                if (TFS_SUCCESS == ret)
                {
                  status = mysql_stmt_store_result(stmt);
                  if (status)
                  {
                    TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                        mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                    ret = TFS_ERROR;
                  }
                }
                while (TFS_SUCCESS == ret )
                {
                  status = mysql_stmt_fetch(stmt);
                  if (1 == status)
                  {
                    TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                        mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                    ret = TFS_ERROR;
                    break;
                  }
                  else if (MYSQL_NO_DATA == status)
                  {
                    break;
                  }
                  else if (MYSQL_DATA_TRUNCATED == status)
                  {
                    TBSYS_LOG(ERROR, "MYSQL_DATA_TRUNCATED");
                    break;
                  }
                  if (0 == get_count)
                  {
                    KvMemValue* p_value = new KvMemValue();
                    char* data_ = p_value->malloc_data(value_len_);
                    memcpy(data_, value_buff_, value_len_);
                    *pp_value = p_value;
                    *version = version_;
                  }
                  get_count++;

                }
                mysql_next_result(&mysql_.mysql); //mysql bugs, we must have this
              }
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
      int status;
      int get_count = 0;

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
        MYSQL_STMT *stmt;
        MYSQL_BIND ps_params[2];  /* input parameter buffers */
        char str[1024];
        int limit_=limit;
        if (limit < 0)
        {
          limit_ = -limit;
          if (skip_first)
          {
            snprintf(str, 1024, "select meta_key, meta_value, version from tfsmeta_%d where "
                "meta_key<? and meta_key<? order by meta_key desc limit %d", area, limit_);
          }
          else
          {
            snprintf(str, 1024, "select meta_key, meta_value, version from tfsmeta_%d where "
                "meta_key<=? and meta_key<=? order by meta_key desc limit %d", area, limit_);
          }
        }
        else
        {
          if (skip_first)
          {
            snprintf(str, 1024, "select meta_key, meta_value, version from tfsmeta_%d where "
                "meta_key>? and meta_key<=? order by meta_key asc limit %d ", area, limit_);
          }
          else
          {
            snprintf(str, 1024, "select meta_key, meta_value, version from tfsmeta_%d where "
                "meta_key>=? and meta_key<=? order by meta_key asc limit %d ", area, limit_);
          }
        }

        tbutil::Mutex::Lock lock(mutex_);
retry:
        if (!is_connected_)
        {
          connect();
        }
        if (is_connected_)
        {
          stmt = mysql_stmt_init(&mysql_.mysql);
          ret = TFS_SUCCESS;
          status = mysql_stmt_prepare(stmt, str, strlen(str));
          if (status)
          {
            if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
            {
              close();
              goto retry;
            }
            TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
            ret = TFS_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            memset(ps_params, 0, sizeof (ps_params));
            ps_params[0].buffer_type =MYSQL_TYPE_VAR_STRING;
            ps_params[0].buffer = (char *)start_key.key_;
            unsigned long key_size_ = start_key.key_size_;
            ps_params[0].length = &key_size_;
            ps_params[0].is_null = 0;

            ps_params[1].buffer_type =MYSQL_TYPE_VAR_STRING;
            unsigned long value_size_ = 0;
            if (limit < 0 )
            {
              ps_params[1].buffer = (char *)start_key.key_;
              value_size_ = start_key.key_size_;
              ps_params[1].length = &value_size_;
              ps_params[1].is_null = 0;
            }
            else
            {
              ps_params[1].buffer = (char *)end_key.key_;
              value_size_ = end_key.key_size_;
              ps_params[1].length = &value_size_;
              ps_params[1].is_null = 0;
            }

            status = mysql_stmt_bind_param(stmt, ps_params);
            if (status)
            {
              TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                  mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
              ret = TFS_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              status = mysql_stmt_execute(stmt);
              if (status)
              {
                if (2006 == mysql_stmt_errno(stmt) && retry_time++ < 3)
                {
                  close();
                  goto retry;
                }
                TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                    mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                ret = TFS_ERROR;
              }
              if (TFS_SUCCESS == ret)
              {
                MYSQL_BIND rs_bind[3];  /* for output buffers */
                my_bool    is_null[3];

                memset(rs_bind, 0, sizeof (rs_bind) );
                unsigned long key_len_ = 0;
                unsigned long value_len_ = 0;
                int32_t version_ = 0;

                /* set up and bind result set output buffers */
                rs_bind[0].buffer_type = MYSQL_TYPE_BLOB;
                rs_bind[0].is_null = &is_null[0];
                rs_bind[0].buffer_length = key_buff_size_;
                rs_bind[0].buffer = (char *) key_buff_;
                rs_bind[0].length= &key_len_;

                rs_bind[1].buffer_type = MYSQL_TYPE_BLOB;
                rs_bind[1].is_null = &is_null[1];
                rs_bind[1].buffer_length = value_buff_size_;
                rs_bind[1].buffer = (char *) value_buff_;
                rs_bind[1].length= &value_len_;

                rs_bind[2].buffer_type = MYSQL_TYPE_LONG;
                rs_bind[2].is_null = &is_null[2];
                rs_bind[2].buffer = (char *) &version_;

                status = mysql_stmt_bind_result(stmt, rs_bind);
                if (status)
                {
                  TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                      mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                  ret = TFS_ERROR;
                }
                if (TFS_SUCCESS == ret)
                {
                  status = mysql_stmt_store_result(stmt);
                  if (status)
                  {
                    TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                        mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                    ret = TFS_ERROR;
                  }
                }
                while (TFS_SUCCESS == ret )
                {
                  status = mysql_stmt_fetch(stmt);
                  if (1 == status)
                  {
                    TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
                        mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
                    ret = TFS_ERROR;
                    break;
                  }
                  else if (MYSQL_NO_DATA == status)
                  {
                    break;
                  }
                  else if (MYSQL_DATA_TRUNCATED == status)
                  {
                    TBSYS_LOG(ERROR, "MYSQL_DATA_TRUNCATED");
                    break;
                  }
                  KvMemValue* p_key = new KvMemValue();
                  char* data_ =(p_key)->malloc_data(key_len_);
                  memcpy(data_, key_buff_, key_len_);
                  keys->push_back(p_key);

                  KvMemValue* p_value = new KvMemValue();
                  data_ =(p_value)->malloc_data(value_len_);
                  memcpy(data_, value_buff_, value_len_);
                  values->push_back(p_value);

                  get_count++;
                }
                mysql_next_result(&mysql_.mysql); //mysql bugs, we must have this
              }
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
        if (1062 == mysql_stmt_errno(stmt))
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

