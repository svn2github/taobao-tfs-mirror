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

#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <vector>
#include "common/define.h"

using namespace std;
namespace 
{
  struct mysql_ex {
    string host;
    int port;
    string user;
    string pass;
    string database;
    bool   isopen;
    bool   inited;
    MYSQL  mysql;
  };

  static mysql_ex  mysql_;
  static int split_string(const char* line, const char del, vector<string> & fields) 
  {
    const char* start = line;
    const char* p = NULL;
    char buffer[256];
    while (start != NULL) 
    {
      p = strchr(start, del);
      if (p != NULL) 
      {
        memset(buffer, 0, 256);
        strncpy(buffer, start, p - start);
        if (strlen(buffer) > 0) fields.push_back(buffer);
        start = p + 1;
      } 
      else 
      {
        memset(buffer, 0, 256);
        strcpy(buffer, start);
        if (strlen(buffer) > 0) fields.push_back(buffer);
        break;
      }
    }
    return fields.size();
  }
  static bool init_mysql(const char* mysqlconn, const char* user_name, const char* passwd) 
  {
    vector<string> fields;
    split_string(mysqlconn, ':', fields);
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

  static bool open_mysql()
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

  static int close_mysql()
  {
    if (mysql_.isopen) 
    {
      mysql_close(&mysql_.mysql);
    }
    return 0;
  }
  static bool excute_stmt(MYSQL_STMT *stmt, int32_t& mysql_proc_ret)
  {
    bool ret = true;
    int status;
    my_bool    is_null;    /* input parameter nullability */
    status = mysql_stmt_execute(stmt);
    if (status)
    {
      TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
          mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
      ret = false;
    }
    if (ret)
    {
      int num_fields;       /* number of columns in result */
      MYSQL_BIND rs_bind;  /* for output buffers */

      /* the column count is > 0 if there is a result set */
      /* 0 if the result is only the final status packet */
      num_fields = mysql_stmt_field_count(stmt);
      TBSYS_LOG(DEBUG, "num_fields = %d", num_fields);
      if (num_fields == 1)
      {

        memset(&rs_bind, 0, sizeof (MYSQL_BIND));

        /* set up and bind result set output buffers */
        rs_bind.buffer_type = MYSQL_TYPE_LONG;
        rs_bind.is_null = &is_null;

        rs_bind.buffer = (char *) &mysql_proc_ret;
        rs_bind.buffer_length = sizeof(mysql_proc_ret);

        status = mysql_stmt_bind_result(stmt, &rs_bind);
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = false;
        }
        while (ret)
        {
          status = mysql_stmt_fetch(stmt);

          if (status == MYSQL_NO_DATA)
          {
            break;
          }
          if (status == 1)
          {
            TBSYS_LOG(ERROR, "mysql_stmt_fetch error");
          }
          TBSYS_LOG(DEBUG, "mysql_proc_ret = %d", mysql_proc_ret);
        }
        mysql_next_result(&mysql_.mysql); //mysql bugs, we must have this
      }
      else
      {
        TBSYS_LOG(ERROR, "num_fields = %d have debug info in prcedure?", num_fields);
        ret = false;
      }

      mysql_stmt_free_result(stmt);
      mysql_stmt_close(stmt);
    }
    return ret;
  }
}

namespace tfs
{
  namespace namemetaserver
  {
    using namespace common;
    MysqlDatabaseHelper::~MysqlDatabaseHelper()
    {
      close();
      mysql_server_end();
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

    int MysqlDatabaseHelper::create_dir(const int64_t app_id, const int64_t uid,
        const int64_t ppid, const char* pname, const int32_t pname_len,
        const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
        int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[7];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL create_dir(?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[3].buffer = (char *) pname;
          ps_params[3].length = &_pname_len;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[4].buffer = (char *) &pid;
          ps_params[4].length = 0;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &id;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[6].buffer = (char *) name;
          ps_params[6].length = &_name_len;
          ps_params[6].is_null = 0;

          //ps_params[7].buffer_type = MYSQL_TYPE_LONG;
          //ps_params[7].buffer = (char *) &mysql_proc_ret;
          //ps_params[7].length = 0;
          //ps_params[7].is_null = 0;

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
      return ret;
    }
    int MysqlDatabaseHelper::rm_dir(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int32_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int32_t name_len, int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[7];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL rm_dir(?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[3].buffer = (char *) pname;
          ps_params[3].length = &_pname_len;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[4].buffer = (char *) &pid;
          ps_params[4].length = 0;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &id;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[6].buffer = (char *) name;
          ps_params[6].length = &_name_len;
          ps_params[6].is_null = 0;

          //ps_params[7].buffer_type = MYSQL_TYPE_LONG;
          //ps_params[7].buffer = (char *) &mysql_proc_ret;
          //ps_params[7].length = 0;
          //ps_params[7].is_null = 0;

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
      return ret;
    }

    int MysqlDatabaseHelper::mv_dir(const int64_t app_id, const int64_t uid, 
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[10];  /* input parameter buffers */
      unsigned long _s_pname_len = s_pname_len;
      unsigned long _d_pname_len = d_pname_len;
      unsigned long _s_name_len = s_name_len;
      unsigned long _d_name_len = d_name_len;
      int        status;
      const char* str = "CALL mv_dir(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &s_ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &s_pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) s_pname;
          ps_params[4].length = &_s_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &d_ppid;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[6].buffer = (char *) &d_pid;
          ps_params[6].length = 0;
          ps_params[6].is_null = 0;

          ps_params[7].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[7].buffer = (char *) d_pname;
          ps_params[7].length = &_d_pname_len;
          ps_params[7].is_null = 0;

          ps_params[8].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[8].buffer = (char *) s_name;
          ps_params[8].length = &_s_name_len;
          ps_params[8].is_null = 0;

          ps_params[9].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[9].buffer = (char *) d_name;
          ps_params[9].length = &_d_name_len;
          ps_params[9].is_null = 0;


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
      return ret;
    }
    int MysqlDatabaseHelper::create_file(const int64_t app_id, const int64_t uid, 
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[6];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL create_file(?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) pname;
          ps_params[4].length = &_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[5].buffer = (char *) name;
          ps_params[5].length = &_name_len;
          ps_params[5].is_null = 0;

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
      return ret;
    }
    int MysqlDatabaseHelper::rm_file(const int64_t app_id, const int64_t uid, 
            const int64_t ppid, const int64_t pid, const char* pname, const int32_t pname_len,
            const char* name, const int32_t name_len, int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[6];  /* input parameter buffers */
      unsigned long _pname_len = pname_len;
      unsigned long _name_len = name_len;
      int        status;
      const char* str = "CALL rm_file(?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) pname;
          ps_params[4].length = &_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[5].buffer = (char *) name;
          ps_params[5].length = &_name_len;
          ps_params[5].is_null = 0;

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
      return ret;
    }
    int MysqlDatabaseHelper::pwrite_file(const int64_t app_id, const int64_t uid, 
        const int64_t pid, const char* name, const int32_t name_len,
        const int64_t size, const int16_t ver_no, const char* meta_info, const int32_t meta_len,
        int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[7];  /* input parameter buffers */
      unsigned long _name_len = name_len;
      unsigned long _meta_len = meta_len;
      int        status;
      const char* str = "CALL pwrite_file(?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &pid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[3].buffer = (char *) name;
          ps_params[3].length = &_name_len;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[4].buffer = (char *) &size;
          ps_params[4].length = 0;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_SHORT;
          ps_params[5].buffer = (char *) &ver_no;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[6].buffer = (char *) meta_info;
          ps_params[6].length = &_meta_len;
          ps_params[6].is_null = 0;

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
      return ret;
    }
    int MysqlDatabaseHelper::mv_file(const int64_t app_id, const int64_t uid, 
        const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
        const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
        const char* s_name, const int32_t s_name_len,
        const char* d_name, const int32_t d_name_len,
        int32_t& mysql_proc_ret)
    {
      int ret = TFS_ERROR;
      MYSQL_STMT *stmt;
      MYSQL_BIND ps_params[10];  /* input parameter buffers */
      unsigned long _s_pname_len = s_pname_len;
      unsigned long _d_pname_len = d_pname_len;
      unsigned long _s_name_len = s_name_len;
      unsigned long _d_name_len = d_name_len;
      int        status;
      const char* str = "CALL mv_file(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      mysql_proc_ret = 0;

      tbutil::Mutex::Lock lock(mutex_);
      if (!is_connected_)
      {
        connect();
      }
      if (is_connected_)
      {
        stmt = mysql_stmt_init(&mysql_.mysql);
        ret = TFS_SUCCESS;
        status = mysql_stmt_prepare(stmt, str, strlen(str)); //TODO prepare once
        if (status)
        {
          TBSYS_LOG(ERROR, "Error: %s (errno: %d)\n",
              mysql_stmt_error(stmt), mysql_stmt_errno(stmt));
          ret = TFS_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          memset(ps_params, 0, sizeof (ps_params));
          ps_params[0].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[0].buffer = (char *) &app_id;
          ps_params[0].length = 0;
          ps_params[0].is_null = 0;

          ps_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[1].buffer = (char *) &uid;
          ps_params[1].length = 0;
          ps_params[1].is_null = 0;

          ps_params[2].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[2].buffer = (char *) &s_ppid;
          ps_params[2].length = 0;
          ps_params[2].is_null = 0;

          ps_params[3].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[3].buffer = (char *) &s_pid;
          ps_params[3].length = 0;
          ps_params[3].is_null = 0;

          ps_params[4].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[4].buffer = (char *) s_pname;
          ps_params[4].length = &_s_pname_len;
          ps_params[4].is_null = 0;

          ps_params[5].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[5].buffer = (char *) &d_ppid;
          ps_params[5].length = 0;
          ps_params[5].is_null = 0;

          ps_params[6].buffer_type = MYSQL_TYPE_LONGLONG;
          ps_params[6].buffer = (char *) &d_pid;
          ps_params[6].length = 0;
          ps_params[6].is_null = 0;

          ps_params[7].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[7].buffer = (char *) d_pname;
          ps_params[7].length = &_d_pname_len;
          ps_params[7].is_null = 0;

          ps_params[8].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[8].buffer = (char *) s_name;
          ps_params[8].length = &_s_name_len;
          ps_params[8].is_null = 0;

          ps_params[9].buffer_type = MYSQL_TYPE_VAR_STRING;
          ps_params[9].buffer = (char *) d_name;
          ps_params[9].length = &_d_name_len;
          ps_params[9].is_null = 0;


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
      return ret;
    }
  }
}

