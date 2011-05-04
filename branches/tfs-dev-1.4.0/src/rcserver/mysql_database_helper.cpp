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
#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <vector>
#include "mysql_database_helper.h"

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
    while (start != NULL) {
        p = strchr(start, del);
        if (p != NULL) {
            memset(buffer, 0, 256);
            strncpy(buffer, start, p - start);
            if (strlen(buffer) > 0) fields.push_back(buffer);
            start = p + 1;
        } else {
            memset(buffer, 0, 256);
            strcpy(buffer, start);
            if (strlen(buffer) > 0) fields.push_back(buffer);
            break;
        }
    }
    /*
    for (int i = 0; i < fields.size(); ++i) {
        printf("i:%d, fds:(%s)\n", i, fields[i].c_str());
    }
    */
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
            mysql_.port, NULL, 0);
    if (!conn) {
        TBSYS_LOG(ERROR, "connect mysql database (%s:%s:%s) error(%s)",
                mysql_.host.c_str(), mysql_.user.c_str(), mysql_.database.c_str(),
                mysql_error(&mysql_.mysql));
        return false;
    }
    mysql_.isopen = true;
    return true;
}

static int close_mysql()
{
    if (mysql_.isopen) {
        mysql_close(&mysql_.mysql);
    }
    return 0;
}
}

namespace tfs
{
  namespace rcserver
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
        close_mysql();
        is_connected_ = false;
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
    }

    int MysqlDatabaseHelper::select(const ResourceServerInfo& inparam, ResourceServerInfo& outparam)
    {
      //TODO
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::update(const ResourceServerInfo& inparam)
    {
      //TODO 
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::remove(const ResourceServerInfo& inparam)
    {
      //TODO
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::scan(VResourceServerInfo& outparam)
    {
      outparam.clear();
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      if (is_connected_)
      {
        char sql[1024];
        char table[256];
        snprintf(table, 256, "%s", "T_RESOURCE_SERVER_INFO");
        snprintf(sql, 1024, "select ADDR_INFO, STAT, REM from %s", table);
        ret = mysql_query(&mysql_.mysql, sql);
        if (ret) {
          TBSYS_LOG(WARN, "query (%s) failure: %s %s", sql,  mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          return TFS_ERROR;
        }

        MYSQL_ROW row;
        ResourceServerInfo tmp;
        MYSQL_RES *mysql_ret = mysql_store_result(&mysql_.mysql);
        if (mysql_ret == NULL) {
          TBSYS_LOG(WARN, "mysql_store_result failure: %s %s", mysql_.host.c_str(), mysql_error(&mysql_.mysql));
          close();
          ret = TFS_ERROR;
          goto error;
        }

        while(row = mysql_fetch_row(mysql_ret))
        {
          snprintf(tmp.addr_info_, ADDR_INFO_LEN, "%s", row[0]);
          tmp.stat_ = atoi(row[1]);
          snprintf(tmp.rem_, REM_LEN, "%s", row[2]);
          outparam.push_back(tmp);
        }

error:
        mysql_free_result(mysql_ret);
      }
      return ret;
    }

    int MysqlDatabaseHelper::select(const AppInfo& inparam, AppInfo& outparam)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }
    
    int MysqlDatabaseHelper::update(const AppInfo& inparam)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::remove(const AppInfo& inparam)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }

    int MysqlDatabaseHelper::scan(MIdAppInfo& outparam)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_ERROR;
      return ret;
    }
  }
}
