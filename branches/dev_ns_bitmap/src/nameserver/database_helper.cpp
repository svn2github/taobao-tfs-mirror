/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: database_helper.cpp 2140 2012-07-18 10:28:04Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "common/error_msg.h"
#include "database_helper.h"
using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    DataBaseHelper::DataBaseHelper(std::string& connstr, std::string& username, std::string& password):
      connstr_(connstr),
      username_(username),
      password_(password),
      port_(0),
      is_connected_(false)
    {

    }

    DataBaseHelper::~DataBaseHelper()
    {
      disconnect_();
    }

    int DataBaseHelper::initialize_()
    {
      std::vector<std::string> fields;
      tfs::common::Func::split_string(connstr_.c_str(), ':', fields);
      is_connected_ = false;
      int32_t ret = fields.size() < 3 ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        hostname_ = fields[0];
        port_ = atoi(fields[1].c_str());
        dbname_   = fields[2];
        int32_t timeout = 5;//seconds
        mysql_init(&mysql_);
        mysql_options(&mysql_, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&timeout);
        mysql_options(&mysql_, MYSQL_OPT_READ_TIMEOUT, (const char *)&timeout);
        mysql_options(&mysql_, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&timeout);
      }
      return ret;
    }

    int DataBaseHelper::create_family_id(int64_t& family_id)
    {
      family_id = -1;
      int32_t ret = TFS_SUCCESS;
      int32_t retry = 0, mysql_errno = 0;
      const char* sql = "CALL erasurecode_seq_nextval();";
      do
      {
        if (!is_connected_)
          connect_();

        if (is_connected_)
        {
          tbutil::Mutex::Lock lock(mutex_);
          MYSQL_STMT* stmt = mysql_stmt_init(&mysql_);
          ret = mysql_stmt_prepare(stmt, sql, strlen(sql)) ? EXIT_PREPARE_SQL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            mysql_errno = mysql_stmt_errno(stmt);
            TBSYS_LOG(ERROR, "prepare sql: %s error: %d=>%s",
                sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
          }
          if (TFS_SUCCESS == ret)
          {
            ret = excute_stmt_(stmt);
            if (TFS_SUCCESS == ret)
            {
              int32_t num_fields = mysql_stmt_field_count(stmt);
              ret = 1 == num_fields ? TFS_SUCCESS : EXIT_MYSQL_FETCH_DATA_ERROR;
              if (TFS_SUCCESS == ret)
              {
                my_bool is_null[1];
                MYSQL_BIND rs_bind[1];
                memset(rs_bind, 0, sizeof(rs_bind));
                rs_bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
                rs_bind[0].is_null = &is_null[0];
                rs_bind[0].buffer = (char *) &family_id;
                rs_bind[0].buffer_length = sizeof(family_id);

                ret = mysql_stmt_bind_result(stmt, rs_bind) ?  EXIT_BIND_PARAMETER_ERROR : TFS_SUCCESS;
                if (TFS_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                      mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                }
                if (TFS_SUCCESS == ret)
                {
                  ret = mysql_stmt_store_result(stmt) ? EXIT_EXECUTE_SQL_ERROR : TFS_SUCCESS;
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "mysql store result: %s error: %d=>%s", sql,
                        mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                  }
                }

                int32_t result = 0;
                while (TFS_SUCCESS == ret && result == 0)
                {
                  result = mysql_stmt_fetch(stmt);
                  if (0 != result && MYSQL_NO_DATA != result)
                  {
                    if (MYSQL_DATA_TRUNCATED == result)
                      ret = EXIT_MYSQL_DATA_TRUNCATED;
                    else
                      ret = EXIT_MYSQL_FETCH_DATA_ERROR;
                  }
                  TBSYS_LOG(DEBUG, "family_id: %"PRI64_PREFIX"d", family_id);
                  mysql_next_result(&mysql_);
                }
              }
              mysql_stmt_free_result(stmt);
            }
            if (TFS_SUCCESS != ret)
               disconnect_();
          }
          mysql_stmt_close(stmt);
        }
      }
      while (2006 == mysql_errno && retry++ < 3);

      return ret;
    }

    int DataBaseHelper::create_family(FamilyInfo& family_info, int64_t& mysql_ret)
    {
      mysql_ret = -1;
      int32_t ret = TFS_SUCCESS;
      int32_t retry = 0, mysql_errno = 0;
      const char* sql = "CALL create_family(?,?,?)";
      do
      {
        if (!is_connected_)
          connect_();

        if (is_connected_)
        {
          tbutil::Mutex::Lock lock(mutex_);
          MYSQL_STMT* stmt = mysql_stmt_init(&mysql_);
          ret = mysql_stmt_prepare(stmt, sql, strlen(sql)) ? EXIT_PREPARE_SQL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            mysql_errno = mysql_stmt_errno(stmt);
            TBSYS_LOG(ERROR, "prepare sql: %s error: %d=>%s",
                sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
          }
          if (TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            const int32_t MAX_VALUE_LENGTH = 1024;
            char value[MAX_VALUE_LENGTH] = {'\0'};
            ret = TFS_SUCCESS != family_info.serialize(value, MAX_VALUE_LENGTH, pos) ? EXIT_SERIALIZE_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              MYSQL_BIND bind_paramter[3];
              memset(&bind_paramter, 0, sizeof(bind_paramter));
              bind_paramter[0].buffer_type = MYSQL_TYPE_LONGLONG;
              bind_paramter[0].buffer = (char *)&family_info.family_id_;
              bind_paramter[0].is_null = 0;
              bind_paramter[0].length = 0;

              bind_paramter[1].buffer_type = MYSQL_TYPE_LONG;
              bind_paramter[1].buffer = (char *)&family_info.family_aid_info_;
              bind_paramter[1].is_null = 0;
              bind_paramter[1].length = 0;

              bind_paramter[2].buffer_type = MYSQL_TYPE_BLOB;
              bind_paramter[2].buffer = (char *) value;
              bind_paramter[2].length = (long unsigned int*)&pos;
              bind_paramter[2].is_null = 0;

              if (mysql_stmt_bind_param(stmt, bind_paramter))
              {
                TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                  mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                ret = EXIT_BIND_PARAMETER_ERROR;
              }

              if (TFS_SUCCESS == ret)
              {
                ret = excute_stmt_(stmt);
                if (TFS_SUCCESS == ret)
                {
                  int32_t num_fields = mysql_stmt_field_count(stmt);
                  ret = 1 == num_fields ? TFS_SUCCESS : EXIT_MYSQL_FETCH_DATA_ERROR;
                  if (TFS_SUCCESS == ret)
                  {
                    my_bool is_null[1];
                    MYSQL_BIND rs_bind[1];
                    memset(rs_bind, 0, sizeof(rs_bind));
                    rs_bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
                    rs_bind[0].is_null = &is_null[1];
                    rs_bind[0].buffer = (char *) &mysql_ret;
                    rs_bind[0].buffer_length = sizeof(mysql_ret);
                    ret = mysql_stmt_bind_result(stmt, rs_bind) ?  EXIT_BIND_PARAMETER_ERROR : TFS_SUCCESS;
                    if (TFS_SUCCESS != ret)
                    {
                      TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                          mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                    }
                    if (TFS_SUCCESS == ret)
                    {
                      ret = mysql_stmt_store_result(stmt) ? EXIT_MYSQL_FETCH_DATA_ERROR : TFS_SUCCESS;
                      if (TFS_SUCCESS != ret)
                      {
                        TBSYS_LOG(ERROR, "mysql store result: error: %d=>%s",
                            mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                      }
                    }
                    int32_t result = 0;
                    while (TFS_SUCCESS == ret && 0 == result)
                    {
                      result = mysql_stmt_fetch(stmt);
                      if (0 != result && MYSQL_NO_DATA != result)
                      {
                        if (MYSQL_DATA_TRUNCATED == result)
                          ret = EXIT_MYSQL_DATA_TRUNCATED;
                        else
                          ret = EXIT_MYSQL_FETCH_DATA_ERROR;
                      }
                      TBSYS_LOG(DEBUG, "mysql_ret: %"PRI64_PREFIX"d", mysql_ret);
                      mysql_next_result(&mysql_);
                    }
                  }
                }
              }
              mysql_stmt_free_result(stmt);
            }
            if (TFS_SUCCESS != ret && EXIT_SERIALIZE_ERROR != ret)
               disconnect_();
          }
          mysql_stmt_close(stmt);
        }
      }
      while (2006 == mysql_errno && retry++ < 3);

      return ret;
    }

    int DataBaseHelper::del_family(int64_t& mysql_ret, const int64_t family_id)
    {
      int32_t ret = TFS_SUCCESS;
      int32_t retry = 0, mysql_errno = 0;
      const char* sql = "CALL del_family(?)";
      do
      {
        if (!is_connected_)
          connect_();

        if (is_connected_)
        {
          tbutil::Mutex::Lock lock(mutex_);
          MYSQL_STMT* stmt = mysql_stmt_init(&mysql_);
          int32_t ret = mysql_stmt_prepare(stmt, sql, strlen(sql)) ? EXIT_PREPARE_SQL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            mysql_errno = mysql_stmt_errno(stmt);
            TBSYS_LOG(ERROR, "prepare sql: %s error: %d=>%s",
                sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
          }
          if (TFS_SUCCESS == ret)
          {
            MYSQL_BIND bind_paramter[1];
            memset(&bind_paramter, 0, sizeof(bind_paramter));

            bind_paramter[0].buffer_type = MYSQL_TYPE_LONGLONG;
            bind_paramter[0].buffer = (char *)&family_id;
            bind_paramter[0].is_null = 0;
            bind_paramter[0].length = 0;

            if (mysql_stmt_bind_param(stmt, bind_paramter))
            {
              TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
              ret = EXIT_BIND_PARAMETER_ERROR;
            }

            if (TFS_SUCCESS == ret)
            {
              ret = excute_stmt_(stmt);
              if (TFS_SUCCESS == ret)
              {
                int32_t num_fields = mysql_stmt_field_count(stmt);
                ret = 1 == num_fields ? TFS_SUCCESS : EXIT_MYSQL_FETCH_DATA_ERROR;
                if (TFS_SUCCESS == ret)
                {
                  my_bool is_null[1];
                  MYSQL_BIND rs_bind[1];
                  memset(rs_bind, 0, sizeof(rs_bind));
                  rs_bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
                  rs_bind[0].is_null = &is_null[0];
                  rs_bind[0].buffer = (char *) &mysql_ret;
                  rs_bind[0].buffer_length = sizeof(mysql_ret);
                  ret = mysql_stmt_bind_result(stmt, rs_bind) ?  EXIT_BIND_PARAMETER_ERROR : TFS_SUCCESS;
                  if (TFS_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                        mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                  }
                  if (TFS_SUCCESS == ret)
                  {
                    ret = mysql_stmt_store_result(stmt) ? EXIT_EXECUTE_SQL_ERROR : TFS_SUCCESS;
                    if (TFS_SUCCESS != ret)
                    {
                      TBSYS_LOG(ERROR, "mysql store result: %s error: %d=>%s", sql,
                          mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
                    }
                  }
                  int32_t result = 0;
                  while (TFS_SUCCESS == ret && 0 == result)
                  {
                    result = mysql_stmt_fetch(stmt);
                    if (0 != result && MYSQL_NO_DATA != result)
                    {
                      if (MYSQL_DATA_TRUNCATED == result)
                        ret = EXIT_MYSQL_DATA_TRUNCATED;
                      else
                        ret = EXIT_MYSQL_FETCH_DATA_ERROR;
                    }
                    TBSYS_LOG(DEBUG, "mysql_ret: %"PRI64_PREFIX"d", mysql_ret);
                    mysql_next_result(&mysql_);
                  }
                }
              }
              mysql_stmt_free_result(stmt);
            }
            if (TFS_SUCCESS != ret && EXIT_SERIALIZE_ERROR != ret)
              disconnect_();
          }
          mysql_stmt_close(stmt);
        }
      }
      while (2006 == mysql_errno && retry++ < 3);

      return ret;
    }

    int DataBaseHelper::connect_()
    {
      if (is_connected_)
        disconnect_();

      tbutil::Mutex::Lock lock(mutex_);
      int32_t ret = initialize_();
      if (TFS_SUCCESS == ret)
      {
        MYSQL* new_conn = mysql_real_connect(&mysql_, hostname_.c_str(), username_.c_str(),
            password_.c_str(), dbname_.c_str(), port_,
            NULL, CLIENT_MULTI_STATEMENTS);
        ret = NULL != new_conn ? TFS_SUCCESS : EXIT_CONNECT_MYSQL_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "connect mysql database: %s %d %s %s %s error: %s:%d", hostname_.c_str(), port_,dbname_.c_str(),
              username_.c_str(), password_.c_str(), mysql_error(&mysql_), mysql_errno(&mysql_));
        }
        else
        {
          char sql[256];
          const char* name = "erasurecode_sequence";
          long unsigned int length = strlen(name);
          snprintf(sql, 256, "select current_value from t_erasurecode_sequence where name = ?");
          MYSQL_STMT* stmt = mysql_stmt_init(&mysql_);
          if (mysql_stmt_prepare(stmt, sql, strlen(sql)))
          {
            TBSYS_LOG(ERROR, "prepare sql: %s error: %d=>%s",
                sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
            ret = EXIT_PREPARE_SQL_ERROR;
          }
          else
          {
            MYSQL_BIND bind_paramter[1];
            memset(&bind_paramter, 0, sizeof(bind_paramter));
            bind_paramter[0].buffer_type = MYSQL_TYPE_VAR_STRING;
            bind_paramter[0].buffer = (char *) name;
            bind_paramter[0].length = &length;
            bind_paramter[0].is_null = 0;
            if (mysql_stmt_bind_param(stmt, bind_paramter))
            {
              TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
              ret = EXIT_BIND_PARAMETER_ERROR;
            }
          }
          if (TFS_SUCCESS != ret)
            mysql_close(&mysql_);
        }
      }
      is_connected_ = TFS_SUCCESS == ret;
      return ret;
    }

    int DataBaseHelper::disconnect_()
    {
      if (is_connected_)
        mysql_close(&mysql_);
      return TFS_SUCCESS;
    }

    int DataBaseHelper::excute_stmt_(MYSQL_STMT* stmt)
    {
      int32_t ret = NULL != stmt ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = mysql_stmt_execute(stmt) ? EXIT_EXECUTE_SQL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "exectue sql error %d=>%s",
              mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
        }
      }
      return ret;
    }

    int DataBaseHelper::scan(std::vector<FamilyInfo>& family_infos, const int64_t start_family_id)
    {
      char sql[512];
      int32_t ret = TFS_SUCCESS;
      const int32_t ROW_LIMIT = 3;
      int32_t retry = 0, mysql_errno = 0;
      snprintf(sql, 512, "select family_id, family_aid_info, member_infos from t_family_info"
              " where family_id > %"PRI64_PREFIX"d limit %d",
              start_family_id,ROW_LIMIT);
      do
      {
        if (!is_connected_)
          connect_();

        if (is_connected_)
        {
          tbutil::Mutex::Lock lock(mutex_);
          family_infos.clear();
          MYSQL_STMT* stmt = mysql_stmt_init(&mysql_);
          int32_t ret = mysql_stmt_prepare(stmt, sql, strlen(sql)) ? EXIT_PREPARE_SQL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "prepare sql: %s error: %d=>%s",
              sql, mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
          }
          if (TFS_SUCCESS == ret)
          {
            ret = mysql_stmt_execute(stmt) ? EXIT_EXECUTE_SQL_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              mysql_errno = mysql_stmt_errno(stmt);
              TBSYS_LOG(ERROR, "execute sql: %s error: %d=>%s", sql,
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
            }
          }

          if (TFS_SUCCESS == ret)
          {
            my_bool isnull[3];
            unsigned long length[3];
            const int32_t MAX_VALUE_LENGTH = 1024;
            char value[MAX_VALUE_LENGTH];
            int64_t family_id;
            int32_t family_aid_info;

            MYSQL_BIND bind_paramter[3];
            memset(&length , 0, sizeof(length));
            memset(&bind_paramter, 0, sizeof(bind_paramter));
            bind_paramter[0].buffer_type = MYSQL_TYPE_LONGLONG;
            bind_paramter[0].buffer = (char *)&family_id;
            bind_paramter[0].is_null = &isnull[0];
            bind_paramter[0].length = &length[0];

            bind_paramter[1].buffer_type = MYSQL_TYPE_LONG;
            bind_paramter[1].buffer = (char *)&family_aid_info;
            bind_paramter[1].is_null = &isnull[1];
            bind_paramter[1].length = &length[1];

            bind_paramter[2].buffer_type = MYSQL_TYPE_BLOB;
            bind_paramter[2].buffer = (char *) value;
            bind_paramter[2].buffer_length = MAX_VALUE_LENGTH;
            bind_paramter[2].length = &length[2];
            bind_paramter[2].is_null = &isnull[2];

            if (mysql_stmt_bind_result(stmt, bind_paramter))
            {
              TBSYS_LOG(ERROR, "bind parameter: error: %d=>%s",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
              ret = EXIT_BIND_PARAMETER_ERROR;
            }

            if (TFS_SUCCESS == ret)
            {
              ret = mysql_stmt_store_result(stmt) ? EXIT_EXECUTE_SQL_ERROR : TFS_SUCCESS;
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "mysql store result: %s error: %d=>%s", sql,
                  mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
              }
            }
            if (TFS_SUCCESS == ret)
            {
              int32_t result = 0;
              do
              {
                result = mysql_stmt_fetch(stmt);
                if (0 != result && MYSQL_NO_DATA != result)
                {
                  TBSYS_LOG(ERROR, "mysql fetch data error: %d=>%s, result: %d",
                    mysql_stmt_errno(stmt), mysql_stmt_error(stmt), result);
                  if (MYSQL_DATA_TRUNCATED == result)
                    ret = EXIT_MYSQL_DATA_TRUNCATED;
                  else if (MYSQL_NO_DATA == result)
                    ret = EXIT_MYSQL_NO_DATA;
                  else
                    ret = EXIT_MYSQL_FETCH_DATA_ERROR;
                }

                if (0 == result)
                {
                  int64_t pos = 0;
                  int64_t data_length = length[2];
                  family_infos.push_back(FamilyInfo());
                  std::vector<FamilyInfo>::iterator iter = family_infos.end() - 1;
                  (*iter).family_id_ = family_id;
                  (*iter).family_aid_info_ = family_aid_info;
                  assert(!isnull[2]);
                  ret = TFS_SUCCESS != (*iter).deserialize(value, data_length, pos) ? EXIT_DESERIALIZE_ERROR : TFS_SUCCESS;
                }
              }
              while (0 == result && TFS_SUCCESS == ret);
              mysql_stmt_free_result(stmt);
            }
            if (TFS_SUCCESS != ret && EXIT_DESERIALIZE_ERROR != ret)
              disconnect_();
          }
          mysql_stmt_close(stmt);
        }
      }
      while (2006 == mysql_errno && retry++ < 3);

      return ret;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
