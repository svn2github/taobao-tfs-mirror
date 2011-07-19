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
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 */

#ifndef TFS_NAMEMETASERVER_DATABASE_HELPER_H_
#define TFS_NAMEMETASERVER_DATABASE_HELPER_H_
#include "common/internal.h"
namespace tfs
{
  namespace namemetaserver
  {
    class DatabaseHelper
    {
      public:
        DatabaseHelper();
        virtual ~DatabaseHelper();

        bool is_connected() const;
        int set_conn_param(const char* conn_str, const char* user_name, const char* passwd);

        virtual int connect() = 0;
        virtual int close() = 0;

        virtual int create_dir(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
            int32_t& mysql_proc_ret) = 0;

      protected:
        enum 
        {
          CONN_STR_LEN = 256,
        };
        char conn_str_[CONN_STR_LEN];
        char user_name_[CONN_STR_LEN];
        char passwd_[CONN_STR_LEN];
        bool is_connected_;

      private:
        DISALLOW_COPY_AND_ASSIGN(DatabaseHelper);
    };
  }
}
#endif
