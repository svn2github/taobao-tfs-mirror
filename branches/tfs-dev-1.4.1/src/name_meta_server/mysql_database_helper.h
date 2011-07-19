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
#ifndef TFS_NAMEMETASERVER_MYSQL_DATABASE_HELPER_H_
#define TFS_NAMEMETASERVER_MYSQL_DATABASE_HELPER_H_
#include <tbsys.h>
#include <Mutex.h>
#include "database_helper.h"
namespace tfs
{
  namespace namemetaserver
  {
    class MysqlDatabaseHelper :public DatabaseHelper
    {
      public:
        virtual ~MysqlDatabaseHelper();
        virtual int connect();
        virtual int close();

        virtual int create_dir(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const int64_t id, const char* name, const int32_t name_len,
            int32_t& mysql_proc_ret);


      private:

      private:
        tbutil::Mutex mutex_;

    };

  }
}
#endif
