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
#ifndef TFS_COMMON_MYSQL_CLUSTER_DATABASE_POOL_H_
#define TFS_COMMON_MYSQL_CLUSTER_DATABASE_POOL_H_
#include <tbsys.h>
#include <Mutex.h>
#include "common/internal.h"
namespace tfs
{
  namespace common
  {
    class MysqlDatabaseHelper;
    class DataBasePool
    {
      public:
        enum
        {
          MAX_POOL_SIZE = 256,
        };
        DataBasePool();
        ~DataBasePool();
        bool init_pool(const int32_t pool_size,
            const char* conn_str, const char* user_name,
            const char* passwd);
        bool destroy_pool(void);
        MysqlDatabaseHelper* get();
        void release(MysqlDatabaseHelper* database_helper);
      private:
        struct DataBaseInfo
        {
          DataBaseInfo():database_helper_(NULL), busy_flag_(true){}
          MysqlDatabaseHelper* database_helper_;
          bool busy_flag_;
        };
        DataBaseInfo base_info_[MAX_POOL_SIZE];
        int32_t pool_size_;
        int32_t choose_index_;
        tbutil::Mutex mutex_;
      private:
        DISALLOW_COPY_AND_ASSIGN(DataBasePool);
    };
  }
}
#endif
