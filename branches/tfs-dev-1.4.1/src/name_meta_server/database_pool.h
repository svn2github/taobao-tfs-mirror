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
#ifndef TFS_NAMEMETASERVER_DATABASE_POOL_H_
#define TFS_NAMEMETASERVER_DATABASE_POOL_H_
#include <tbsys.h>
#include <Mutex.h>
#include "common/internal.h"
namespace tfs
{
  namespace namemetaserver
  {
    class DatabaseHelper;
    class DataBasePool
    {
      public:
        explicit DataBasePool(const int32_t pool_size);
        ~DataBasePool();
        bool init_pool(const char** conn_str, const char** user_name,
            const char** passwd, const int32_t* hash_flag);
        DatabaseHelper* get(const int32_t hash_flag);
        void release(DatabaseHelper* database_helper);
      private:
        enum
        {
          MAX_POOL_SIZE = 20,
        };
        struct DataBaseInfo
        {
          DataBaseInfo():database_helper_(NULL), busy_flag_(true), hash_flag_(-1) {}
          DatabaseHelper* database_helper_;
          bool busy_flag_;
          int32_t hash_flag_;
        };
        DataBaseInfo base_info_[MAX_POOL_SIZE];
        int32_t pool_size_;
        tbutil::Mutex mutex_;
      private:
        DISALLOW_COPY_AND_ASSIGN(DataBasePool);
    };
  }
}
#endif
