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
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#define TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#include "Memory.hpp"
#include "meta_server_define.h"
#include "meta_info.h"
#include "mysql_database_helper.h"
#include "database_pool.h"

namespace tfs
{
  namespace namemetaserver
  {
    class MetaStoreManager
    {
      public:
        MetaStoreManager();
        ~MetaStoreManager();
        int init(const int32_t pool_size);

        int select(const int64_t app_id, const int64_t uid,
            const int64_t pid, const char* name, const int32_t name_len, const bool is_file, std::vector<MetaInfo>& out_v_meta_info);

        int insert(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const char* name, const int32_t name_len,
            const FileType type, MetaInfo* meta_info = NULL);

        int update(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len,
            const FileType type);

        int remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int64_t name_len,
            const FileType type);
      private:
        DataBasePool* database_pool_;
    };
  }
}

#endif
