/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id$
*
* Authors:
*   chuyu <chuyu@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#define TFS_NAMEMETASERVER_NAMEMETASTOREMANAGER_H_
#include "Memory.hpp"
#include "common/meta_server_define.h"

#include "mysql_database_helper.h"
#include "database_pool.h"
#include "meta_cache_info.h"

namespace tfs
{
  namespace namemetaserver
  {
    class MetaStoreManager
    {
      public:
        MetaStoreManager();
        ~MetaStoreManager();
        //TODO int init(const int32_t pool_size, const int32_t cache_size, const int32_t mutex_count); //cache_size M
        int init(const int32_t pool_size); //will replace 

        tbsys::CThreadMutex* get_mutex(const int64_t app_id, const int64_t uid);

        int select(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
            const char* name, const bool is_file, void*& ret_node);

        int get_file_frag_info(const int64_t app_id, const int64_t uid, 
            CacheDirMetaNode* p_dir_node, CacheFileMetaNode* file_node, 
            const int64_t offset, std::vector<common::MetaInfo>& out_v_meta_info,
            int32_t& cluster_id, int64_t& last_offset);

        int ls(const int64_t app_id, const int64_t uid, CacheDirMetaNode* p_dir_node,
            const char* name, const bool is_file,
            std::vector<common::MetaInfo>& out_v_meta_info, bool& still_have);

        int insert(const int64_t app_id, const int64_t uid,
            CacheDirMetaNode* p_p_dir_node, CacheDirMetaNode* p_dir_node,
            const char* name, const common::FileType type, common::MetaInfo* meta_info = NULL);

        int update(const int64_t app_id, const int64_t uid,
            CacheDirMetaNode* s_p_p_dir_node, CacheDirMetaNode* s_p_dir_node, 
            CacheDirMetaNode* d_p_p_dir_node, CacheDirMetaNode* d_p_dir_node, 
            const char* s_name, const char* d_name,
            const common::FileType type);

        int remove(const int64_t app_id, const int64_t uid, 
            CacheDirMetaNode* p_p_dir_node,
            CacheDirMetaNode* p_dir_node,
            const char* name, const common::FileType type);
        //TODO private:
      public:
        int select(const int64_t app_id, const int64_t uid,
            const int64_t pid, const char* name, const int32_t name_len, const bool is_file,
            std::vector<common::MetaInfo>& out_v_meta_info);

        int ls(const int64_t app_id, const int64_t uid, const int64_t pid, const char* name,
            const int32_t name_len, const bool is_file,
            std::vector<common::MetaInfo>& out_v_meta_info, bool& still_have);

        int insert(const int64_t app_id, const int64_t uid,
            const int64_t ppid, const char* pname, const int32_t pname_len,
            const int64_t pid, const char* name, const int32_t name_len,
            const common::FileType type, common::MetaInfo* meta_info = NULL);

        int update(const int64_t app_id, const int64_t uid,
            const int64_t s_ppid, const int64_t s_pid, const char* s_pname, const int32_t s_pname_len,
            const int64_t d_ppid, const int64_t d_pid, const char* d_pname, const int32_t d_pname_len,
            const char* s_name, const int32_t s_name_len,
            const char* d_name, const int32_t d_name_len,
            const common::FileType type);

        int remove(const int64_t app_id, const int64_t uid, const int64_t ppid,
            const char* pname, const int64_t pname_len, const int64_t pid, const int64_t id,
            const char* name, const int64_t name_len,
            const common::FileType type);
      private:
        int get_return_status(const int status, const int proc_ret);
        int force_rc(const int32_t need_size);

      private:
        DISALLOW_COPY_AND_ASSIGN(MetaStoreManager);
        DataBasePool* database_pool_;
        //TODO lrucache
        int mutex_count_;
        tbsys::CThreadMutex* app_id_uid_mutex_;
    };
  }
}

#endif
