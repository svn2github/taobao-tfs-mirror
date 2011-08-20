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
 *      chuyu(chuyu@taobao.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_META_CLIENT_API_IMPL_H_
#define TFS_CLIENT_META_CLIENT_API_IMPL_H_

#include <stdio.h>
#include <tbsys.h>
#include <Timer.h>
#include "common/define.h"
#include "common/meta_server_define.h"
#include "tfs_meta_client_api.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    const int64_t MAX_BATCH_DATA_LENGTH = 1 << 23; 
    const int64_t MAX_SEGMENT_LENGTH = 1 << 21;
    class NameMetaClientImpl
    {
      public:
        NameMetaClientImpl();
        ~NameMetaClientImpl();

        inline void set_app_id(const int64_t app_id)
        {
          app_id_ = app_id;
        }
        inline int64_t get_app_id() const
        {
          return app_id_;
        }

        inline void set_user_id(const int64_t user_id)
        {
          user_id_ = user_id;
        }
        inline int64_t get_user_id() const
        {
          return user_id_;
        }

        void set_meta_servers(const char* meta_server_str);
        void set_server(const char* server_ip_port);
        void set_server_id(const uint64_t server_id);
        uint64_t get_server_id() const;

        TfsRetType create_dir(const char* dir_path);
        TfsRetType create_file(const char* file_path);

        TfsRetType rm_dir(const char* dir_path);
        TfsRetType rm_file(const char* file_path);

        TfsRetType mv_dir(const char* src_dir_path, const char* dest_dir_path);
        TfsRetType mv_file(const char* src_file_path, const char* dest_file_path);

        TfsRetType ls_dir(const char* dir_path, const int64_t pid, std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive = false);
        TfsRetType ls_file(const char* file_path, common::FileMetaInfo& frag_info);

        int64_t read(const char* file_path, const int64_t offset, const int64_t length, void* buffer);
        int64_t write(const char* file_path, const int64_t offset, void* buffer, const int64_t pos, const int64_t length);

      private:
        DISALLOW_COPY_AND_ASSIGN(NameMetaClientImpl);

      private:
        bool is_valid_file_path(const char* file_path);
        TfsRetType do_file_action(common::MetaActionOp action, const char* path, const char* new_path = NULL);
        int read_frag_info(const char* file_path, common::FragInfo& frag_info);
        int unlink_file(common::FragInfo& frag_info);
        int do_ls_ex(const char* file_path, const common::FileType file_type, const int64_t pid, 
            std::vector<common::FileMetaInfo>& v_file_meta_info);
        int64_t read_data(common::FragInfo& frag_info, void* buffer, int64_t pos, int64_t length);
        int64_t write_data(int32_t cluster_id, void* buffer, int64_t pos, int64_t length, 
            common::FragInfo& frag_info);
        int do_read(const char* path, const int64_t offset, const int64_t size, 
            common::FragInfo& frag_info, bool& still_have);
        int32_t get_cluster_id(const char* path);
        int write_meta_info(const char* path, common::FragInfo& frag_info);

      private:
        int64_t app_id_;
        int64_t user_id_;
        char ns_server_ip_[64];
        std::vector<uint64_t> v_meta_server_;
    };
  }
}

#endif
