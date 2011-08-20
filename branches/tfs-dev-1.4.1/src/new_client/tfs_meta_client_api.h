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
#ifndef TFS_CLIENT_META_CLIENT_API_H_
#define TFS_CLIENT_META_CLIENT_API_H_

#include <string>
#include "common/define.h"
#include "common/stream.h"
#include "common/meta_server_define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class NameMetaClientImpl;
    class NameMetaClient
    {
      public:
        NameMetaClient();
        ~NameMetaClient();

        void set_app_id(const int64_t app_id);
        int64_t get_app_id() const;

        void set_user_id(const int64_t user_id);
        int64_t get_user_id() const;

        void set_meta_servers(const char* meta_server_str);
        void set_server(const char* server_ip_port);

        TfsRetType create_dir(const char* dir_path);
        TfsRetType create_file(const char* file_path);

        TfsRetType rm_dir(const char* dir_path);
        TfsRetType rm_file(const char* file_path);

        TfsRetType mv_dir(const char* src_dir_path, const char* dest_dir_path);
        TfsRetType mv_file(const char* src_file_path, const char* dest_file_path);

        TfsRetType ls_dir(const char* dir_path, int64_t pid, std::vector<common::FileMetaInfo>& v_file_meta_info, bool is_recursive = false);
        TfsRetType ls_file(const char* file_path, common::FileMetaInfo& file_meta_info);

        int read(const char* file_path, const int64_t offset, const int64_t length, void* buffer);
        int write(const char* file_path, const int64_t offset, void* buffer, const int64_t pos, const int64_t length);

      private:
        NameMetaClient(const NameMetaClient&);
        NameMetaClientImpl* impl_;

    };
  }
}

#endif
