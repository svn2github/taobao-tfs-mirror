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
 *    daoan<aoan@taobao.com>
 *      - initial release
 *
 */
#include "common/define.h"
#include "tfs_meta_client_api.h"
#include "tfs_meta_client_api_impl.h"

namespace tfs
{
  namespace client
  {
    using namespace tfs::common;
    using namespace std;

    NameMetaClient::NameMetaClient():impl_(NULL)
    {
      impl_ = new NameMetaClientImpl();
    }

    NameMetaClient::~NameMetaClient()
    {
      delete impl_;
      impl_ = NULL;
    }
    void NameMetaClient::set_app_id(const int64_t app_id)
    {
      impl_->set_app_id(app_id);
    }
    int64_t NameMetaClient::get_app_id() const
    {
      return impl_->get_app_id();
    }

    void NameMetaClient::set_user_id(const int64_t user_id)
    {
      impl_->set_user_id(user_id);
    }
    int64_t NameMetaClient::get_user_id() const
    {
      return impl_->get_user_id();
    }

    void NameMetaClient::set_meta_servers(const char* meta_server_str)
    {
      impl_->set_meta_servers(meta_server_str);
    }

    void NameMetaClient::set_server(const char* server_ip_port)
    {
      impl_->set_server(server_ip_port);
    }

    TfsRetType NameMetaClient::create_dir(const char* dir_path)
    {
      return impl_->create_dir(dir_path);
    }

    TfsRetType NameMetaClient::create_file(const char* file_path)
    {
      return impl_->create_file(file_path);
    }

    TfsRetType NameMetaClient::rm_dir(const char* dir_path)
    {
      return impl_->rm_dir(dir_path);
    }

    TfsRetType NameMetaClient::rm_file(const char* file_path)
    {
      return impl_->rm_file(file_path);
    }

    TfsRetType NameMetaClient::mv_dir(const char* src_dir_path, const char* dest_dir_path)
    {
      return impl_->mv_dir(src_dir_path, dest_dir_path);
    }

    TfsRetType NameMetaClient::mv_file(const char* src_file_path, const char* dest_file_path)
    {
      return impl_->mv_file(src_file_path, dest_file_path);
    }

    TfsRetType NameMetaClient::ls_dir(const char* dir_path, int64_t pid, std::vector<FileMetaInfo>& v_file_meta_info,
        bool is_recursive)
    {
      return impl_->ls_dir(dir_path, pid, v_file_meta_info, is_recursive);
    }

    TfsRetType NameMetaClient::ls_file(const char* file_path, FileMetaInfo& file_meta_info)
    {
      return impl_->ls_file(file_path, file_meta_info);
    }

    int NameMetaClient::read(const char* file_path, int64_t offset, int64_t length, void* buffer)
    {
      return impl_->read(file_path, offset, length, buffer);
    }

    int NameMetaClient::write(const char* file_path, int64_t offset, void* buffer, int64_t pos, int64_t length)
    {
      return impl_->write(file_path, offset, buffer, pos, length);
    }

    //int64_t NameMetaClient::save_file(const char* source_data, const int32_t data_len,
    //    char* tfs_name_buff, const int32_t buff_len)
    //{
    //  return impl_->save_file(source_data, data_len, tfs_name_buff, buff_len);
    //}

  }
}
