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
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "tfs_client_api.h"
#include "tfs_file.h"
#include "tfs_session.h"
#include "tfs_session_pool.h"
#include <string>

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace client
  {
    TfsClient::TfsClient():
      tfs_file_(new TfsFile())
    {

    }

    int TfsClient::initialize(const std::string& ns_ip_port, int32_t cache_time, int32_t cache_items)
    {
      TfsSession* session = TfsSessionPool::get_instance().get(ns_ip_port, cache_time, cache_items);
      if (session == NULL)
      {
        TBSYS_LOG(ERROR, "tfs client initialize failed, must be exit");
        return TFS_ERROR;
      }
      tfs_file_->set_session(session);
      return TFS_SUCCESS;
    }

    TfsClient::~TfsClient()
    {
      tbsys::gDelete( tfs_file_);
    }

    bool TfsClient::is_eof() const
    {
      return tfs_file_->is_eof();
    }

    int TfsClient::stat(const char *filename, const char *suffix, FileInfo *file_info)
    {
      return tfs_file_->stat(filename, suffix, file_info);
    }

    int TfsClient::save_file(const char *filename, const char *tfsname, const char *suffix)
    {
      return tfs_file_->save_file(filename, tfsname, suffix);
    }

    int TfsClient::save_file(const char* tfsname, const char* suffix, char* data, const int32_t length)
    {
      return tfs_file_->save_file(tfsname, suffix, data, length);
    }

    int TfsClient::get_file_list(const uint32_t block_id, FILE_INFO_LIST &list)
    {
      return tfs_file_->get_file_list(block_id, list);
    }

    int TfsClient::unlink(const char *filename, const char *suffix, const int32_t action)
    {
      return tfs_file_->unlink(filename, suffix, action);
    }

    int TfsClient::unlink(const uint32_t block_id, const uint64_t file_id, const int32_t action)
    {
      return tfs_file_->unlink(block_id, file_id, action);
    }

    int TfsClient::rename(const char *filename, const char *old_prefix, const char *new_prefix)
    {
      return tfs_file_->rename(filename, old_prefix, new_prefix);
    }

    int TfsClient::rename(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id)
    {
      return tfs_file_->rename(block_id, file_id, new_file_id);
    }

    const char* TfsClient::get_file_name()
    {
      return tfs_file_->get_file_name();
    }

    int TfsClient::new_filename()
    {
      return tfs_file_->new_filename();
    }

    const char* TfsClient::get_error_message()
    {
      return tfs_file_->get_error_message();
    }

    void TfsClient::set_option_flag(const int32_t flag)
    {
      tfs_file_->set_option_flag(flag);
    }

    void TfsClient::set_crc(const uint32_t crc)
    {
      tfs_file_->set_crc(crc);
    }

    int TfsClient::tfs_open(const char *file_name, const char *suffix, const int32_t mode)
    {
      return tfs_file_->tfs_open(file_name, suffix, mode);
    }

    int TfsClient::tfs_open(const uint32_t block_id, const uint64_t file_id, const int32_t mode)
    {
      return tfs_file_->tfs_open(block_id, file_id, mode);
    }

    int TfsClient::tfs_read(char *data, const int32_t len)
    {
      return tfs_file_->tfs_read(data, len);
    }

    int TfsClient::tfs_read_v2(char *data, const int32_t len, FileInfo *file_info)
    {
      return tfs_file_->tfs_read_v2(data, len, file_info);
    }

    int TfsClient::tfs_read_scale_image(char *data, const int32_t len, const int32_t width, const int32_t height,
        FileInfo *file_info)
    {
      return tfs_file_->tfs_read_scale_image(data, len, width, height, file_info);
    }

    int64_t TfsClient::tfs_lseek(const int64_t offset, const int32_t whence)
    {
      return tfs_file_->tfs_lseek(offset, whence);
    }

    int TfsClient::tfs_write(char *data, const int32_t len)
    {
      return tfs_file_->tfs_write(data, len);
    }

    int TfsClient::tfs_stat(FileInfo *file_info, const int32_t mode)
    {
      return tfs_file_->tfs_stat(file_info, mode);
    }

    int TfsClient::tfs_close()
    {
      return tfs_file_->tfs_close();
    }

    int TfsClient::tfs_reset_read()
    {
      return tfs_file_->tfs_reset_read();
    }

    uint64_t TfsClient::get_last_elect_ds_id() const
    {
      return tfs_file_->get_last_elect_ds_id();
    }

    uint64_t TfsClient::get_ns_ip_port() const
    {
      return tfs_file_->get_ns_ip_port();
    }
    int TfsClient::create_block_info(uint32_t& block_id, VUINT64 &rds, const int32_t flag, VUINT64& fail_servers)
    {
      return tfs_file_->get_session()->create_block_info(block_id, rds, flag, fail_servers);
    }

    int TfsClient::get_block_info(uint32_t& block_id, VUINT64 &rds)
    {
      return tfs_file_->get_session()->get_block_info(block_id, rds);
    }

    int TfsClient::get_unlink_block_info(uint32_t& block_id, VUINT64 &rds)
    {
      return tfs_file_->get_session()->get_unlink_block_info(block_id, rds);
    }

    uint32_t TfsClient::crc(uint32_t crc, const char* data, const int32_t len)
    {
      return Func::crc(crc, data, len);
    }
  }
}
