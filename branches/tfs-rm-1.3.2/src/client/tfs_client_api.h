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
#ifndef TFS_CLIENT_API_H_
#define TFS_CLIENT_API_H_

#include <string>
#include "common/define.h"

namespace tfs
{
  namespace client
  {
    enum
    {
      TFS_SEEK_SET = 0,
      TFS_SEEK_CUR
    };
    class TfsFile;
    class TfsClient
    {
    public:
      TfsClient();
      virtual ~TfsClient();
      int initialize(const std::string& ns_ip_port, int32_t cache_time = common::DEFAULT_BLOCK_CACHE_TIME,
          int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS);
      bool is_eof() const;
      int stat(const char *filename, const char *suffix, common::FileInfo *file_info);
      int save_file(const char *filename, const char *tfsname, const char *suffix);
      int save_file(const char* tfsname, const char* suffix, char* data, const int32_t length);
      int get_file_list(const uint32_t block_id, common::FILE_INFO_LIST &list);
      int unlink(const char *filename, const char *suffix = NULL, const int32_t action = 0);
      int unlink(const uint32_t block_id, const uint64_t file_id, const int32_t action);
      int rename(const char *filename, const char *old_prefix, const char *new_prefix);
      int rename(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id);
      const char *get_file_name();
      int new_filename();
      const char *get_error_message();
      void set_option_flag(const int32_t flag);
      void set_crc(const uint32_t crc);

      int tfs_open(const char *file_name, const char *suffix, const int32_t mode);
      int tfs_open(const uint32_t block_id, const uint64_t file_id, const int32_t mode);
      int tfs_read(char *data, const int32_t len);
      int tfs_read_v2(char *data, const int32_t len, common::FileInfo *file_info);
      int tfs_read_scale_image(char *data, const int32_t len, const int32_t width, const int32_t height,
          common::FileInfo *file_info);
      int64_t tfs_lseek(const int64_t offset, const int32_t whence);
      int tfs_write(const char *data, const int32_t len);
      int tfs_stat(common::FileInfo *file_info, const int32_t mode = 0);
      int tfs_close();
      int tfs_reset_read();
      uint64_t get_last_elect_ds_id() const;
      uint64_t get_ns_ip_port() const;
      int create_block_info(uint32_t& block_id, common::VUINT64 &rds, const int32_t flag, common::VUINT64& fail_servers);
      int get_block_info(uint32_t& block_id, common::VUINT64 &rds);
      int get_unlink_block_info(uint32_t& block_id, common::VUINT64 &rds);

			static uint32_t crc(uint32_t crc, const char* data, const int32_t len);
    private:
      DISALLOW_COPY_AND_ASSIGN( TfsClient);
      TfsFile* tfs_file_;
    };
  }
}

#endif /* TFS_CLIENT_API_H_ */
