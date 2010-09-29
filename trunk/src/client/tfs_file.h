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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSFILE_H_
#define TFS_CLIENT_TFSFILE_H_

#include<Memory.hpp>
#include "message/client.h"
#include "tfs_session.h"
#include "tfs_client_api.h"

namespace tfs
{
  namespace client
  {
    enum TfsFileOpenFlag
    {
      TFS_FILE_OPEN_FLAG_NO = 0x00,
      TFS_FILE_OPEN_FLAG_YES,
    };
    enum TfsFileEofFlag
    {
      TFS_FILE_EOF_FLAG_NO = 0x00,
      TFS_FILE_EOF_FLAG_YES
    };

    class TfsFile
    {
    public:
      TfsFile();
      virtual ~TfsFile();

      inline const char *get_error_message()
      {
        return error_message_;
      }

      inline void set_option_flag(const int32_t flag)
      {
        option_flag_ = flag;
      }

      inline void set_crc(const uint32_t crc)
      {
        crc_ = crc;
      }

      inline bool is_eof() const
      {
        return eof_ == TFS_FILE_EOF_FLAG_YES;
      }
      inline uint64_t get_last_elect_ds_id() const
      {
        return last_elect_ds_id_;
      }

      inline uint64_t get_ns_ip_port() const
      {
        return session_->get_ns_ip_port();
      }
      inline TfsSession* get_session()
      {
        return session_;
      }

      void set_session(TfsSession *session);
      int stat(const char *filename, const char *suffix, common::FileInfo *file_info);
      int save_file(const char *filename, const char *tfsname, const char *suffix);
      int save_file(const char* tfsname, const char* suffix, char* data, const int32_t length);
      int get_file_list(const uint32_t block_id, common::FILE_INFO_LIST &l);
      int unlink(const char *filename, const char *suffix = NULL, const int32_t action = 0);
      int unlink(const uint32_t block_id, const uint64_t file_id, const int32_t action);
      int rename(const char *filename, const char *old_prefix, const char *new_prefix);
      int rename(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id);
      const char *get_file_name();
      int new_filename();

      //tfs interface
      int tfs_open(const char *file_name, const char *suffix, const int32_t mode);
      int tfs_open(const uint32_t block_id, const uint64_t file_id, const int32_t mode);
      int tfs_read(char *data, const int32_t len);
      int tfs_read_v2(char *data, const int32_t len, common::FileInfo *file_info);
      int tfs_read_scale_image(char *data, const int32_t len, const int32_t width, const int32_t height,
          common::FileInfo *file_info);
      int64_t tfs_lseek(const int64_t offset, const int32_t whence);
      int tfs_write(char *data, const int32_t len);
      int tfs_stat(common::FileInfo *file_info, const int32_t mode = 0);
      int tfs_close();
      int tfs_reset_read();

    private:
      DISALLOW_COPY_AND_ASSIGN( TfsFile);
      int connect_next_ds();
      int connect_ds();
      int create_filename();
      void conv_name(const char *fileName, const char *suffix);
      int get_file_crc_size(const char *filename, uint32_t& crc, int32_t& size);

    private:
      common::VUINT64 ds_list_;
      common::VUINT64 fail_servers_;
      TfsSession *session_;
      message::Client *client_;
      char error_message_[common::ERR_MSG_SIZE];
      char file_name_[common::TFS_FILE_LEN];
      uint64_t file_number_;
      uint64_t file_id_;
      uint64_t last_elect_ds_id_;
      uint32_t block_id_;
      uint32_t crc_;
      int32_t mode_;
      int32_t offset_;
      int32_t pri_ds_index_;
      int32_t is_open_flag_;
      int32_t option_flag_;
      TfsFileEofFlag eof_;
#ifdef __CLIENT_METRICS__
      static ClientMetrics read_metrics_;
      static ClientMetrics write_metrics_;
      static ClientMetrics close_metrics_;
#endif
    };
  }
}
#endif
