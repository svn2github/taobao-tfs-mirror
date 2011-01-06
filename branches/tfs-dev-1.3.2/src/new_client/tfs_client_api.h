/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.h 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSCLIENTAPI_H_
#define TFS_CLIENT_TFSCLIENTAPI_H_

#include <string>
#include <Mutex.h>
#include "common/define.h"
#include "message/message_factory.h"
#include "message/tfs_packet_streamer.h"

#include <stdio.h>

#ifdef __OPTIMIZE__
extern int error_open_missing_mode (void)
    __attribute__((__error__ ("open with (T_LARGE & T_WRITE) flag needs 1 additional argument")));
extern int error_no_addition_mode (void)
    __attribute__((__error__ ("open without (T_LARGE & T_WRITE) flag needs no more additional argument")));
extern int error_open_too_many_arguments (void)
    __attribute__((__error__ ("open can be called with either 3 or 4 or 5 arguments, no more permitted")));
#define missing_log_error() error_open_missing_mode()
#define noadditional_log_error() error_no_addition_mode()
#define overmany_log_error() warn_open_too_many_arguments()
#else
#define missing_log_error() TBSYS_LOG(ERROR, "open with (T_LARGE & T_WRITE) flag needs 1 additional argument")
#define noadditional_log_error() TBSYS_LOG(ERROR, "open without (T_LARGE & T_WRITE) flag needs no more additional argument")
#define overmany_log_error() TBSYS_LOG(ERROR, "open can be called with either 3 or 4 or 5 arguments, no more permitted")
#endif

namespace tfs
{
  namespace client
  {
    class tbutil::Mutex;
    class TfsFile;
    class TfsSession;
    typedef std::map<int, TfsFile*> FILE_MAP;

    class TfsClient
    {
    public:
      static TfsClient* Instance()
      {
        static TfsClient tfs_client;
        return &tfs_client;
      }

      int initialize(const char* ns_addr, const int32_t cache_time = common::DEFAULT_BLOCK_CACHE_TIME,
                     const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS);

      __always_inline __attribute__ ((__gnu_inline__)) int
        open(const char* file_name, const char* suffix, const int flags, ... )
      {
        if (__builtin_va_arg_pack_len() > 1)
        {
          overmany_log_error();
          return common::EXIT_INVALIDFD_ERROR;
        }

        if (flags & common::T_WRITE && flags & common::T_LARGE)
        {
          if (__builtin_va_arg_pack_len() != 1)
          {
            missing_log_error();
            return common::EXIT_INVALIDFD_ERROR;
          }
        }
        else if (__builtin_va_arg_pack_len() > 0)
        {
          noadditional_log_error();
          return common::EXIT_INVALIDFD_ERROR;
        }

        return open_ex(file_name, suffix, (const char*)NULL, flags, __builtin_va_arg_pack());
      }

      __always_inline __attribute__ ((__gnu_inline__)) int
        open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ... )
      {
        if (__builtin_va_arg_pack_len() > 1)
        {
          overmany_log_error();
          return common::EXIT_INVALIDFD_ERROR;
        }

        if (flags & common::T_WRITE && flags & common::T_LARGE)
        {
          if (__builtin_va_arg_pack_len() != 1)
          {
            missing_log_error();
            return common::EXIT_INVALIDFD_ERROR;
          }
        }
        else if (__builtin_va_arg_pack_len() > 0)
        {
          noadditional_log_error();
          return common::EXIT_INVALIDFD_ERROR;
        }
        return open_ex(file_name, suffix, ns_addr, flags, __builtin_va_arg_pack());
      }

      int64_t read(const int fd, void* buf, const int64_t count);
      int64_t write(const int fd, const void* buf, const int64_t count);
      int64_t lseek(const int fd, const int64_t offset, const int whence);
      int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
      int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
      int fstat(const int fd, common::FileInfo* buf, const int mode = common::NORMAL_STAT);
      int close(const int fd, char* tfs_name = NULL, const int32_t len = 0);

      // LargeFile's name will be start with L
      int unlink(const char* file_name, const char* suffix = NULL, const int action = 0)
      {
        return unlink(file_name, suffix, NULL, action);
      }
      int unlink(const char* file_name, const char* suffix, const char* ns_addr, const int action = 0);

    private:
      int open_ex(const char* file_name, const char* suffix, const char* ns_addr,
             const int flags, ...);

      TfsFile* get_file(const int fd);
      int erase_file(const int fd);

    private:
      TfsClient();
      DISALLOW_COPY_AND_ASSIGN(TfsClient);
      ~TfsClient();

      bool is_init_;
      TfsSession* default_tfs_session_;
      int fd_;
      FILE_MAP tfs_file_map_;
      tbutil::Mutex mutex_;

      tbnet::Transport transport_;
      message::MessageFactory factory_;
      message::TfsPacketStreamer streamer_;
    };
  }
}

#endif  // TFS_CLIENT_TFSCLIENTAPI_H_
