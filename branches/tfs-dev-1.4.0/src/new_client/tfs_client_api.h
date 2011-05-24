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
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSCLIENTAPI_H_
#define TFS_CLIENT_TFSCLIENTAPI_H_

#include <stdio.h>
#include "common/define.h"
#include "common/client_define.h"

// #ifdef __OPTIMIZE__
// extern int error_open_missing_mode (void)
//     __attribute__((__error__ ("open with (T_LARGE & T_WRITE) flag needs 1 additional not-NULL argument")));
// extern int error_no_addition_mode (void)
//     __attribute__((__error__ ("open without (T_LARGE & T_WRITE) flag needs no more additional argument")));
// extern int error_open_too_many_arguments (void)
//     __attribute__((__error__ ("open can be called with either 3 or 4 or 5 arguments, no more permitted")));
// #define missing_log_error() error_open_missing_mode()
// #define noadditional_log_error() error_no_addition_mode()
// #define overmany_log_error() error_open_too_many_arguments()
// #else
#define missing_log_error() fprintf(stderr, "%s\n", "open with (T_LARGE & T_WRITE) flag needs 1 additional not-NULL argument")
#define noadditional_log_error() fprintf(stderr, "%s\n", "open without (T_LARGE & T_WRITE) flag needs no more additional argument")
#define overmany_log_error() fprintf(stderr, "%s\n", "open can be called with either 3 or 4 or 5 arguments, no more permitted")
// #endif

namespace tfs
{
  namespace client
  {
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
      int destroy();

      __always_inline __attribute__ ((__gnu_inline__)) int
        open(const char* file_name, const char* suffix, const int flags, ... )
      {
        int ret = common::EXIT_INVALIDFD_ERROR;
        if (__builtin_va_arg_pack_len() > 1)
        {
          overmany_log_error();
        }
        else if (flags & common::T_WRITE && flags & common::T_LARGE)
        {
          if (__builtin_va_arg_pack_len() != 1)
          {
            missing_log_error();
          }
          else
          {
            ret = open_ex_with_arg(file_name, suffix, (const char*)NULL, flags, __builtin_va_arg_pack());
          }
        }
        else
        {
          if (__builtin_va_arg_pack_len() > 0)
          {
            noadditional_log_error();
          }
          else
          {
            ret = open_ex(file_name, suffix, (const char*)NULL, flags);
          }
        }

        return ret;
      }

      __always_inline __attribute__ ((__gnu_inline__)) int
        open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ... )
      {
        int ret = common::EXIT_INVALIDFD_ERROR;
        if (__builtin_va_arg_pack_len() > 1)
        {
          overmany_log_error();
        }
        else if (flags & common::T_WRITE && flags & common::T_LARGE)
        {
          if (__builtin_va_arg_pack_len() != 1)
          {
            missing_log_error();
          }
          else
          {
            ret = open_ex_with_arg(file_name, suffix, ns_addr, flags, __builtin_va_arg_pack());
          }
        }
        else
        {
          if (__builtin_va_arg_pack_len() > 0)
          {
            noadditional_log_error();
          }
          else
          {
            ret = open_ex(file_name, suffix, ns_addr, flags);
          }
        }

        return ret;
      }

      int64_t read(const int fd, void* buf, const int64_t count);
      int64_t write(const int fd, const void* buf, const int64_t count);
      int64_t lseek(const int fd, const int64_t offset, const int whence);
      int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
      int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
      int fstat(const int fd, TfsFileStat* buf, const TfsStatFlag mode = NORMAL_STAT);
      int close(const int fd, char* tfs_name = NULL, const int32_t len = 0);
      int64_t get_file_length(const int fd);

      // LargeFile's name will be start with L
      int unlink(const char* file_name, const char* suffix = NULL, const TfsUnlinkType action = DELETE)
      {
        return unlink(file_name, suffix, NULL, action);
      }
      int unlink(const char* file_name, const char* suffix, const char* ns_addr, const TfsUnlinkType action = DELETE);

      void set_segment_size(const int64_t segment_size);
      int64_t get_segment_size();

      void set_batch_count(const int64_t batch_count);
      int64_t get_batch_count();

      void set_gc_interval(const int64_t gc_interval_s);
      int64_t get_gc_interval();

      void set_gc_expired_time(const int64_t gc_expired_time_s);
      int64_t get_gc_expired_time();

      void set_batch_time_out(const int64_t time_out_us);
      int64_t get_batch_time_out();

      void set_log_level(const char* level);

    private:
      TfsClient();
      DISALLOW_COPY_AND_ASSIGN(TfsClient);
      ~TfsClient();
      int open_ex(const char* file_name, const char* suffix, const char* ns_addr, const int flags);
      int open_ex_with_arg(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ...);
    };
  }
}

#endif  // TFS_CLIENT_TFSCLIENTAPI_H_
