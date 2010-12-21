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

#define __builtin_va_arg_pack() __builtin_va_arg_pack()
#define __builtin_va_arg_pack_len() __builtin_va_arg_pack_len()

#include <string>

#include "common/define.h"
#include "Mutex.h"

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
      static TfsClient* Instance();

      int initialize(const char* ns_addr, const int32_t cache_time = common::DEFAULT_BLOCK_CACHE_TIME,
                     const int32_t cache_items = common::DEFAULT_BLOCK_CACHE_ITEMS);

      inline int __attribute__ ((__gnu_inline__)) 
        open(const char* file_name, const char* suffix, const int flags, ... )
      {
        return open(file_name, suffix, (const char*)NULL, flags, __builtin_va_arg_pack());
      }

      inline int __attribute__ ((__gnu_inline__)) 
        open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ... )
      {
        // just run time check
        if (__builtin_va_arg_pack_len() > 1)
        {
          TBSYS_LOG(ERROR, "tfs_open can be called with either 3 or 4 or 5 arguments, no more permitted"); // __errordecl ?
          return common::EXIT_INVALIDFD_ERROR;
        }

        if (flags & common::T_LARGE)
        {
          if (__builtin_va_arg_pack_len() != 1)
          {
            TBSYS_LOG(ERROR, "tfs_open with O_LARGE flag needs additional argument"); // __errordecl ?
            return common::EXIT_INVALIDFD_ERROR;
          }
        }
        else if (__builtin_va_arg_pack_len() > 0)
        {
          TBSYS_LOG(ERROR, "tfs_open without O_LARGE need no additional argument\n"); // __errordecl ?
          return common::EXIT_INVALIDFD_ERROR;
        }

        return open_ex(file_name, suffix, ns_addr, flags, __builtin_va_arg_pack_len(), __builtin_va_arg_pack());
      }

      int32_t read(const int fd, void* buf, const int32_t count);
      int32_t write(const int fd, const void* buf, const int32_t count);
      int64_t lseek(const int fd, const int64_t offset, const int whence);
      int32_t pread(const int fd, void* buf, const int32_t count, const int64_t offset);
      int32_t pwrite(const int fd, const void* buf, const int32_t count, const int64_t offset);
      int fstat(const int fd, common::FileInfo* buf, const int mode = common::NORMAL_STAT);
      int close(const int fd, char* tfs_name, const int32_t len);

    private:
      int open_ex(const char* file_name, const char* suffix, const char* ns_addr,
             const int flags, const int32_t arg_cnt, ... );

      TfsFile* get_file(int fd);
      int erase_file(int fd);

    private:
      TfsClient();
      DISALLOW_COPY_AND_ASSIGN(TfsClient);
      ~TfsClient();

      TfsSession* default_tfs_session_;
      int fd_;
      FILE_MAP tfs_file_map_;
      tbutil::Mutex mutex_;
    };
  }
}

#endif  // TFS_CLIENT_TFSCLIENTAPI_H_
