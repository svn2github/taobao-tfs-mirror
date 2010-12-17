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

//#if __GNUC_PREREQ (4,3)
#define __va_arg_pack() __builtin_va_arg_pack ()
#define __va_arg_pack_len() __builtin_va_arg_pack_len ()
//#endif

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

      inline int open(const char* file_name, const char* suffix, const int flags, ... )
      {
        return open(file_name, suffix, (const char*)NULL, flags, __va_arg_pack());
      }

      inline int open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ... )
      {
        // just run time check
        if (__va_arg_pack_len() > 1)
        {
          TBSYS_LOG(ERROR, "tfs_open can be called with either 3 or 4 or 5 arguments, no more permitted"); // __errordecl ?
          return common::EXIT_INVALIDFD_ERROR;
        }

        if (flags & common::T_LARGE)
        {
          if (__va_arg_pack_len() != 1)
          {
            TBSYS_LOG(ERROR, "tfs_open with O_LARGE flag needs additional argument"); // __errordecl ?
            return common::EXIT_INVALIDFD_ERROR;
          }
        }
        else if (__va_arg_pack_len() > 0)
        {
          TBSYS_LOG(ERROR, "tfs_open without O_LARGE need no additional argument\n"); // __errordecl ?
          return common::EXIT_INVALIDFD_ERROR;
        }

        return open_ex(file_name, suffix, ns_addr, flags, __va_arg_pack_len(), __va_arg_pack());
      }

      ssize_t read(int fd, void* buf, size_t count);
      ssize_t write(int fd, const void* buf, size_t count);
      off_t lseek(int fd, off_t offset, int whence);
      ssize_t pread(int fd, void* buf, size_t count, off_t offset);
      ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset);
      int fstat(int fd, common::FileInfo* buf, int mode = common::NORMAL_STAT);
      int close(int fd);
      const char* get_file_name(int fd);

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
