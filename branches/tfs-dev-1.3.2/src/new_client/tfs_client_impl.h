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
#ifndef TFS_CLIENT_TFSCLIENTIMPL_H_
#define TFS_CLIENT_TFSCLIENTIMPL_H_

#include <Mutex.h>
#include <stdio.h>
#include <pthread.h>
#include "common/define.h"

namespace tfs
{
  namespace client
  {
    class tbutil::Mutex;
    class TfsFile;
    class TfsSession;
    class GcWorker;
    typedef std::map<int, TfsFile*> FILE_MAP;

    class TfsClientImpl
    {
    public:
      static TfsClientImpl* Instance()
      {
        static TfsClientImpl tfs_client_impl;
        return &tfs_client_impl;
      }

      int initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items);
      int destroy();

      int open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ...);
      int64_t read(const int fd, void* buf, const int64_t count);
      int64_t write(const int fd, const void* buf, const int64_t count);
      int64_t lseek(const int fd, const int64_t offset, const int whence);
      int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
      int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
      int fstat(const int fd, TfsFileStat* buf, const TfsStatFlag mode = NORMAL_STAT);
      int close(const int fd, char* tfs_name = NULL, const int32_t len = 0);

      // LargeFile's name will be start with L
      int unlink(const char* file_name, const char* suffix = NULL, const TfsUnlinkType action = DELETE)
      {
        return unlink(file_name, suffix, NULL, action);
      }
      int unlink(const char* file_name, const char* suffix, const char* ns_addr, const TfsUnlinkType action = DELETE);

    private:
      //int open_ex(const char* file_name, const char* suffix, const char* ns_addr,
      //            const int flags, ...);
      bool check_init();
      TfsFile* get_file(const int fd);
      int erase_file(const int fd);
      int start_gc();

    private:
      TfsClientImpl();
      DISALLOW_COPY_AND_ASSIGN(TfsClientImpl);
      ~TfsClientImpl();

      bool is_init_;
      TfsSession* default_tfs_session_;
      int fd_;
      FILE_MAP tfs_file_map_;
      tbutil::Mutex mutex_;
      GcWorker* gc_worker_;
      pthread_t gc_tid_;
    };
  }
}

#endif  // TFS_CLIENT_TFSCLIENTAPI_H_
