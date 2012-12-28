/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_TFSCLIENTAPI_H_
#define TFS_CLIENTV2_TFSCLIENTAPI_H_

#include <stdio.h>
#include <pthread.h>

#include "common/define.h"

namespace tfs
{
  namespace clientv2
  {
    class TfsClient
    {
    public:
      static TfsClient* Instance()
      {
        static TfsClient tfs_client_;
        return &tfs_client_;
      }

      int initialize(const char* ns_addr);
      int destroy();
      int set_option_flag(const int fd, const int opt_flag);

      int open(const char* file_name, const char* suffix, const int mode);
      int64_t read(const int fd, void* buf, const int64_t count);
      int64_t readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* file_info);
      int64_t write(const int fd, const void* buf, const int64_t count);
      int64_t lseek(const int fd, const int64_t offset, const int whence);
      int64_t pread(const int fd, void* buf, const int64_t count, const int64_t offset);
      int64_t pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset);
      int fstat(const int fd, common::TfsFileStat* buf);
      int close(const int fd, char* ret_tfs_name = NULL, const int32_t ret_tfs_name_len = 0);
      int unlink(int64_t& file_size, const int fd, const common::TfsUnlinkType action = common::DELETE);

    private:
      TfsClient();
      DISALLOW_COPY_AND_ASSIGN(TfsClient);
      ~TfsClient();
    };
  }
}

#endif  // TFS_CLIENTV2_TFSCLIENTAPI_H_
