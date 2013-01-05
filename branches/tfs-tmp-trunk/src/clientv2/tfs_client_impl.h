/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_impl.h 868 2011-09-29 05:07:38Z duanfei@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_TFSCLIENTIMPL_H_
#define TFS_CLIENTV2_TFSCLIENTIMPL_H_

#include <Mutex.h>
#include <stdio.h>
#include <pthread.h>

#include "common/internal.h"

namespace tfs
{
  namespace common
  {
    class BasePacketFactory;
    class BasePacketStreamer;
  }
  namespace clientv2
  {
    class tbutil::Mutex;
    class TfsFile;
    typedef std::map<int, TfsFile*> FILE_MAP;

    class TfsClientImpl
    {
    public:
      static TfsClientImpl* Instance()
      {
        static TfsClientImpl tfs_client_impl;
        return &tfs_client_impl;
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
      int get_fd();
      TfsFile* get_file(const int fd);
      int insert_file(const int fd, TfsFile* tfs_file);
      int erase_file(const int fd);
      int initialize_cluster_id();

   private:
      TfsClientImpl();
      DISALLOW_COPY_AND_ASSIGN(TfsClientImpl);
      ~TfsClientImpl();

      bool is_init_;
      int fd_;
      uint64_t ns_addr_;
      int32_t cluster_id_;
      FILE_MAP tfs_file_map_;
      tbutil::Mutex mutex_;
      common::BasePacketFactory* packet_factory_;
      common::BasePacketStreamer* packet_streamer_;
    };
  }
}

#endif  // TFS_CLIENT_TFSCLIENTAPI_H_
