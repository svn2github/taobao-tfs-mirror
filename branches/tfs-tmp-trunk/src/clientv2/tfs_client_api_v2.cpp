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

#include "tfs_client_impl_v2.h"
#include "tfs_client_api_v2.h"

namespace tfs
{
  namespace clientv2
  {
    using namespace common;

    TfsClientV2::TfsClientV2()
    {
    }

    TfsClientV2::~TfsClientV2()
    {
    }

    int TfsClientV2::initialize(const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->initialize(ns_addr);
    }

    int TfsClientV2::destroy()
    {
      return TfsClientImplV2::Instance()->destroy();
    }

    int TfsClientV2::open(const char* file_name, const char* suffix, const char* ns_addr, const int mode)
    {
      return TfsClientImplV2::Instance()->open(file_name, suffix, ns_addr, mode);
    }

    int TfsClientV2::open(const uint64_t block_id, const uint64_t file_id, const char* ns_addr, const int mode)
    {
      return TfsClientImplV2::Instance()->open(block_id, file_id, ns_addr, mode);
    }

    int TfsClientV2::set_option_flag(const int fd, const int option_flag)
    {
      return TfsClientImplV2::Instance()->set_option_flag(fd, option_flag);
    }

    int64_t TfsClientV2::read(const int fd, void* buf, const int64_t count)
    {
      return TfsClientImplV2::Instance()->read(fd, buf, count);
    }

    int64_t TfsClientV2::readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* file_info)
    {
      return TfsClientImplV2::Instance()->readv2(fd, buf, count, file_info);
    }

    int64_t TfsClientV2::write(const int fd, const void* buf, const int64_t count)
    {
      return TfsClientImplV2::Instance()->write(fd, buf, count);
    }

    int64_t TfsClientV2::lseek(const int fd, const int64_t offset, const int whence)
    {
      return TfsClientImplV2::Instance()->lseek(fd, offset, whence);
    }

    int TfsClientV2::fstat(const int fd, TfsFileStat* buf)
    {
      return TfsClientImplV2::Instance()->fstat(fd, buf);
    }

    int TfsClientV2::close(const int fd, char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      return TfsClientImplV2::Instance()->close(fd, ret_tfs_name, ret_tfs_name_len);
    }

    int TfsClientV2::unlink(int64_t& file_size, const int fd, const common::TfsUnlinkType action)
    {
      return TfsClientImplV2::Instance()->unlink(file_size, fd, action);
    }

    int TfsClientV2::stat_file(common::TfsFileStat* file_stat, const char* file_name, const char* suffix,
        const common::TfsStatType stat_type, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->stat_file(file_stat, file_name, suffix, stat_type, ns_addr);
    }

    int64_t TfsClientV2::save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* local_file, const int32_t mode, const char* suffix, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->save_file(ret_tfs_name, ret_tfs_name_len,
          local_file, mode, suffix, ns_addr);
    }

    int TfsClientV2::fetch_file(const char* local_file, const char* file_name, const char* suffix, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->fetch_file(local_file, file_name, suffix, ns_addr);
    }

    int TfsClientV2::unlink(int64_t& file_size, const char* file_name, const char* suffix,
        const common::TfsUnlinkType action, const common::OptionFlag option_flag, const char* ns_addr)
    {
      return TfsClientImplV2::Instance()->unlink(file_size, file_name, suffix, action, option_flag, ns_addr);
    }

    int64_t TfsClientV2::get_server_id()
    {
      return TfsClientImplV2::Instance()->get_server_id();
    }

    int32_t TfsClientV2::get_cluster_id()
    {
      return TfsClientImplV2::Instance()->get_cluster_id();
    }
  }
}
