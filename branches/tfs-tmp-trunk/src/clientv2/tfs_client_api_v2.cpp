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

#include "tfs_client_impl.h"
#include "tfs_client_api_v2.h"

namespace tfs
{
  namespace clientv2
  {
    using namespace common;

    TfsClient::TfsClient()
    {
    }

    TfsClient::~TfsClient()
    {
    }

    int TfsClient::initialize(const char* ns_addr)
    {
      return TfsClientImpl::Instance()->initialize(ns_addr);
    }

    int TfsClient::destroy()
    {
      return TfsClientImpl::Instance()->destroy();
    }

    int TfsClient::open(const char* file_name, const char* suffix, const int mode)
    {
      return TfsClientImpl::Instance()->open(file_name, suffix, mode);
    }

    int TfsClient::set_option_flag(const int fd, const int option_flag)
    {
      return TfsClientImpl::Instance()->set_option_flag(fd, option_flag);
    }

    int64_t TfsClient::read(const int fd, void* buf, const int64_t count)
    {
      return TfsClientImpl::Instance()->read(fd, buf, count);
    }

    int64_t TfsClient::write(const int fd, const void* buf, const int64_t count)
    {
      return TfsClientImpl::Instance()->write(fd, buf, count);
    }

    int64_t TfsClient::lseek(const int fd, const int64_t offset, const int whence)
    {
      return TfsClientImpl::Instance()->lseek(fd, offset, whence);
    }

    int TfsClient::fstat(const int fd, TfsFileStat* buf)
    {
      return TfsClientImpl::Instance()->fstat(fd, buf);
    }

    int TfsClient::close(const int fd, char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      return TfsClientImpl::Instance()->close(fd, ret_tfs_name, ret_tfs_name_len);
    }

    int TfsClient::unlink(int64_t& file_size, const int fd, const common::TfsUnlinkType action)
    {
      return TfsClientImpl::Instance()->unlink(file_size, fd, action);
    }

  }
}
