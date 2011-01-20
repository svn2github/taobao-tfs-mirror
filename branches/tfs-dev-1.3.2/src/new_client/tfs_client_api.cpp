/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include "tfs_client_api.h"
#include "tfs_client_impl.h"

using namespace tfs::common;
using namespace tfs::client;
using namespace std;

TfsClient::TfsClient()
{
}

TfsClient::~TfsClient()
{
}

int TfsClient::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  return TfsClientImpl::Instance()->initialize(ns_addr, cache_items, cache_items);
}

int destory()
{
  return TfsClientImpl::Instance()->destory();
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

int64_t TfsClient::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
{
  return TfsClientImpl::Instance()->pread(fd, buf, count, offset);
}

int64_t TfsClient::pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
{
  return TfsClientImpl::Instance()->pwrite(fd, buf, count, offset);
}

int TfsClient::fstat(const int fd, TfsFileStat* buf, const TfsStatFlag mode)
{
  return TfsClientImpl::Instance()->fstat(fd, buf, mode);
}

int TfsClient::close(const int fd, char* tfs_name, const int32_t len)
{
  return TfsClientImpl::Instance()->close(fd, tfs_name, len);
}

int TfsClient::unlink(const char* file_name, const char* suffix, const char* ns_addr, const TfsUnlinkType action)
{
  return TfsClientImpl::Instance()->unlink(file_name, suffix, ns_addr, action);
}

int TfsClient::open_ex(const char* file_name, const char* suffix, const char* ns_addr, const int flags)
{
  return TfsClientImpl::Instance()->open(file_name, suffix, ns_addr, flags);
}

int TfsClient::open_ex_with_arg(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ...)
{
  int ret = EXIT_INVALIDFD_ERROR;
  va_list args;
  va_start(args, flags);
  ret = TfsClientImpl::Instance()->open(file_name, suffix, ns_addr, flags, va_arg(args, char*));
  va_end(args);
  return ret;
}
