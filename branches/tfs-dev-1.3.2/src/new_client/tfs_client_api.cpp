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
#include <stdarg.h>
#include <string>

#include "Memory.hpp"

#include "tfs_client_api.h"
#include "tfs_session_pool.h"
#include "tfs_large_file.h"
#include "tfs_small_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace std;

TfsClient::TfsClient() : default_tfs_session_(NULL), fd_(0)
{
}

TfsClient::~TfsClient()
{
  for (FILE_MAP::iterator it = tfs_file_map_.begin(); it != tfs_file_map_.end(); it++)
  {
    tbsys::gDelete(it->second);
  }
  tfs_file_map_.clear();
}

TfsClient* TfsClient::Instance()
{
  static TfsClient tfs_client;
  return &tfs_client;
}

int TfsClient::initialize(const char* ns_addr, int32_t cache_time, int32_t cache_items)
{
  if (!ns_addr)
  {
    TBSYS_LOG(ERROR, "tfsclient initialize need ns ip");
    return TFS_ERROR;
  }
  if (!(default_tfs_session_ = SESSION_POOL.get(ns_addr, cache_time, cache_items)))
  {
    TBSYS_LOG(ERROR, "tfsclient initialize to ns %s failed. must exit", ns_addr);
    return TFS_ERROR;
  }

  // is_init_ = true;
  return TFS_SUCCESS;
}

int TfsClient::open_ex(const char* file_name, const char* suffix, const char* ns_addr, int flags, int32_t arg_cnt, ... )
{
  TfsSession* tfs_session = (NULL == ns_addr) ? default_tfs_session_ :
    SESSION_POOL.get(ns_addr, default_tfs_session_->get_cache_time(), default_tfs_session_->get_cache_items());

  if (!tfs_session)
  {
    TBSYS_LOG(ERROR, "can not get tfs session : %s.", ns_addr);
    return TFS_ERROR;
  }

  TfsFile* tfs_file = NULL;
  int ret = TFS_SUCCESS;
  if (arg_cnt < 1)
  {
    tfs_file = new TfsSmallFile();
    tfs_file->set_session(tfs_session);
    ret = tfs_file->open(file_name, suffix, flags);
  }
  else
  {
    va_list args;
    va_start(args, arg_cnt);
    tfs_file = new TfsLargeFile();
    tfs_file->set_session(tfs_session);
    ret = tfs_file->open(file_name, suffix, flags, va_arg(args, char*));
  }

  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "open tfsfile fail, filename: %s suffix: %s flags: %d ret %d", file_name, suffix, flags, ret);
    return EXIT_INVALIDFD_ERROR;
  }

  ret = EXIT_INVALIDFD_ERROR;
  tbutil::Mutex::Lock lock(mutex_);
  if (tfs_file_map_.find(fd_) != tfs_file_map_.end()) // should never happen
  {
    TBSYS_LOG(ERROR, "create new fd fail : fd %d already exist in map", fd_);
  }
  else if (!(tfs_file_map_.insert(std::map<int, TfsFile*>::value_type(fd_, tfs_file))).second)
  {
    TBSYS_LOG(ERROR, "insert fd %d to global file map fail", fd_);
  }
  else
  {
    ret = fd_++;
  }

  return ret;
}

ssize_t TfsClient::read(int fd, void* buf, size_t count)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->read(buf, count) : EXIT_INVALIDFD_ERROR;
}

ssize_t TfsClient::write(int fd, const void* buf, size_t count)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->write(buf, count) : EXIT_INVALIDFD_ERROR;
}

off_t TfsClient::lseek(int fd, off_t offset, int whence)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->lseek(offset, whence) : EXIT_INVALIDFD_ERROR;
}

ssize_t TfsClient::pread(int fd, void* buf, size_t count, off_t offset)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->pread(buf, count, offset) : EXIT_INVALIDFD_ERROR;
}

ssize_t TfsClient::pwrite(int fd, const void* buf, size_t count, off_t offset)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->pwrite(buf, count, offset) : EXIT_INVALIDFD_ERROR;
}

int TfsClient::fstat(int fd, FileInfo* buf, int mode)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->fstat(buf, static_cast<int32_t>(mode)) : EXIT_INVALIDFD_ERROR;
}

int TfsClient::close(int fd)
{
  TfsFile* tfs_file = get_file(fd);
  int ret = tfs_file ? tfs_file->close() : EXIT_INVALIDFD_ERROR;
  // erase tfsfile from map
  erase_file(fd);
  return ret;
}

const char* TfsClient::get_file_name(int fd)
{
  TfsFile* tfs_file = get_file(fd);
  return tfs_file ? tfs_file->get_file_name() : NULL;
}

TfsFile* TfsClient::get_file(int fd)
{
  tbutil::Mutex::Lock lock(mutex_);
  FILE_MAP::iterator it = tfs_file_map_.find(fd);
  if (tfs_file_map_.end() == it)
  {
    TBSYS_LOG(ERROR, "invaild fd: %d", fd);
    return NULL;
  }
  return it->second;
}

int TfsClient::erase_file(int fd)
{
  tbutil::Mutex::Lock lock(mutex_);
  FILE_MAP::iterator it = tfs_file_map_.find(fd);
  if (tfs_file_map_.end() == it)
  {
    TBSYS_LOG(ERROR, "invaild fd: %d", fd);
    return EXIT_INVALIDFD_ERROR;
  }
  tbsys::gDelete(it->second);
  tfs_file_map_.erase(fd);
  return TFS_SUCCESS;
}
