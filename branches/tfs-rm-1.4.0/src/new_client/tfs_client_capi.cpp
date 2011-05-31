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
 *      - initial release
 *
 */
#include "tfs_client_api.h"
#include "tfs_client_capi.h"

using namespace tfs::client;

int t_initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  return TfsClient::Instance()->initialize(ns_addr, cache_time, cache_items);
}

int t_destroy()
{
  return TfsClient::Instance()->destroy();
}

int t_open(const char* file_name, const char* suffix, const int flags, const char* local_key)
{
  return t_open2(file_name, suffix, NULL, flags, local_key);
}

int t_open2(const char* file_name, const char* suffix, const char* ns_addr, const int flags, const char* local_key)
{
  int ret = EXIT_INVALIDFD_ERROR;
  if (NULL == local_key)
  {
    ret = TfsClient::Instance()->open(file_name, suffix, ns_addr, flags);
  }
  else
  {
    ret = TfsClient::Instance()->open(file_name, suffix, ns_addr, flags, local_key);
  }
  return ret;
}

int64_t t_read(const int fd, void* buf, const int64_t count)
{
  return TfsClient::Instance()->read(fd, buf, count);
}

int64_t t_write(const int fd, const void* buf, const int64_t count)
{
  return TfsClient::Instance()->write(fd, buf, count);
}

int64_t t_lseek(const int fd, const int64_t offset, const int whence)
{
  return TfsClient::Instance()->lseek(fd, offset, whence);
}

int64_t t_pread(const int fd, void* buf, const int64_t count, const int64_t offset)
{
  return TfsClient::Instance()->pread(fd, buf, count, offset);
}

int64_t t_pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
{
  return TfsClient::Instance()->pwrite(fd, buf, count, offset);
}

int t_fstat(const int fd, TfsFileStat* buf, const TfsStatType mode)
{
  return TfsClient::Instance()->fstat(fd,
                                      reinterpret_cast<tfs::common::TfsFileStat*>(buf),
                                      static_cast<tfs::common::TfsStatType>(mode));
}

int t_close(const int fd, char* tfs_name, const int32_t len)
{
  return TfsClient::Instance()->close(fd, tfs_name, len);
}

int64_t t_get_file_length(const int fd)
{
  return TfsClient::Instance()->get_file_length(fd);
}

int t_unlink(const char* file_name, const char* suffix, const TfsUnlinkType action)
{
  return TfsClient::Instance()->unlink(file_name, suffix, static_cast<tfs::common::TfsUnlinkType>(action));
}

void t_set_segment_size(const int64_t segment_size)
{
  return TfsClient::Instance()->set_segment_size(segment_size);
}

int64_t t_get_segment_size()
{
  return TfsClient::Instance()->get_segment_size();
}

void t_set_batch_count(const int64_t batch_count)
{
  return TfsClient::Instance()->set_batch_count(batch_count);
}

int64_t t_get_batch_count()
{
  return TfsClient::Instance()->get_batch_count();
}

void t_set_stat_interval(const int64_t stat_interval_ms)
{
  return TfsClient::Instance()->set_stat_interval(stat_interval_ms);
}

int64_t t_get_stat_interval()
{
  return TfsClient::Instance()->get_stat_interval();
}

void t_set_gc_interval(const int64_t gc_interval_ms)
{
  return TfsClient::Instance()->set_gc_interval(gc_interval_ms);
}

int64_t t_get_gc_interval()
{
  return TfsClient::Instance()->get_gc_interval();
}

void t_set_gc_expired_time(const int64_t gc_expired_time_ms)
{
  return TfsClient::Instance()->set_gc_expired_time(gc_expired_time_ms);
}

int64_t t_get_gc_expired_time()
{
  return TfsClient::Instance()->get_gc_expired_time();
}

void t_set_batch_timeout(const int64_t timeout_ms)
{
  return TfsClient::Instance()->set_batch_timeout(timeout_ms);
}

int64_t t_get_batch_timeout()
{
  return TfsClient::Instance()->get_batch_timeout();
}

void t_set_wait_timeout(const int64_t timeout_ms)
{
  return TfsClient::Instance()->set_wait_timeout(timeout_ms);
}

int64_t t_get_wait_timeout()
{
  return TfsClient::Instance()->get_wait_timeout();
}

void t_set_client_retry_count(const int64_t count)
{
  return TfsClient::Instance()->set_client_retry_count(count);
}

int64_t t_get_client_retry_count()
{
  return TfsClient::Instance()->get_client_retry_count();
}

void t_set_log_level(const char* level)
{
  return TfsClient::Instance()->set_log_level(level);
}
