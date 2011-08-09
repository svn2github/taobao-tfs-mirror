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
 *      - initial release
 *
 */
#include "tfs_client_api.h"
#include "tfs_client_impl.h"
#include "client_config.h"

using namespace tfs::common;
using namespace tfs::client;
using namespace std;

TfsClient::TfsClient()
{
}

TfsClient::~TfsClient()
{
}

int TfsClient::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items,
                          const bool start_bg)
{
  return TfsClientImpl::Instance()->initialize(ns_addr, cache_time, cache_items, start_bg);
}

int TfsClient::set_default_server(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  return TfsClientImpl::Instance()->set_default_server(ns_addr, cache_time, cache_items);
}

int TfsClient::destroy()
{
  return TfsClientImpl::Instance()->destroy();
}

int64_t TfsClient::read(const int fd, void* buf, const int64_t count)
{
  return TfsClientImpl::Instance()->read(fd, buf, count);
}

int64_t TfsClient::readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* file_info)
{
  return TfsClientImpl::Instance()->readv2(fd, buf, count, file_info);
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

int TfsClient::fstat(const int fd, TfsFileStat* buf, const TfsStatType mode)
{
  return TfsClientImpl::Instance()->fstat(fd, buf, mode);
}

int TfsClient::close(const int fd, char* tfs_name, const int32_t len)
{
  return TfsClientImpl::Instance()->close(fd, tfs_name, len);
}

int64_t TfsClient::get_file_length(const int fd)
{
  return TfsClientImpl::Instance()->get_file_length(fd);
}

int TfsClient::set_option_flag(const int fd, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->set_option_flag(fd, option_flag);
}

int TfsClient:: unlink(const char* file_name, const char* suffix, int64_t& file_size,
                       const TfsUnlinkType action, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->unlink(file_name, suffix, file_size, action, option_flag);
}

int TfsClient::unlink(const char* file_name, const char* suffix, const char* ns_addr, int64_t& file_size,
                      const TfsUnlinkType action, const OptionFlag option_flag)
{
  return TfsClientImpl::Instance()->unlink(file_name, suffix, ns_addr, file_size, action, option_flag);
}

#ifdef WITH_UNIQUE_STORE
int TfsClient::init_unique_store(const char* master_addr, const char* slave_addr,
                                     const char* group_name, const int32_t area, const char* ns_addr)
{
  return TfsClientImpl::Instance()->init_unique_store(master_addr, slave_addr, group_name, area, ns_addr);
}

int64_t TfsClient::save_unique(const char* buf, const int64_t count,
                           const char* file_name, const char* suffix,
                           char* ret_tfs_name, const int32_t ret_tfs_name_len, const char* ns_addr)
{
  return TfsClientImpl::Instance()->save_unique(buf, count, file_name, suffix,
                                                ret_tfs_name, ret_tfs_name_len, ns_addr);
}

int64_t TfsClient::save_unique(const char* local_file,
                           const char* file_name, const char* suffix,
                           char* ret_tfs_name, const int32_t ret_tfs_name_len, const char* ns_addr)
{
  return TfsClientImpl::Instance()->save_unique(local_file, file_name, suffix,
                                                ret_tfs_name, ret_tfs_name_len, ns_addr);
}

int32_t TfsClient::unlink_unique(const char* file_name, const char* suffix, int64_t& file_size,
                                 const int32_t count, const char* ns_addr)
{
  return TfsClientImpl::Instance()->unlink_unique(file_name, suffix, file_size, count, ns_addr);
}
#endif

void TfsClient::set_cache_items(const int64_t cache_items)
{
  return TfsClientImpl::Instance()->set_cache_items(cache_items);
}

int64_t TfsClient::get_cache_items() const
{
  return TfsClientImpl::Instance()->get_cache_items();
}

void TfsClient::set_cache_time(const int64_t cache_time)
{
  return TfsClientImpl::Instance()->set_cache_time(cache_time);
}

int64_t TfsClient::get_cache_time() const
{
  return TfsClientImpl::Instance()->get_cache_time();
}

void TfsClient::set_segment_size(const int64_t segment_size)
{
  return TfsClientImpl::Instance()->set_segment_size(segment_size);
}

int64_t TfsClient::get_segment_size() const
{
  return TfsClientImpl::Instance()->get_segment_size();
}

void TfsClient::set_batch_count(const int64_t batch_count)
{
  return TfsClientImpl::Instance()->set_batch_count(batch_count);
}

int64_t TfsClient::get_batch_count() const
{
  return TfsClientImpl::Instance()->get_batch_count();
}

void TfsClient::set_stat_interval(const int64_t stat_interval_ms)
{
  return TfsClientImpl::Instance()->set_stat_interval(stat_interval_ms);
}

int64_t TfsClient::get_stat_interval() const
{
  return TfsClientImpl::Instance()->get_stat_interval();
}

void TfsClient::set_gc_interval(const int64_t gc_interval_ms)
{
  return TfsClientImpl::Instance()->set_gc_interval(gc_interval_ms);
}

int64_t TfsClient::get_gc_interval() const
{
  return TfsClientImpl::Instance()->get_gc_interval();
}

void TfsClient::set_gc_expired_time(const int64_t gc_expired_time_ms)
{
  return TfsClientImpl::Instance()->set_gc_expired_time(gc_expired_time_ms);
}

int64_t TfsClient::get_gc_expired_time() const
{
  return TfsClientImpl::Instance()->get_gc_expired_time();
}

void TfsClient::set_batch_timeout(const int64_t timeout_ms)
{
  return TfsClientImpl::Instance()->set_batch_timeout(timeout_ms);
}

int64_t TfsClient::get_batch_timeout() const
{
  return TfsClientImpl::Instance()->get_batch_timeout();
}

void TfsClient::set_wait_timeout(const int64_t timeout_ms)
{
  return TfsClientImpl::Instance()->set_wait_timeout(timeout_ms);
}

int64_t TfsClient::get_wait_timeout() const
{
  return TfsClientImpl::Instance()->get_wait_timeout();
}

void TfsClient::set_client_retry_count(const int64_t count)
{
  return TfsClientImpl::Instance()->set_client_retry_count(count);
}

int64_t TfsClient::get_client_retry_count() const
{
  return TfsClientImpl::Instance()->get_client_retry_count();
}

void TfsClient::set_log_level(const char* level)
{
  return TfsClientImpl::Instance()->set_log_level(level);
}

void TfsClient::set_log_file(const char* file)
{
  return TfsClientImpl::Instance()->set_log_file(file);
}

int32_t TfsClient::get_block_cache_time() const
{
  return TfsClientImpl::Instance()->get_block_cache_time();
}

int32_t TfsClient::get_block_cache_items() const
{
  return TfsClientImpl::Instance()->get_block_cache_items();
}

int32_t TfsClient::get_cache_hit_ratio() const
{
  return TfsClientImpl::Instance()->get_cache_hit_ratio();
}

uint64_t TfsClient::get_server_id()
{
  return TfsClientImpl::Instance()->get_server_id();
}

int32_t TfsClient::get_cluster_id()
{
  return TfsClientImpl::Instance()->get_cluster_id();
}

int64_t TfsClient::save_file(const char* local_file, const char* tfs_name, const char* suffix,
                         char* ret_tfs_name, const int32_t ret_tfs_name_len,
                         const char* ns_addr, const int32_t flag)
{
  return TfsClientImpl::Instance()->save_file(local_file, tfs_name, suffix,
                                              ret_tfs_name, ret_tfs_name_len, ns_addr, flag);
}

int64_t TfsClient::save_file(const char* buf, const int64_t count, const char* tfs_name, const char* suffix,
                         char* ret_tfs_name, const int32_t ret_tfs_name_len,
                         const char* ns_addr, const int32_t flag, const char* key)
{
  return TfsClientImpl::Instance()->save_file(buf, count, tfs_name, suffix,
                                              ret_tfs_name, ret_tfs_name_len, ns_addr, flag, key);
}

int TfsClient::fetch_file(const char* local_file, const char* tfs_name, const char* suffix, const char* ns_addr)
{
  return TfsClientImpl::Instance()->fetch_file(local_file, tfs_name, suffix, ns_addr);
}

int TfsClient::fetch_file(const char* tfs_name, const char* suffix, char*& buf, int64_t& count, const char* ns_addr)
{
  return TfsClientImpl::Instance()->fetch_file(tfs_name, suffix, buf, count, ns_addr);
}

int TfsClient::stat_file(const char* tfs_name, const char* suffix,
                         TfsFileStat* file_stat, const TfsStatType stat_type, const char* ns_addr)
{
  return TfsClientImpl::Instance()->stat_file(tfs_name, suffix, file_stat, stat_type, ns_addr);
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
