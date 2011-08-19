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
#include <stdarg.h>
#include <string>
#include <Memory.hpp>
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tfs_client_impl.h"
#include "tfs_large_file.h"
#include "tfs_small_file.h"
#include "gc_worker.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

TfsClientImpl::TfsClientImpl() : is_init_(false), default_tfs_session_(NULL), fd_(0),
                                 packet_factory_(NULL), packet_streamer_(NULL)
{
  packet_factory_ = new MessageFactory();
  packet_streamer_ = new BasePacketStreamer(packet_factory_);
}

TfsClientImpl::~TfsClientImpl()
{
  for (FILE_MAP::iterator it = tfs_file_map_.begin(); it != tfs_file_map_.end(); ++it)
  {
    tbsys::gDelete(it->second);
  }
  tfs_file_map_.clear();

  tbsys::gDelete(packet_factory_);
  tbsys::gDelete(packet_streamer_);
}

int TfsClientImpl::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items,
                              const bool start_bg)
{
  int ret = TFS_SUCCESS;

  tbutil::Mutex::Lock lock(mutex_);
  if (is_init_)
  {
    TBSYS_LOG(INFO, "tfsclient already initialized");
  }
  else if (TFS_SUCCESS != (ret = NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_)))
  {
    TBSYS_LOG(ERROR, "initialize NewClientManager fail, must exit, ret: %d", ret);
  }
  else if (ns_addr != NULL &&   // pass a valid ns addr, then must init success
           NULL == (default_tfs_session_ = SESSION_POOL.get(ns_addr, cache_time, cache_items)))
  {
    TBSYS_LOG(ERROR, "tfsclient initialize to ns %s failed. must exit", ns_addr);
    ret = TFS_ERROR;
  }
  else if (start_bg)
  {
    if ((ret = BgTask::initialize()) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "start bg task fail, must exit. ret: %d", ret);
    }
  }

  if (TFS_SUCCESS == ret)
  {
    set_cache_time(cache_time);
    set_cache_items(cache_items);
    is_init_ = true;
  }

  return ret;
}

int TfsClientImpl::set_default_server(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  int ret = TFS_ERROR;
  TfsSession* session = NULL;
  if (NULL == ns_addr)
  {
    TBSYS_LOG(ERROR, "ns addr is null");
  }
  else if ((session = SESSION_POOL.get(ns_addr, cache_time, cache_items)) == NULL)
  {
    TBSYS_LOG(ERROR, "get session to server %s fail.", ns_addr);
  }
  else
  {
    default_tfs_session_ = session;
    ret = TFS_SUCCESS;
  }
  return ret;
}

int TfsClientImpl::destroy()
{
  BgTask::destroy();
  BgTask::wait_for_shut_down();
  return TFS_SUCCESS;
}

int64_t TfsClientImpl::read(const int fd, void* buf, const int64_t count)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    // modify offset_: use write locker
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->read(buf, count);
  }
  return ret;
}

int64_t TfsClientImpl::readv2(const int fd, void* buf, const int64_t count, TfsFileStat* file_info)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    // modify offset_: use write locker
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->readv2(buf, count, file_info);
  }
  return ret;
}

int64_t TfsClientImpl::write(const int fd, const void* buf, const int64_t count)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->write(buf, count);
  }
  return ret;
}

int64_t TfsClientImpl::lseek(const int fd, const int64_t offset, const int whence)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    // modify offset_: use write locker
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->lseek(offset, whence);
  }
  return ret;
}

int64_t TfsClientImpl::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, READ_LOCKER);
    ret = tfs_file->pread(buf, count, offset);
  }
  return ret;
}

int64_t TfsClientImpl::pwrite(const int fd, const void* buf, const int64_t count, const int64_t offset)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->pwrite(buf, count, offset);
  }
  return ret;
}

int TfsClientImpl::fstat(const int fd, TfsFileStat* buf, const TfsStatType mode)
{
  int ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
    ret = tfs_file->fstat(buf, mode);
  }
  return ret;
}

int TfsClientImpl::close(const int fd, char* tfs_name, const int32_t len)
{
  int ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    {
      ScopedRWLock scoped_lock(tfs_file->rw_lock_, WRITE_LOCKER);
      ret = tfs_file->close();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "tfs close failed. fd: %d, ret: %d", fd, ret);
      }
      // buffer not null, then consider as wanting tfs name back
      // len must invalid
      else if (NULL != tfs_name)
      {
        if (len < TFS_FILE_LEN)
        {
          TBSYS_LOG(ERROR, "name buffer length less: %d < %d", len, TFS_FILE_LEN);
          ret = TFS_ERROR;
        }
        else
        {
          memcpy(tfs_name, tfs_file->get_file_name(), TFS_FILE_LEN);
        }
      }
    }
    erase_file(fd);
  }

  return ret;
}

int64_t TfsClientImpl::get_file_length(const int fd)
{
  int64_t ret = EXIT_INVALIDFD_ERROR;
  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    ScopedRWLock scoped_lock(tfs_file->rw_lock_, READ_LOCKER);
    ret = tfs_file->get_file_length();
  }
  return ret;
}

int TfsClientImpl::open(const char* file_name, const char* suffix, const char* ns_addr, const int flags, ...)
{
  int ret_fd = EXIT_INVALIDFD_ERROR;
  TfsSession* tfs_session = NULL;

  if (!check_init())
  {
    TBSYS_LOG(ERROR, "tfs client not init");
  }
  else if (NULL == (tfs_session = get_session(ns_addr)))
  {
    TBSYS_LOG(ERROR, "can not get tfs session: %s.", NULL == ns_addr ? "default" : ns_addr);
  }
  else if ((ret_fd = get_fd()) <= 0)
  {
    TBSYS_LOG(ERROR, "can not get fd. ret: %d", ret_fd);
  }
  else
  {
    TfsFile* tfs_file = NULL;
    int ret = TFS_ERROR;

    if (0 == (flags & common::T_LARGE))
    {
      tfs_file = new TfsSmallFile();
      tfs_file->set_session(tfs_session);
      ret = tfs_file->open(file_name, suffix, flags);
    }
    else
    {
      va_list args;
      va_start(args, flags);
      tfs_file = new TfsLargeFile();
      tfs_file->set_session(tfs_session);
      ret = tfs_file->open(file_name, suffix, flags, va_arg(args, char*));
      va_end(args);
    }

    if (ret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open tfsfile fail, filename: %s, suffix: %s, flags: %d, ret: %d", file_name, suffix, flags, ret);
    }
    else if ((ret = insert_file(ret_fd, tfs_file)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "add fd fail: %d", ret_fd);
    }

    if (ret != TFS_SUCCESS)
    {
      ret_fd = EXIT_INVALIDFD_ERROR;
      tbsys::gDelete(tfs_file);
    }
  }

  return ret_fd;
}

int TfsClientImpl::set_option_flag(const int fd, const common::OptionFlag option_flag)
{
  int ret = EXIT_INVALIDFD_ERROR;

  TfsFile* tfs_file = get_file(fd);
  if (NULL != tfs_file)
  {
    tfs_file->set_option_flag(option_flag);
    ret = TFS_SUCCESS;
  }
  return ret;
}

int TfsClientImpl::unlink(int64_t& file_size, const char* file_name, const char* suffix,
                          const TfsUnlinkType action, const OptionFlag option_flag)
{
  return unlink(file_size, file_name, suffix, NULL, action, option_flag);
}

int TfsClientImpl::unlink(int64_t& file_size, const char* file_name, const char* suffix, const char* ns_addr,
                          const TfsUnlinkType action, const OptionFlag option_flag)
{
  int ret = TFS_ERROR;
  TfsSession* tfs_session = NULL;

  if (!check_init())
  {
    TBSYS_LOG(ERROR, "tfs client not init");
  }
  else if (NULL == (tfs_session = get_session(ns_addr)))
  {
    TBSYS_LOG(ERROR, "can not get tfs session: %s.", NULL == ns_addr ? "default" : ns_addr);
  }
  else
  {
    TfsFile* tfs_file = NULL;
    TfsFileType file_type = FSName::check_file_type(file_name);
    if (file_type == SMALL_TFS_FILE_TYPE)
    {
      tfs_file = new TfsSmallFile();
      tfs_file->set_session(tfs_session);
      tfs_file->set_option_flag(option_flag);
      ret = tfs_file->unlink(file_name, suffix, file_size, action);
    }
    else if (file_type == LARGE_TFS_FILE_TYPE)
    {
      tfs_file = new TfsLargeFile();
      tfs_file->set_session(tfs_session);
      tfs_file->set_option_flag(option_flag);
      ret = tfs_file->unlink(file_name, suffix, file_size, action);
    }
    else
    {
      TBSYS_LOG(ERROR, "tfs file name illegal: %s", file_name);
    }

    tbsys::gDelete(tfs_file);
  }
  return ret;
}

#ifdef WITH_UNIQUE_STORE
TfsUniqueStore* TfsClientImpl::get_unique_store(const char* ns_addr)
{
  TfsUniqueStore* unique_store = NULL;
  TfsSession* session = get_session(ns_addr);

  if (session != NULL)
  {
    unique_store = session->get_unique_store();
  }
  else
  {
    TBSYS_LOG(ERROR, "session not init");
  }

  return unique_store;
}

int TfsClientImpl::init_unique_store(const char* master_addr, const char* slave_addr,
                                     const char* group_name, const int32_t area, const char* ns_addr)
{
  int ret = TFS_ERROR;
  TfsSession* session = get_session(ns_addr);

  if (NULL == session)
  {
    TBSYS_LOG(ERROR, "session not init");
  }
  else
  {
    ret = session->init_unique_store(master_addr, slave_addr, group_name, area);
  }

  return ret;
}
int64_t TfsclientImpl::save_unique_update(const char* tfs_file_name, const char* buf, const int64_t count,
                          const char* suffix, const int32_t flag, const char* ns_addr, const char* key)
{
  int64_t ret = NULL == buf || count <= 0 || NULL == tfs_file_name || (flag & common::T_LARGE)
              || static_cast<int32_t>(strlen(tfs_file_name)) < FILE_NAME_LEN
              || NULL == suffix || '\0' == tfs_file_name[0] || '\0' ==  suffix[0] 
              ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    ret = INVALID_FILE_SIZE;
    TfsUniqueStore* unique_store = get_unique_store(ns_addr);
    if (NULL != unique_strore)
    {
      ret = unique_store->save(buf, count, file_name, suffix, NULL, 0);
    }
    else
    {
      TBSYS_LOG(ERROR, "unique store not init");
    }
  } 
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}

int64_t TfsClientImpl::save_unique(char* ret_tfs_file_name, const int32_t ret_tfs_file_name_len, const char* buf, const int64_t count,
                   const int32_t flag, const char* suffix, const char* ns_addr, const char* key)
{
  int64_t ret = NULL == buf || count <= 0 || NULL == ret_tfs_file_name 
                || ret_tfs_file_name_len < TFS_FILE_LEN ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    ret = INVALID_FILE_SIZE;
    TfsUniqueStore* unique_store = get_unique_store(ns_addr);
    if (NULL != unique_strore)
    {
      ret = unique_store->save(buf, count, NULL, suffix, ret_tfs_file_name, ret_tfs_file_name_len);
    }
    else
    {
      TBSYS_LOG(ERROR, "unique store not init");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}


int64_t TfsClientImpl::save_unique_file_update(const char* tfs_file_name, const char* local_file,
                               const char* suffix, const int32_t flag,  const char* ns_addr, const char* key)
{
  int64_t ret = NULL == local_file || '\0' == local_file[0] || NULL == tfs_file_name
                || static_cast<int32_t>(strlen(tfs_file_name)) < TFS_FILE_LEN
                || (flag & common::T_LARGE) 
                || '\0' == tfs_file_name[0] ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    ret = INVALID_FILE_SIZE;
    TfsUniqueStore* unique_store = get_unique_store(ns_addr);
    if (NULL != unique_strore)
    {
      ret = unique_store->save(local_file, tfs_file_name, suffix, NULL, 0);
    }
    else
    {
      TBSYS_LOG(ERROR, "unique store not init");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}


int64_t TfsClientImpl::save_unique_file(char* ret_tfs_file_name, const int32_t ret_tfs_file_name_len, const char* local_file,
                        const int32_t flag,  const char* suffix, const char* ns_addr, const char* key)
{
  int64_t ret = NULL == local_file || '\0' == local_file[0] || NULL == ret_tfs_file_name
                || ret_tfs_file_name_len <= TFS_FILE_LEN ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    ret = INVALID_FILE_SIZE;
    TfsUniqueStore* unique_store = get_unique_store(ns_addr);
    if (NULL != unique_strore)
    {
      ret = unique_store->save(local_file, NULL, suffix, ret_tfs_file_name, ret_tfs_file_name_len);
    }
    else
    {
      TBSYS_LOG(ERROR, "unique store not init");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}

int32_t TfsClientImpl::unlink_unique(int64_t& file_size, const char* file_name, const char* suffix,
                                     const int32_t count, const char* ns_addr)
{
  int ret = TFS_ERROR;
  TfsUniqueStore* unique_store = get_unique_store(ns_addr);

  if (unique_store != NULL)
  {
    ret = unique_store->unlink(file_name, suffix, file_size, count);
  }
  else
  {
    TBSYS_LOG(ERROR, "unique store not init");
  }

  return ret;
}
#endif

void TfsClientImpl::set_cache_items(const int64_t cache_items)
{
  if (cache_items > 0)
  {
    ClientConfig::cache_items_ = cache_items;
    TBSYS_LOG(INFO, "set cache items: %"PRI64_PREFIX"d", ClientConfig::cache_items_);
  }
  else
  {
    TBSYS_LOG(WARN, "set cache items invalid: %"PRI64_PREFIX"d", cache_items);
  }
}

int64_t TfsClientImpl::get_cache_items() const
{
  return ClientConfig::cache_items_;
}

void TfsClientImpl::set_cache_time(const int64_t cache_time)
{
  if (cache_time > 0)
  {
    ClientConfig::cache_time_ = cache_time;
    TBSYS_LOG(INFO, "set cache time: %"PRI64_PREFIX"d", ClientConfig::cache_time_);
  }
  else
  {
    TBSYS_LOG(WARN, "set cache time invalid: %"PRI64_PREFIX"d", cache_time);
  }
}

int64_t TfsClientImpl::get_cache_time() const
{
  return ClientConfig::cache_time_;
}

void TfsClientImpl::set_segment_size(const int64_t segment_size)
{
  if (segment_size > 0 && segment_size <= MAX_SEGMENT_SIZE)
  {
    ClientConfig::segment_size_ = segment_size;
    ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
    TBSYS_LOG(INFO, "set segment size: %" PRI64_PREFIX "d, batch count: %" PRI64_PREFIX "d, batch size: %" PRI64_PREFIX "d",
              ClientConfig::segment_size_, ClientConfig::batch_count_, ClientConfig::batch_size_);
  }
  else
  {
    TBSYS_LOG(WARN, "set segment size %"PRI64_PREFIX"d not in (0, %"PRI64_PREFIX"d]", segment_size, MAX_SEGMENT_SIZE);
  }
}

int64_t TfsClientImpl::get_segment_size() const
{
  return ClientConfig::segment_size_;
}

void TfsClientImpl::set_batch_count(const int64_t batch_count)
{
  if (batch_count > 0 && batch_count <= MAX_BATCH_COUNT)
  {
    ClientConfig::batch_count_ = batch_count;
    ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
    TBSYS_LOG(INFO, "set batch count: %" PRI64_PREFIX "d, segment size: %" PRI64_PREFIX "d, batch size: %" PRI64_PREFIX "d",
              ClientConfig::batch_count_, ClientConfig::segment_size_, ClientConfig::batch_size_);
  }
  else
  {
    TBSYS_LOG(WARN, "set batch count %"PRI64_PREFIX"d not in (0, %"PRI64_PREFIX"d]", batch_count, MAX_BATCH_COUNT);
  }
}

int64_t TfsClientImpl::get_batch_count() const
{
  return ClientConfig::batch_count_;
}

void TfsClientImpl::set_stat_interval(const int64_t stat_interval_ms)
{
  if (stat_interval_ms > 0)
  {
    ClientConfig::stat_interval_ = stat_interval_ms;
    BgTask::get_stat_mgr().reset_schedule_interval(stat_interval_ms * 1000);
    TBSYS_LOG(INFO, "set stat interval: %" PRI64_PREFIX "d ms", ClientConfig::stat_interval_);
  }
  else
  {
    TBSYS_LOG(WARN, "set stat interval %"PRI64_PREFIX"d <= 0", stat_interval_ms);
  }
}

int64_t TfsClientImpl::get_stat_interval() const
{
  return ClientConfig::stat_interval_;
}

void TfsClientImpl::set_gc_interval(const int64_t gc_interval_ms)
{
  if (gc_interval_ms > 0)
  {
    ClientConfig::gc_interval_ = gc_interval_ms;
    BgTask::get_gc_mgr().reset_schedule_interval(gc_interval_ms);
    TBSYS_LOG(INFO, "set gc interval: %" PRI64_PREFIX "d ms", ClientConfig::gc_interval_);
  }
  else
  {
    TBSYS_LOG(WARN, "set gc interval %"PRI64_PREFIX"d <= 0", gc_interval_ms);
  }
}

int64_t TfsClientImpl::get_gc_interval() const
{
  return ClientConfig::gc_interval_;
}

void TfsClientImpl::set_gc_expired_time(const int64_t gc_expired_time_ms)
{
  if (gc_expired_time_ms >= MIN_GC_EXPIRED_TIME)
  {
    ClientConfig::expired_time_ = gc_expired_time_ms;
    TBSYS_LOG(INFO, "set gc expired time: %" PRI64_PREFIX "d ms", ClientConfig::expired_time_);
  }
  else
  {
    TBSYS_LOG(WARN, "set gc expired interval %"PRI64_PREFIX"d < %"PRI64_PREFIX"d",
              gc_expired_time_ms, MIN_GC_EXPIRED_TIME);
  }
}

int64_t TfsClientImpl::get_gc_expired_time() const
{
  return ClientConfig::expired_time_;
}

void TfsClientImpl::set_batch_timeout(const int64_t timeout_ms)
{
  if (timeout_ms > 0)
  {
    ClientConfig::batch_timeout_ = timeout_ms;
    TBSYS_LOG(INFO, "set batch timeout: %" PRI64_PREFIX "d ms", ClientConfig::batch_timeout_);
  }
  else
  {
    TBSYS_LOG(WARN, "set batch timeout %"PRI64_PREFIX"d <= 0", timeout_ms);
  }
}

int64_t TfsClientImpl::get_batch_timeout() const
{
  return ClientConfig::batch_timeout_;
}

void TfsClientImpl::set_wait_timeout(const int64_t timeout_ms)
{
  if (timeout_ms > 0)
  {
    ClientConfig::wait_timeout_ = timeout_ms;
    TBSYS_LOG(INFO, "set wait timeout: %" PRI64_PREFIX "d ms", ClientConfig::wait_timeout_);
  }
  else
  {
    TBSYS_LOG(WARN, "set wait timeout %"PRI64_PREFIX"d <= 0", timeout_ms);
  }
}

int64_t TfsClientImpl::get_wait_timeout() const
{
  return ClientConfig::wait_timeout_;
}

void TfsClientImpl::set_client_retry_count(const int64_t count)
{
  if (count > 0)
  {
    ClientConfig::client_retry_count_ = count;
    TBSYS_LOG(INFO, "set client retry count: %" PRI64_PREFIX "d", ClientConfig::client_retry_count_);
  }
  else
  {
    TBSYS_LOG(WARN, "set client retry count %"PRI64_PREFIX"d <= 0", count);
  }
}

int64_t TfsClientImpl::get_client_retry_count() const
{
  return ClientConfig::client_retry_count_;
}

void TfsClientImpl::set_log_level(const char* level)
{
  TBSYS_LOG(INFO, "set log level: %s", level);
  TBSYS_LOGGER.setLogLevel(level);
}

void TfsClientImpl::set_log_file(const char* file)
{
  if (NULL == file)
  {
    TBSYS_LOG(ERROR, "file is null");
  }
  else
  {
    TBSYS_LOG(INFO, "set log file: %s", file);
  }
  TBSYS_LOGGER.setFileName(file);
}

int32_t TfsClientImpl::get_block_cache_time() const
{
  int32_t ret = 0;
  if (NULL == default_tfs_session_)
  {
    TBSYS_LOG(ERROR, "no default session");
  }
  else
  {
    ret = default_tfs_session_->get_cache_time();
  }
  return ret;
}

int32_t TfsClientImpl::get_block_cache_items() const
{
  int32_t ret = 0;
  if (NULL == default_tfs_session_)
  {
    TBSYS_LOG(ERROR, "no default session");
  }
  else
  {
    ret = default_tfs_session_->get_cache_items();
  }
  return ret;
}

int32_t TfsClientImpl::get_cache_hit_ratio() const
{
  return BgTask::get_cache_hit_ratio();
}

uint64_t TfsClientImpl::get_server_id()
{
  uint64_t server_id = 0;
  if (default_tfs_session_ != NULL)
  {
    server_id = default_tfs_session_->get_ns_addr();
  }
  return server_id;
}

int32_t TfsClientImpl::get_cluster_id()
{
  int32_t cluster_id = 0;
  if (default_tfs_session_ != NULL)
  {
    cluster_id = default_tfs_session_->get_cluster_id();
  }
  return cluster_id;
}

int64_t TfsClientImpl::save_update(const char* tfs_file_name, const char* buf, const int64_t count,
            const int32_t flag, const char* suffix, const char* ns_addr, const char* key)
{
  int64_t ret = NULL == buf || count <= 0 || NULL == tfs_file_name || (flag &  common::T_LARGE) 
            || '\0' == tfs_file_name[0] || static_cast<int32_t>(strlen(tfs_file_name)) < TFS_FILE_LEN
              ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int fd = open(tfs_file_name, suffix, ns_addr, (T_WRITE|flag), key);
    if (fd <= 0)
    {
      TBSYS_LOG(ERROR, "open tfs file to write fail. tfsname: %s, suffix: %s, flag: %d, key: %s, ret: %d",
                tfs_file_name, suffix, flag, key, fd);
    }
    else
    {
      int64_t write_len = this->write(fd, buf, count);
      if (write_len != count)
      {
        TBSYS_LOG(ERROR, "write to tfs fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
                  count, write_len);
      }

      // close anyway
      if ((ret = this->close(fd)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}

int64_t TfsClientImpl::save(char* ret_tfs_file_name, const int32_t ret_tfs_file_name_len,
                        const char* buf, const int64_t count, const int32_t flag, 
                        const char* suffix, const char* ns_addr, const char* key)
{
  int64_t ret = NULL == buf || count <= 0 || NULL == ret_tfs_file_name 
              || ret_tfs_file_name_len < TFS_FILE_LEN ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int fd = this->open(NULL, suffix, ns_addr, (T_WRITE|flag), key);
    if (fd <= 0)
    {
      TBSYS_LOG(ERROR, "open tfs file to write fail. suffix: %s, flag: %d, key: %s, ret: %d",
                suffix, flag, key, fd);
    }
    else
    {
      int64_t write_len = this->write(fd, buf, count);
      if (write_len != count)
      {
        TBSYS_LOG(ERROR, "write to tfs fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
                  count, write_len);
      }

      // close anyway
      if ((ret = this->close(fd, ret_tfs_file_name, ret_tfs_file_name_len)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}
int64_t TfsClientImpl::save_file_update(const char* tfs_file_name,const char* local_file, 
                         const int32_t flag, const char* suffix, const char* ns_addr, const char* key)
{
  UNUSED(key);
  int64_t ret = NULL == local_file || '\0' == local_file[0] 
              || NULL == tfs_file_name || '\0' == tfs_file_name || flag & T_LARGE
              || static_cast<int32_t>(strlen(tfs_file_name)) < TFS_FILE_LEN ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int32_t fd = ::open(local_file, O_RDONLY);
    if (fd < 0)
    {
      TBSYS_LOG(ERROR, "open local file %s fail: %s", local_file, strerror(errno));
    }
    else
    {
      int tfs_fd = this->open(tfs_file_name, suffix, ns_addr, T_WRITE|flag, local_file);
      if (tfs_fd <= 0)
      {
        TBSYS_LOG(ERROR, "open tfs file to write fail. tfsname: %s, suffix: %s, flag: %d, ret: %d",
            tfs_file_name, suffix, flag, tfs_fd);
      }
      else
      {
        int32_t io_size = MAX_READ_SIZE;
        char* buf = new char[io_size];
        int64_t length = 0;
        int64_t write_len = 0;
        int64_t file_size = 0;
        while(true)
        {
          length = ::read(fd, buf, io_size);
          if (length < 0)
          {
            if (length == EAGAIN)
              continue;
            else
              break;
          }
          else if (0 == length)
          {
            break;
          }
          else
          {
            write_len = write(tfs_fd, buf, length);
            if (write_len != length)
            {
              TBSYS_LOG(ERROR, "write to tfs fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
                      length, write_len);
              break;
            }
            file_size += length;
          }
        }
        if ((ret = this->close(tfs_fd)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
        }
        else
        {
          ret = file_size;
        }
        tbsys::gDeleteA(buf);
      }
      ::close(fd);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}
int64_t TfsClientImpl::save_file(char* ret_tfs_file_name, const int32_t ret_tfs_file_name_len, const char* local_file,
                        const int32_t flag,  const char* suffix, const char* ns_addr , const char* key)
{
  UNUSED(key);
  int64_t ret = NULL == local_file || '\0' == local_file[0] 
              || NULL == ret_tfs_file_name || ret_tfs_file_name_len < TFS_FILE_LEN
              ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
  if (TFS_SUCCESS == ret)
  {
    int32_t fd = ::open(local_file, O_RDONLY);
    if (fd < 0)
    {
      TBSYS_LOG(ERROR, "open local file %s fail: %s", local_file, strerror(errno));
    }
    else
    {
      int tfs_fd = this->open(NULL, suffix, ns_addr, T_WRITE|flag, local_file);
      if (tfs_fd <= 0)
      {
        TBSYS_LOG(ERROR, "open tfs file to write fail.suffix: %s, flag: %d, ret: %d",
            suffix, flag, tfs_fd);
      }
      else
      {
        int32_t io_size = flag & T_LARGE ? 4 * MAX_READ_SIZE : MAX_READ_SIZE;
        char* buf = new char[io_size];
        int64_t length = 0;
        int64_t write_len = 0;
        int64_t file_size = 0;
        while(true)
        {
          length = ::read(fd, buf, io_size);
          if (length < 0)
          {
            if (errno == EAGAIN)
              continue;
            else
              break;
          }
          else if (0 == length)
          {
            break;
          }
          else
          {
            write_len = this->write(tfs_fd, buf, length);
            if (write_len != length)
            {
              TBSYS_LOG(ERROR, "write to tfs fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d",
                      length , write_len);
              break;
            }
            file_size += length;
          }
        }
        if ((ret = this->close(tfs_fd, ret_tfs_file_name, ret_tfs_file_name_len)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
        }
        else
        {
          ret = file_size;
        }
        tbsys::gDeleteA(buf);
      }
      ::close(fd);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid parameter, must be return");
  }
  return ret;
}

int TfsClientImpl::fetch_file(const char* local_file, const char* tfs_name, const char* suffix, const char* ns_addr)
{
  int ret = TFS_ERROR;
  int fd = -1;
  TfsFileType file_type = INVALID_TFS_FILE_TYPE;

  if (NULL == local_file)
  {
    TBSYS_LOG(ERROR, "local file is null");
  }
  else if ((file_type = FSName::check_file_type(tfs_name)) == INVALID_TFS_FILE_TYPE)
  {
    TBSYS_LOG(ERROR, "invalid tfs name: %s", tfs_name);
  }
  else if ((fd = ::open(local_file, O_WRONLY|O_CREAT, 0644)) < 0)
  {
    TBSYS_LOG(ERROR, "open local file %s to write fail: %s", local_file, strerror(errno));
  }
  else
  {
    int32_t flag = T_DEFAULT;
    int32_t io_size = MAX_READ_SIZE;
    if (file_type == LARGE_TFS_FILE_TYPE)
    {
      flag = T_LARGE;
      io_size = 4 * MAX_READ_SIZE;
    }

    int tfs_fd = this->open(tfs_name, suffix, ns_addr, T_READ|flag);
    if (tfs_fd <= 0)
    {
      TBSYS_LOG(ERROR, "open tfs file to read fail. tfsname: %s, suffix: %s, ret: %d",
                tfs_name, suffix, tfs_fd);
    }
    else
    {
      char* buf = new char[io_size];
      int64_t read_len = 0, write_len = 0;

      while (1)
      {
        if ((read_len = this->read(tfs_fd, buf, io_size)) < 0)
        {
          TBSYS_LOG(ERROR, "read tfs file fail. tfsname: %s, suffix: %s, ret: %"PRI64_PREFIX"d",
                    tfs_name, suffix, read_len);
          break;
        }

        if (0 == read_len)
        {
          ret = TFS_SUCCESS;
          break;
        }

        if ((write_len = ::write(fd, buf, read_len)) != read_len)
        {
          TBSYS_LOG(ERROR, "write local file %s fail, write len: %"PRI64_PREFIX"d, ret: %"PRI64_PREFIX"d, error: %s",
                    local_file, read_len, write_len, strerror(errno));
          break;
        }

        if (read_len < io_size)
        {
          ret = TFS_SUCCESS;
          break;
        }
      }

      close(tfs_fd);
      tbsys::gDeleteA(buf);
    }
    ::close(fd);
  }

  return ret;
}

// fetch file to buffer, return count
// WARNING: user MUST free buf.
int TfsClientImpl::fetch_file(char*& buf, int64_t& count, const char* tfs_name, const char* suffix,
                              const char* ns_addr)
{
  int ret = TFS_ERROR;
  TfsFileType file_type = INVALID_TFS_FILE_TYPE;

  if ((file_type = FSName::check_file_type(tfs_name)) == INVALID_TFS_FILE_TYPE)
  {
    TBSYS_LOG(ERROR, "invalid tfs name: %s", tfs_name);
  }
  else
  {
    int32_t flag = T_DEFAULT;
    int32_t io_size = MAX_READ_SIZE;
    if (file_type == LARGE_TFS_FILE_TYPE)
    {
      flag = T_LARGE;
      io_size = 4 * MAX_READ_SIZE;
    }

    int tfs_fd = this->open(tfs_name, suffix, ns_addr, T_READ|flag);
    int64_t file_length = 0;

    if (tfs_fd <= 0)
    {
      TBSYS_LOG(ERROR, "open tfs file to read fail. tfsname: %s, suffix: %s, ret: %d",
                tfs_name, suffix, tfs_fd);
    }
    else
    {
      if ((file_length = get_file_length(tfs_fd)) <= 0)
      {
        TBSYS_LOG(ERROR, "get file length fail. ret: %"PRI64_PREFIX"d", file_length);
      }
      else if (file_length > TFS_MALLOC_MAX_SIZE) // cannot alloc buffer once
      {
        TBSYS_LOG(ERROR, "file length larger than max malloc size. %"PRI64_PREFIX"d > %"PRI64_PREFIX"d",
                  file_length, TFS_MALLOC_MAX_SIZE);
      }
      else
      {
        // user MUST free
        buf = new char[file_length];

        int64_t read_len = 0, already_read_len = 0;

        while (1)
        {
          if ((read_len = this->read(tfs_fd, buf + already_read_len, io_size)) < 0)
          {
            TBSYS_LOG(ERROR, "read tfs file fail. tfsname: %s, suffix: %s, ret: %"PRI64_PREFIX"d",
                      tfs_name, suffix, read_len);
            break;
          }

          already_read_len += read_len;
          if (already_read_len >= file_length)
          {
            ret = TFS_SUCCESS;
            count = already_read_len;
            break;
          }
        }

        if (TFS_SUCCESS != ret)
        {
          tbsys::gDeleteA(buf);
        }
      }
      close(tfs_fd);
    }
  }

  return ret;
}

int TfsClientImpl::stat_file(common::TfsFileStat* file_stat, const char* tfs_name, const char* suffix,
                    const char* ns_addr , const common::TfsStatType stat_type)
{
  int ret = TFS_ERROR;
  TfsFileType file_type = INVALID_TFS_FILE_TYPE;

  if (NULL == file_stat)
  {
    TBSYS_LOG(ERROR, "tfsfilestat is null");
  }
  else if ((file_type = FSName::check_file_type(tfs_name)) == INVALID_TFS_FILE_TYPE)
  {
    TBSYS_LOG(ERROR, "invalid tfs name: %s", tfs_name);
  }
  else
  {
    int32_t flag = T_DEFAULT;
    if (LARGE_TFS_FILE_TYPE == file_type)
    {
      flag |= T_LARGE;
    }

    int tfs_fd = this->open(tfs_name, suffix, ns_addr, T_STAT|flag);
    if (tfs_fd < 0)
    {
      TBSYS_LOG(ERROR, "open tfs file stat fail. tfsname: %s, suffix: %s", tfs_name, suffix);
    }
    else if ((ret = fstat(tfs_fd, file_stat, stat_type)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "stat tfs file fail. tfsname: %s, suffix, %s, stattype: %d",
                tfs_name, suffix, stat_type);
    }
    this->close(tfs_fd);
  }

  return ret;
}

// check if tfsclient is already initialized.
// read and write and stuffs that need open first,
// need no init check cause open already does it,
// and they will check if file is open.
bool TfsClientImpl::check_init()
{
  if (!is_init_)
  {
    TBSYS_LOG(ERROR, "tfsclient not initialized");
  }

  return is_init_;
}

TfsSession* TfsClientImpl::get_session(const char* ns_addr)
{
  return NULL == ns_addr ? default_tfs_session_ :
    SESSION_POOL.get(ns_addr, ClientConfig::cache_time_, ClientConfig::cache_items_);
}

TfsFile* TfsClientImpl::get_file(const int fd)
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

int TfsClientImpl::get_fd()
{
  int ret_fd = EXIT_INVALIDFD_ERROR;

  tbutil::Mutex::Lock lock(mutex_);
  if (static_cast<int32_t>(tfs_file_map_.size()) >= MAX_OPEN_FD_COUNT)
  {
    TBSYS_LOG(ERROR, "too much open files");
  }
  else
  {
    if (MAX_FILE_FD == fd_)
    {
      fd_ = 0;
    }

    bool fd_confict = true;
    int retry = MAX_OPEN_FD_COUNT;

    while (retry-- > 0 &&
           (fd_confict = (tfs_file_map_.find(++fd_) != tfs_file_map_.end())))
    {
      if (MAX_FILE_FD == fd_)
      {
        fd_ = 0;
      }
    }

    if (fd_confict)
    {
      TBSYS_LOG(ERROR, "too much open files");
    }
    else
    {
      ret_fd = fd_;
    }
  }

  return ret_fd;
}

int TfsClientImpl::insert_file(const int fd, TfsFile* tfs_file)
{
  int ret = TFS_ERROR;

  if (NULL != tfs_file)
  {
    tbutil::Mutex::Lock lock(mutex_);
    ret = (tfs_file_map_.insert(std::map<int, TfsFile*>::value_type(fd, tfs_file))).second ?
      TFS_SUCCESS : TFS_ERROR;
  }

  return ret;
}

int TfsClientImpl::erase_file(const int fd)
{
  tbutil::Mutex::Lock lock(mutex_);
  FILE_MAP::iterator it = tfs_file_map_.find(fd);
  if (tfs_file_map_.end() == it)
  {
    TBSYS_LOG(ERROR, "invaild fd: %d", fd);
    return EXIT_INVALIDFD_ERROR;
  }
  tbsys::gDelete(it->second);
  tfs_file_map_.erase(it);
  return TFS_SUCCESS;
}
