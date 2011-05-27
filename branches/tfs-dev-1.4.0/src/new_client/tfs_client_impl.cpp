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

TfsClientImpl::TfsClientImpl() : is_init_(false), default_tfs_session_(NULL), fd_(1), 
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

  if (NULL != packet_factory_)
  {
    delete packet_factory_;
    packet_factory_ = NULL;
  }
  if (NULL != packet_streamer_)
  {
    delete packet_streamer_;
    packet_streamer_ = NULL;
  }
}

int TfsClientImpl::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
{
  int ret = TFS_SUCCESS;

  tbutil::Mutex::Lock lock(mutex_);
  if (is_init_)
  {
    TBSYS_LOG(INFO, "tfsclient already initialized");
  }
  else
  {

    if (NULL == ns_addr)
    {
      TBSYS_LOG(ERROR, "tfsclient initialize need ns ip");
      ret = TFS_ERROR;
    }
    else
    {
      if (NULL == (default_tfs_session_ = SESSION_POOL.get(ns_addr, cache_time, cache_items)))
      {
        TBSYS_LOG(ERROR, "tfsclient initialize to ns %s failed. must exit", ns_addr);
        ret = TFS_ERROR;
      }
      else if ((ret = BgTask::initialize()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "start bg task fail, ret: %d", ret);
      }
      else
      {
        if (TFS_SUCCESS != (ret = NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_)))
        {
          TBSYS_LOG(ERROR, "initialize NewClientManager fail, ret: %d", ret);
        }
        else
        {
          is_init_ = true;
        }
      }
    }
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

int TfsClientImpl::fstat(const int fd, TfsFileStat* buf, const TfsStatFlag mode)
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
      else
      {
        if (NULL == tfs_name || len < TFS_FILE_LEN)
        {
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
  if (!check_init())
  {
    return common::EXIT_NOT_INIT_ERROR;
  }

  TfsSession* tfs_session = (NULL == ns_addr) ? default_tfs_session_ :
    SESSION_POOL.get(ns_addr, default_tfs_session_->get_cache_time(), default_tfs_session_->get_cache_items());

  if (NULL == tfs_session)
  {
    TBSYS_LOG(ERROR, "can not get tfs session : %s.", ns_addr);
    return TFS_ERROR;
  }

  TfsFile* tfs_file = NULL;
  int ret = TFS_SUCCESS;
  if (!(flags & common::T_LARGE))
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
    return EXIT_INVALIDFD_ERROR;
  }

  ret = EXIT_INVALIDFD_ERROR;
  tbutil::Mutex::Lock lock(mutex_);
  if (tfs_file_map_.find(fd_) != tfs_file_map_.end()) // should never happen
  {
    TBSYS_LOG(ERROR, "create new fd fail. fd: %d already exist in map", fd_);
  }
  else if (!(tfs_file_map_.insert(std::map<int, TfsFile*>::value_type(fd_, tfs_file))).second)
  {
    TBSYS_LOG(ERROR, "insert fd: %d to global file map fail", fd_);
  }
  else
  {
    ret = fd_++;
  }

  return ret;
}

int TfsClientImpl::unlink(const char* file_name, const char* suffix, const char* ns_addr, const TfsUnlinkType action)
{
  int ret = TFS_SUCCESS;

  if (!check_init())
  {
    ret = EXIT_NOT_INIT_ERROR;
  }
  else
  {
    TfsSession* tfs_session = (NULL == ns_addr) ? default_tfs_session_ :
      SESSION_POOL.get(ns_addr, default_tfs_session_->get_cache_time(), default_tfs_session_->get_cache_items());

    if (NULL == tfs_session)
    {
      TBSYS_LOG(ERROR, "can not get tfs session : %s.", ns_addr);
      ret = TFS_ERROR;
    }
    else
    {
      TfsFile* tfs_file = NULL;
      if (file_name[0] == 'T')
      {
        tfs_file = new TfsSmallFile();
        tfs_file->set_session(tfs_session);
        ret = tfs_file->unlink(file_name, suffix, action);
      }
      else if (file_name[0] == 'L')
      {
        if (DELETE != action && CONCEAL != action && REVEAL != action)
        {
          TBSYS_LOG(ERROR, "now can not unlink large file with action: %d", action);
          ret = TFS_ERROR;
        }
        else
        {
          tfs_file = new TfsLargeFile();
          tfs_file->set_session(tfs_session);
          ret = tfs_file->unlink(file_name, suffix, action);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "tfs file name illegal: %s", file_name);
      }
    }
  }
  return ret;
}

void TfsClientImpl::set_segment_size(const int64_t segment_size)
{
  ClientConfig::segment_size_ = segment_size;
  ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
  TBSYS_LOG(INFO, "set segment size: %" PRI64_PREFIX "d, batch count: %" PRI64_PREFIX "d, batch size: %" PRI64_PREFIX "d",
      ClientConfig::segment_size_, ClientConfig::batch_count_, ClientConfig::batch_size_);
}

int64_t TfsClientImpl::get_segment_size()
{
  return ClientConfig::segment_size_;
}

void TfsClientImpl::set_batch_count(const int64_t batch_count)
{
  ClientConfig::batch_count_ = batch_count;
  ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
  TBSYS_LOG(INFO, "set batch count: %" PRI64_PREFIX "d, segment size: %" PRI64_PREFIX "d, batch size: %" PRI64_PREFIX "d",
      ClientConfig::batch_count_, ClientConfig::segment_size_, ClientConfig::batch_size_);
}

int64_t TfsClientImpl::get_batch_count()
{
  return ClientConfig::batch_count_;
}

void TfsClientImpl::set_gc_expired_time(const int64_t gc_expired_time_s)
{
  ClientConfig::expired_time_ = gc_expired_time_s;
  TBSYS_LOG(INFO, "set gc expired time: %" PRI64_PREFIX "d", ClientConfig::expired_time_);
}

int64_t TfsClientImpl::get_gc_expired_time()
{
  return ClientConfig::expired_time_;
}

void TfsClientImpl::set_gc_interval(const int64_t gc_interval_s)
{
  ClientConfig::gc_interval_ = gc_interval_s;
  BgTask::get_gc_mgr().reset_schedule_interval(gc_interval_s);
  TBSYS_LOG(INFO, "set gc interval: %" PRI64_PREFIX "d", ClientConfig::gc_interval_);
}

int64_t TfsClientImpl::get_gc_interval()
{
  return ClientConfig::gc_interval_;
}

void TfsClientImpl::set_batch_time_out(const int64_t time_out_us)
{
  ClientConfig::batch_time_out_ = time_out_us;
  TBSYS_LOG(INFO, "set batch time out: %" PRI64_PREFIX "d", ClientConfig::batch_time_out_);
}

int64_t TfsClientImpl::get_batch_time_out()
{
  return ClientConfig::batch_time_out_;
}

void TfsClientImpl::set_log_level(const char* level)
{
  TBSYS_LOG(INFO, "set log level: %s", level);
  TBSYS_LOGGER.setLogLevel(level);
}

// check if tfsclient is already initialized.
// read and write and stuffs that need open first,
// need no init check cause open already does it,
// and they will check if file is open.
bool TfsClientImpl::check_init()
{
  tbutil::Mutex::Lock lock(mutex_);
  if (!is_init_)
  {
    TBSYS_LOG(ERROR, "tfsclient not initialized");
  }

  return is_init_;
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
