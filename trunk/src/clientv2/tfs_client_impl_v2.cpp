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
#include <stdarg.h>
#include <string>
#include <Memory.hpp>
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "tfs_file.h"
#include "tfs_client_impl_v2.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace clientv2
  {
    const int32_t DEFAULT_RETRY_TIMES = 2;

    TfsClientImplV2::TfsClientImplV2() : is_init_(false), fd_(0),
    ns_addr_(0), cluster_id_(0), packet_factory_(NULL), packet_streamer_(NULL),
    default_session_(NULL)
    {
      timer_ = new tbutil::Timer();
      session_pool_ = new TfsSessionPool(timer_);
      packet_factory_ = new MessageFactory();
      packet_streamer_ = new BasePacketStreamer(packet_factory_);
    }

    TfsClientImplV2::~TfsClientImplV2()
    {
      for (FILE_MAP::iterator it = tfs_file_map_.begin(); it != tfs_file_map_.end(); ++it)
      {
        tbsys::gDelete(it->second);
      }
      tfs_file_map_.clear();

      tbsys::gDelete(session_pool_);
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
    }

    int TfsClientImplV2::initialize(const char* ns_addr, const int32_t cache_time, const int32_t cache_items)
    {
      int ret = TFS_SUCCESS;
      tbutil::Mutex::Lock lock(mutex_);
      if (is_init_)
      {
        TBSYS_LOG(INFO, "tfsclient already initialized");
      }
      else
      {
        ret = NewClientManager::get_instance().initialize(packet_factory_, packet_streamer_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize NewClientManager fail, must exit, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (NULL != ns_addr)
        {
          TfsSession* session = session_pool_->get(ns_addr, cache_time, cache_items);
          if (NULL != session)
          {
            default_session_ = session;
          }
          else
          {
            // invalid ns addr
            ret = EXIT_PARAMETER_ERROR;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        is_init_ = true;
        ClientConfig::cache_time_ = cache_time;
        ClientConfig::cache_items_ = cache_items;
      }

      return ret;
    }

    int TfsClientImplV2::destroy()
    {
      tbutil::Mutex::Lock lock(mutex_);
      timer_->destroy();
      if (is_init_)
      {
        NewClientManager::get_instance().destroy();
        is_init_ = false;
      }
      return TFS_SUCCESS;
    }

    void TfsClientImplV2::set_log_level(const char* level)
    {
      TBSYS_LOG(INFO, "set log level: %s", level);
      TBSYS_LOGGER.setLogLevel(level);
    }

    void TfsClientImplV2::set_log_file(const char* file)
    {
      TBSYS_LOG(INFO, "set log file: %s", file);
      TBSYS_LOGGER.setFileName(file);
    }

    int TfsClientImplV2::get_session(const char* ns_addr, TfsSession*& session)
    {
      int ret = TFS_SUCCESS;
      if (NULL == ns_addr)
      {
        if (NULL == default_session_)
        {
          ret = EXIT_PARAMETER_ERROR;
        }
        else
        {
          session = default_session_;
        }
      }
      else
      {
        session = session_pool_->get(ns_addr);
        if (NULL == session)
        {
          // invalid ns addr
          ret = EXIT_PARAMETER_ERROR;
        }
      }

      return ret;
    }

    int TfsClientImplV2::open(const char* file_name,
        const char* suffix, const char* ns_addr, const int mode)
    {
      int ret_fd = EXIT_INVALIDFD_ERROR;
      int ret = TFS_SUCCESS;
      if (!is_init_)
      {
        ret = EXIT_NOT_INIT_ERROR;
        TBSYS_LOG(ERROR, "tfs client not init");
      }
      else if ((ret_fd = get_fd()) <= 0)
      {
        TBSYS_LOG(ERROR, "can not get fd. ret: %d", ret_fd);
      }
      else
      {
        TfsFile* tfs_file = NULL;
        TfsSession* session = NULL;
        ret = get_session(ns_addr, session);
        if (TFS_SUCCESS == ret)
        {
          tfs_file = new TfsFile();
          tfs_file->set_session(session);
          ret = tfs_file->open(file_name, suffix, mode);
        }

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "open tfsfile fail, filename: %s, suffix: %s, mode: %d, ret: %d",
              file_name, suffix, mode, ret);
        }
        else if ((ret = insert_file(ret_fd, tfs_file)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add fd fail: %d", ret_fd);
        }

        if (ret != TFS_SUCCESS)
        {
          ret_fd = (ret < 0) ? ret : EXIT_INVALIDFD_ERROR;
          tbsys::gDelete(tfs_file);
        }
      }

      return ret_fd;
    }

    int TfsClientImplV2::open(const uint64_t block_id,
        const uint64_t file_id, const char* ns_addr, const int mode)
    {
      int ret_fd = EXIT_INVALIDFD_ERROR;
      int ret = TFS_SUCCESS;
      if (!is_init_)
      {
        ret = EXIT_NOT_INIT_ERROR;
        TBSYS_LOG(ERROR, "tfs client not init");
      }
      else if ((ret_fd = get_fd()) <= 0)
      {
        TBSYS_LOG(ERROR, "can not get fd. ret: %d", ret_fd);
      }
      else
      {
        TfsFile* tfs_file = NULL;
        TfsSession* session = NULL;
        ret = get_session(ns_addr, session);
        if (TFS_SUCCESS == ret)
        {
          tfs_file = new TfsFile();
          tfs_file->set_session(session);
          ret = tfs_file->open(block_id, file_id, mode);
        }

        if (ret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "open tfsfile fail, blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, mode: %d, ret: %d",
              block_id, file_id, mode, ret);
        }
        else if ((ret = insert_file(ret_fd, tfs_file)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add fd fail: %d", ret_fd);
        }

        if (ret != TFS_SUCCESS)
        {
          ret_fd = (ret < 0) ? ret : EXIT_INVALIDFD_ERROR;
          tbsys::gDelete(tfs_file);
        }
      }

      return ret_fd;
    }

    int64_t TfsClientImplV2::read(const int fd, void* buf, const int64_t count)
    {
      int64_t ret = TFS_SUCCESS;
      if ((fd < 0) || (NULL == buf) || (count < 0))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->read(buf, count);
          tfs_file->get_session()->update_stat(ST_READ, ret > 0);
        }
      }
      return ret;
    }

    int64_t TfsClientImplV2::pread(const int fd, void* buf, const int64_t count, const int64_t offset)
    {
      int64_t ret = TFS_SUCCESS;
      if ((fd < 0) || (NULL == buf) || (count < 0))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->lseek(offset, T_SEEK_SET);
          if (TFS_SUCCESS == ret)
          {
            ret = tfs_file->read(buf, count);
          }
          tfs_file->get_session()->update_stat(ST_READ, ret > 0);
        }
      }
      return ret;
    }

    int64_t TfsClientImplV2::readv2(const int fd, void* buf, const int64_t count, common::TfsFileStat* file_info)
    {
      int64_t ret = TFS_SUCCESS;
      if ((fd < 0) || (NULL == buf) || (count < 0) || (NULL == file_info))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          tfs_file->set_option_flag(READ_DATA_OPTION_WITH_FINFO);
          ret = tfs_file->read(buf, count, file_info);
          tfs_file->get_session()->update_stat(ST_READ, ret > 0);
        }
      }
      return ret;
    }

    int64_t TfsClientImplV2::write(const int fd, const void* buf, const int64_t count)
    {
      int64_t ret = TFS_SUCCESS;
      if ((fd < 0) || (NULL == buf) || (count < 0))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->write(buf, count);
        }
      }
      return ret;
    }

    int TfsClientImplV2::fstat(const int fd, TfsFileStat* buf)
    {
      int ret = TFS_SUCCESS;
      if ((fd < 0) || (NULL == buf))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->stat(*buf);
          tfs_file->get_session()->update_stat(ST_STAT, TFS_SUCCESS == ret);
        }
      }

      return ret;
    }

    int TfsClientImplV2::close(const int fd,
        char* ret_tfs_name, const int32_t ret_tfs_name_len, const int32_t status)
    {
      int ret = TFS_SUCCESS;
      if (fd < 0)
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->close(status);
          tfs_file->get_session()->update_stat(ST_WRITE, ret > 0);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "tfs close failed. fd: %d, ret: %d", fd, ret);
          }
          else if (NULL != ret_tfs_name)
          {
            if (ret_tfs_name_len < TFS_FILE_LEN_V2)
            {
              TBSYS_LOG(ERROR, "name buffer length less: %d < %d", ret_tfs_name_len, TFS_FILE_LEN_V2);
              ret = TFS_ERROR;
            }
            else
            {
              memcpy(ret_tfs_name, tfs_file->get_file_name(), TFS_FILE_LEN_V2);
            }
          }
          erase_file(fd);
        }
      }

      return ret;
    }

    int TfsClientImplV2::unlink(int64_t& file_size, const int fd, const common::TfsUnlinkType action)
    {
      int ret = TFS_SUCCESS;
      if (fd < 0)
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          ret = tfs_file->unlink(action, file_size);
          tfs_file->get_session()->update_stat(ST_UNLINK, TFS_SUCCESS == ret);
        }
      }
      return ret;
    }

    int TfsClientImplV2::set_option_flag(const int fd, const int option_flag)
    {
      int ret = TFS_SUCCESS;
      if (fd < 0)
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        TfsFile* tfs_file = get_file(fd);
        if (NULL == tfs_file)
        {
          ret = EXIT_INVALIDFD_ERROR;
        }
        else
        {
          tfs_file->set_option_flag(option_flag);
        }
      }
      return ret;
    }

    int TfsClientImplV2::stat_file(common::TfsFileStat* file_stat, const char* file_name, const char* suffix,
        const common::TfsStatType stat_type, const char* ns_addr)
    {
      int fd = open(file_name, suffix, ns_addr, T_READ);
      int ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        set_option_flag(fd, stat_type);
        ret = fstat(fd, file_stat);
        close(fd);
      }
      return ret;
    }

    int64_t TfsClientImplV2::save_file(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* local_file, const int32_t mode, const char* suffix, const char* ns_addr)
    {
      int64_t ret = TFS_SUCCESS;
      if (NULL == local_file)
      {
        ret = EXIT_PARAMETER_ERROR;
        TBSYS_LOG(WARN, "invalid parmater");
      }
      else
      {
        for (int i = 0; i < DEFAULT_RETRY_TIMES; i++)
        {
          ret = save_file_ex(ret_tfs_name, ret_tfs_name_len, local_file, mode, NULL, suffix, ns_addr);
          if (ret >= 0)
          {
            break;
          }
        }
      }
      return ret;
    }

    int64_t TfsClientImplV2::save_file_update(const char* local_file, const int32_t mode,
        const char* tfs_name, const char* suffix, const char* ns_addr)
    {
      int64_t ret = TFS_SUCCESS;
      if ((NULL == local_file) || (NULL == tfs_name) ||
          (static_cast<int32_t>(strlen(tfs_name)) < FILE_NAME_LEN))
      {
        ret = EXIT_PARAMETER_ERROR;
        TBSYS_LOG(WARN, "invalid parmater");
      }
      else
      {
        ret = save_file_ex(NULL, 0, local_file, mode, tfs_name, suffix, ns_addr);
      }
      return ret;
    }

    int64_t TfsClientImplV2::save_file_ex(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* local_file, const int32_t mode, const char* tfs_name, const char* suffix, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      int local_fd = 0;  // local file desp
      int fd = 0;        // tfs file desp
      int64_t done = 0;

      local_fd = ::open(local_file, O_RDONLY);
      if (local_fd < 0)
      {
        ret = -errno;
        TBSYS_LOG(ERROR, "open local file fail. %s", strerror(errno));
      }
      else
      {
        fd = open(tfs_name, suffix, ns_addr, T_WRITE | mode);
        if (fd < 0)
        {
          ret = EXIT_INVALIDFD_ERROR;
          TBSYS_LOG(ERROR, "open new tfs file fail.");
        }
        else
        {
          char buf[MAX_READ_SIZE];
          int rlen = 0;
          int wlen = 0;
          while (1)
          {
            rlen = ::read(local_fd, buf, MAX_READ_SIZE);
            if (rlen < 0)
            {
              ret = -errno;
            }
            else if (rlen > 0)
            {
              wlen = write(fd, buf, rlen);
              if (wlen < 0)
              {
                ret = wlen;
              }
              else
              {
                done += wlen;
              }
            }

            if ((rlen < MAX_READ_SIZE) || (wlen < 0))  // error happens or read the end
            {
              break;
            }
          }

          ret = close(fd, ret_tfs_name, ret_tfs_name_len);
        }

        ::close(local_fd);
      }

      return ret < 0 ? ret : done;
    }

    int64_t TfsClientImplV2::save_buf(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* buf, const int64_t buf_len, const int32_t mode, const char* suffix, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      if ((NULL == buf) || (buf_len <= 0))
      {
        ret = EXIT_PARAMETER_ERROR;
        TBSYS_LOG(WARN, "invalid parmater");
      }
      else
      {
        for (int i = 0; i < DEFAULT_RETRY_TIMES; i++)
        {
          ret = save_buf_ex(ret_tfs_name, ret_tfs_name_len, buf, buf_len, mode, NULL, suffix, ns_addr);
          if (ret >= 0)
          {
            break;
          }
        }
      }
      return ret;
    }

    int64_t TfsClientImplV2::save_buf_update(const char* buf, const int64_t buf_len, const int32_t mode,
        const char* tfs_name, const char* suffix, const char* ns_addr)
    {
      int64_t ret = TFS_SUCCESS;
      if ((NULL == buf) || (buf_len <= 0) || (NULL == tfs_name) ||
          (static_cast<int32_t>(strlen(tfs_name)) < FILE_NAME_LEN))
      {
        ret = EXIT_PARAMETER_ERROR;
        TBSYS_LOG(WARN, "invalid parmater");
      }
      else
      {
        ret = save_buf_ex(NULL, 0, buf, buf_len, mode, tfs_name, suffix, ns_addr);
      }
      return ret;
    }

    int64_t TfsClientImplV2::save_buf_ex(char* ret_tfs_name, const int32_t ret_tfs_name_len,
        const char* buf, const int64_t buf_len, const int32_t mode,
        const char* tfs_name, const char* suffix, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      int fd = 0;        // tfs file desp
      int64_t done = 0;

      fd = open(tfs_name, suffix, ns_addr, T_WRITE | mode);
      if (fd < 0)
      {
        ret = EXIT_INVALIDFD_ERROR;
        TBSYS_LOG(ERROR, "open new tfs file fail.");
      }
      else
      {
        while (done < buf_len)
        {
          int len = std::min(buf_len - done, static_cast<int64_t>(MAX_READ_SIZE));
          int wlen = write(fd, buf + done, len);
          if (wlen < 0)
          {
            ret = wlen;
            break;
          }
          else
          {
            done += wlen;
          }
        }
      }

      ret = close(fd, ret_tfs_name, ret_tfs_name_len);

      return ret < 0 ? ret : done;
    }

    int TfsClientImplV2::fetch_file(const char* local_file, const char* file_name, const char* suffix,const common::ReadDataOptionFlag read_flag, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      int local_fd = 0;  // local file desp
      int fd = 0;        // tfs file desp

      local_fd = ::open(local_file, O_CREAT | O_RDWR | O_TRUNC, 0644);
      if (local_fd < 0)
      {
        ret = -errno;
        TBSYS_LOG(ERROR, "open local file fail. %s", strerror(errno));
      }
      else
      {
        fd = open(file_name, suffix, ns_addr, T_READ);
        if (fd < 0)
        {
          ret = EXIT_INVALIDFD_ERROR;
          TBSYS_LOG(ERROR, "open tfs file %s fail", file_name);
        }
        else
        {
          set_option_flag(fd, read_flag);
          char buf[MAX_READ_SIZE];
          int rlen = 0;
          int wlen = 0;
          while (1)
          {
            rlen = read(fd, buf, MAX_READ_SIZE);
            if (rlen < 0)
            {
              ret = rlen;
            }
            else if (rlen > 0)
            {
              ret = ::write(local_fd, buf, rlen);
              if (ret < 0)
              {
                wlen = -errno;
              }
            }

            if ((rlen < MAX_READ_SIZE) || (wlen < 0))  // error happens or read the end
            {
              break;
            }
          }

          close(fd);
        }

        ::close(local_fd);
      }

      return ret < 0 ? ret : TFS_SUCCESS;
    }

    int TfsClientImplV2::unlink(int64_t& file_size, const char* file_name, const char* suffix,
        const common::TfsUnlinkType action, const char* ns_addr, const int32_t flag)
    {
      int fd = open(file_name, suffix, ns_addr, T_UNLINK);
      int ret = fd < 0 ? fd : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = set_option_flag(fd, flag);
        if (TFS_SUCCESS == ret)
        {
          ret = unlink(file_size, fd, action);
        }
        close(fd);
      }
      return ret;
    }

    uint64_t TfsClientImplV2::get_server_id()
    {
      uint64_t server_id = 0;
      if (NULL != default_session_)
      {
        server_id = Func::get_host_ip(default_session_->get_ns_addr().c_str());
      }
      return server_id;
    }

    int32_t TfsClientImplV2::get_cluster_id()
    {
      uint32_t cluster_id = 0;
      if (NULL != default_session_)
      {
        cluster_id = default_session_->get_cluster_id();
      }
      return cluster_id_;
    }

    void TfsClientImplV2::set_use_local_cache(const bool enable)
    {
      if (enable)
      {
        ClientConfig::use_cache_ |= USE_CACHE_FLAG_LOCAL;
      }
      else
      {
        ClientConfig::use_cache_ &= ~USE_CACHE_FLAG_LOCAL;
      }
    }

    void TfsClientImplV2::set_use_remote_cache(const bool enable)
    {
      if (enable)
      {
        ClientConfig::use_cache_ |= USE_CACHE_FLAG_REMOTE;
      }
      else
      {
        ClientConfig::use_cache_ &= ~USE_CACHE_FLAG_REMOTE;
      }
    }

#ifdef WITH_TAIR_CACHE
      void TfsClientImplV2::set_remote_cache_info(const char* remote_cache_master_addr,
          const char* remote_cache_slave_addr,
          const char* remote_cache_group_name,
          const int32_t area)
      {
        ClientConfig::remote_cache_master_addr_ = remote_cache_master_addr;
        ClientConfig::remote_cache_slave_addr_ = remote_cache_slave_addr;
        ClientConfig::remote_cache_group_name_ = remote_cache_group_name;
        ClientConfig::remote_cache_area_ = area;
      }
#endif

    TfsFile* TfsClientImplV2::get_file(const int fd)
    {
      tbutil::Mutex::Lock lock(mutex_);
      TfsFile* tfs_file = NULL;
      FILE_MAP::iterator it = tfs_file_map_.find(fd);
      if (tfs_file_map_.end() == it)
      {
        TBSYS_LOG(ERROR, "invaild fd, ret: %d", fd);
      }
      else
      {
        tfs_file = it->second;
      }
      return tfs_file;
    }

    int TfsClientImplV2::get_fd()
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

    int TfsClientImplV2::insert_file(const int fd, TfsFile* tfs_file)
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

    int TfsClientImplV2::erase_file(const int fd)
    {
      tbutil::Mutex::Lock lock(mutex_);
      int ret = TFS_SUCCESS;
      FILE_MAP::iterator it = tfs_file_map_.find(fd);
      if (tfs_file_map_.end() == it)
      {
        ret = EXIT_INVALIDFD_ERROR;
        TBSYS_LOG(ERROR, "invaild fd: %d", fd);
      }
      else
      {
        tbsys::gDelete(it->second);
        tfs_file_map_.erase(it);
      }
      return ret;
    }

  }
}
