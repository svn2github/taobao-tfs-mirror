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
    TfsClientImplV2::TfsClientImplV2() : is_init_(false), fd_(0),
    ns_addr_(0), cluster_id_(0), packet_factory_(NULL), packet_streamer_(NULL)
    {
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

      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(packet_streamer_);
    }

    int TfsClientImplV2::initialize(const char* ns_addr)
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
      else
      {
        ns_addr_ = ipport_to_addr(ns_addr);
        if (0 != ns_addr_)
        {
          ret = initialize_cluster_id();
        }

        if (TFS_SUCCESS == ret)
        {
          is_init_ = true;
        }
      }

      return ret;
    }

    int TfsClientImplV2::destroy()
    {
      tbutil::Mutex::Lock lock(mutex_);
      if (is_init_)
      {
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

    int TfsClientImplV2::open(const char* file_name, const char* suffix, const char* ns_addr, const int mode)
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
        uint64_t real_nsaddr = ipport_to_addr(ns_addr);
        real_nsaddr = (0 != real_nsaddr) ? real_nsaddr : ns_addr_;
        if (0 == real_nsaddr)
        {
          ret = EXIT_PARAMETER_ERROR;
        }
        else
        {
          tfs_file = new TfsFile(real_nsaddr, cluster_id_);
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
          ret_fd = (ret < 0) ? ret : EXIT_INVALIDFD_ERROR; // return true error code except TFS_ERROR
          tbsys::gDelete(tfs_file);
        }
      }

      return ret_fd;
    }

    int TfsClientImplV2::open(const uint64_t block_id, const uint64_t file_id, const char* ns_addr, const int mode)
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
        uint64_t real_nsaddr = ipport_to_addr(ns_addr);
        real_nsaddr = (0 != real_nsaddr) ? real_nsaddr : ns_addr_;
        if (0 == real_nsaddr)
        {
          ret = EXIT_PARAMETER_ERROR;
        }
        else
        {
          tfs_file = new TfsFile(real_nsaddr, cluster_id_);
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
          ret_fd = (ret < 0) ? ret : EXIT_INVALIDFD_ERROR; // return true error code except TFS_ERROR
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
      int ret = TFS_SUCCESS;
      int fd = open(file_name, suffix, ns_addr, T_READ);
      if (fd < 0)
      {
        ret = EXIT_INVALIDFD_ERROR;
      }
      else
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
        ret = save_file_ex(ret_tfs_name, ret_tfs_name_len, local_file, mode, NULL, suffix, ns_addr);
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
        ret = save_buf_ex(ret_tfs_name, ret_tfs_name_len, buf, buf_len, mode, NULL, suffix, ns_addr);
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
        const common::TfsUnlinkType action, const char* ns_addr)
    {
      int ret = TFS_SUCCESS;
      int fd = open(file_name, suffix, ns_addr, T_UNLINK);
      if (fd < 0)
      {
        ret = EXIT_INVALIDFD_ERROR;
      }
      else
      {
        ret = unlink(file_size, fd, action);
        close(fd);
      }
      return ret;
    }

    int64_t TfsClientImplV2::get_server_id()
    {
      return ns_addr_;
    }

    int32_t TfsClientImplV2::get_cluster_id()
    {
      return cluster_id_;
    }

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

    uint64_t TfsClientImplV2::ipport_to_addr(const char* addr)
    {
      uint64_t result = 0;
      if (NULL != addr)
      {
        vector<string> fields;
        Func::split_string(addr, ':', fields);
        if (2 == fields.size())
        {
          result = Func::str_to_addr(fields[0].c_str(), atoi(fields[1].c_str()));
        }
      }
      return result;
    }

    int TfsClientImplV2::initialize_cluster_id()
    {
      ClientCmdMessage cc_message;
      cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
      cc_message.set_value3(20);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = send_msg_to_server(ns_addr_, client, &cc_message, rsp);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get cluster id from ns fail, ret: %d", ret);
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
        //ugly use error msg
        if (status_msg->get_status() == STATUS_MESSAGE_OK &&
            strlen(status_msg->get_error()) > 0)
        {
          char cluster_id = static_cast<char> (atoi(status_msg->get_error()));
          if (isdigit(cluster_id) || isalpha(cluster_id))
          {
            cluster_id_ = cluster_id - '0';
            TBSYS_LOG(INFO, "get cluster id from ns %s success. cluster id: %d",
                tbsys::CNetUtil::addrToString(ns_addr_).c_str(), cluster_id_);
          }
          else
          {
            TBSYS_LOG(ERROR, "get cluster id from ns %s fail. cluster id: %d",
                tbsys::CNetUtil::addrToString(ns_addr_).c_str(), cluster_id_);
            ret = TFS_ERROR;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster id from ns %s failed, msg type error. type: %d",
            tbsys::CNetUtil::addrToString(ns_addr_).c_str(), rsp->getPCode());
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

  }
}
