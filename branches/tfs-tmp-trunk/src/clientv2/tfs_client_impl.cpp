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
#include "tfs_client_impl.h"
#include "tfs_file.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace clientv2
  {
    TfsClientImpl::TfsClientImpl() : is_init_(false), fd_(0),
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

    int TfsClientImpl::initialize(const char* ns_addr)
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
      else if ((NULL == ns_addr) || ('0' == ns_addr[0]))
      {
        TBSYS_LOG(ERROR, "tfsclient initialize to ns %s failed. must exit", ns_addr);
        ret = TFS_ERROR;
      }
      else
      {
        vector<string> fields;
        Func::split_string(ns_addr, ':', fields);
        if (2 != fields.size())
        {
          TBSYS_LOG(ERROR, "invalid ns ip");
          ret = TFS_ERROR;
        }
        else
        {
          ns_addr_ = Func::str_to_addr(fields[0].c_str(), atoi(fields[1].c_str()));
          ret = initialize_cluster_id();
          if (TFS_SUCCESS == ret)
          {
            is_init_ = true;
          }
        }
      }

      return ret;
    }

    int TfsClientImpl::destroy()
    {
      tbutil::Mutex::Lock lock(mutex_);
      if (is_init_)
      {
        is_init_ = false;
      }
      return TFS_SUCCESS;
    }

    int TfsClientImpl::open(const char* file_name, const char* suffix, const int mode)
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
        TfsFile* tfs_file = new TfsFile(ns_addr_, cluster_id_);
        ret = tfs_file->open(file_name, suffix, mode);

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

    int64_t TfsClientImpl::read(const int fd, void* buf, const int64_t count)
    {
      int64_t ret = EXIT_INVALIDFD_ERROR;
      TfsFile* tfs_file = get_file(fd);
      if (NULL != tfs_file)
      {
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
        ret = tfs_file->lseek(offset, whence);
      }
      return ret;
    }

    int TfsClientImpl::fstat(const int fd, TfsFileStat* buf)
    {
      int ret = EXIT_INVALIDFD_ERROR;
      TfsFile* tfs_file = get_file(fd);
      if (NULL != tfs_file)
      {
        ret = tfs_file->stat(*buf);
      }
      return ret;
    }

    int TfsClientImpl::close(const int fd, char* ret_tfs_name, const int32_t ret_tfs_name_len)
    {
      int ret = EXIT_INVALIDFD_ERROR;
      TfsFile* tfs_file = get_file(fd);
      if (NULL != tfs_file)
      {
        ret = tfs_file->close();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "tfs close failed. fd: %d, ret: %d", fd, ret);
        }
        else if (NULL != ret_tfs_name)
        {
          if (ret_tfs_name_len < TFS_FILE_LEN)
          {
            TBSYS_LOG(ERROR, "name buffer length less: %d < %d", ret_tfs_name_len, TFS_FILE_LEN);
            ret = TFS_ERROR;
          }
          else
          {
            memcpy(ret_tfs_name, tfs_file->get_file_name(), TFS_FILE_LEN);
          }
        }
        erase_file(fd);
      }

      return ret;
    }

    int TfsClientImpl::unlink(int64_t& file_size, const int fd, const common::TfsUnlinkType action)
    {
      int ret = EXIT_INVALIDFD_ERROR;
      TfsFile* tfs_file = get_file(fd);
      if (NULL != tfs_file)
      {
        ret = tfs_file->unlink(action, file_size);
      }
      return ret;
    }

    int TfsClientImpl::set_option_flag(const int fd, const int option_flag)
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

    TfsFile* TfsClientImpl::get_file(const int fd)
    {
      tbutil::Mutex::Lock lock(mutex_);
      FILE_MAP::iterator it = tfs_file_map_.find(fd);
      if (tfs_file_map_.end() == it)
      {
        TBSYS_LOG(ERROR, "invaild fd, ret: %d", fd);
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

    int TfsClientImpl::initialize_cluster_id()
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
            TBSYS_LOG(INFO, "get cluster id from nameserver success. cluster id: %d", cluster_id_);
          }
          else
          {
            TBSYS_LOG(ERROR, "get cluster id from nameserver fail. cluster id: %c", cluster_id);
            ret = TFS_ERROR;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster id from ns failed, msg type error. type: %d", rsp->getPCode());
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

  }
}
