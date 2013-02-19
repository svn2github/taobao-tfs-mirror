/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include "common/func.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "tfs_file.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace clientv2
  {
    TfsFile::TfsFile(const uint64_t ns_addr, const int32_t cluster_id):
      ns_addr_(ns_addr)
    {
      fsname_.set_cluster_id(cluster_id);
    }

    TfsFile::~TfsFile()
    {
    }

    int TfsFile::open(const char* file_name, const char* suffix, const int32_t mode)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      file_.mode_ = mode;
      if ((mode & T_READ) || (mode & T_STAT))
      {
        file_.mode_ = T_READ;
        if ((NULL == file_name) || (file_name[0] == '\0'))
        {
          ret = EXIT_PARAMETER_ERROR;
        }
      }
      else if ((mode & T_CREATE) || (mode & T_UNLINK))
      {
        file_.mode_ |= T_WRITE;
      }

      if (TFS_SUCCESS == ret)
      {
        fsname_.set_name(file_name, suffix);
        ret = fsname_.is_valid() ? TFS_SUCCESS : EXIT_INVALID_FILE_NAME;
        if (TFS_SUCCESS == ret)
        {
          ret = do_open();
        }
      }

      return ret;
    }

    int64_t TfsFile::lseek(const int64_t offset, const int whence)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else if (0 == (file_.mode_ & (T_READ | T_WRITE)))
      {
        ret = EXIT_NOT_PERM_OPER;
      }
      else
      {
        switch (whence)
        {
          case T_SEEK_SET:
            file_.offset_ = (offset < 0) ? 0: offset;
            break;
          case T_SEEK_CUR:
            file_.offset_ = (file_.offset_ + offset < 0) ? 0 : (file_.offset_ + offset);
            break;
          default:
            TBSYS_LOG(ERROR, "unknown seek flag: %d", whence);
            break;
        }
      }

      return file_.offset_;
    }

    int64_t TfsFile::stat(common::TfsFileStat& file_stat)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      int ret = TFS_SUCCESS;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else
      {
        int32_t retry = file_.get_retry_time();
        while (retry--)
        {
          ret = do_stat(file_stat);
          if (TFS_SUCCESS == ret)
          {
            break;
          }
        }
      }

      return ret;
    }

    int64_t TfsFile::read(void* buf, const int64_t count, common::TfsFileStat* file_stat)
    {
      int ret = TFS_SUCCESS;
      int64_t done = 0;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else
      {
        int64_t read_len = 0;
        int64_t real_read_len = 0;
        while (done < count)
        {
          int32_t retry = file_.get_retry_time();
          read_len = ((count - done) < MAX_READ_SIZE) ? (count - done) : MAX_READ_SIZE;
          while (retry--)
          {
            ret = do_read((char*)buf + done, read_len, real_read_len, file_stat);
            if (TFS_SUCCESS != ret)
            {
              file_.set_next_read_ds();
            }
            else
            {
              // success, no need retry
              break;
            }
          }

          if (TFS_SUCCESS == ret)
          {
            done += real_read_len;
            if (real_read_len < read_len)
            {
              // has reach to the end
              break;
            }
          }
          else
          {
            // error after retry, break
            break;
          }
        }
      }

      return done > 0 ? done : ret;
    }

    int64_t TfsFile::write(const void* buf, const int64_t count)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      int64_t done = 0;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else
      {
        int64_t write_len = 0;
        while (done < count)
        {
          write_len = ((count - done) < MAX_READ_SIZE) ? (count - done) : MAX_READ_SIZE;
          ret = do_write((char*)buf + done, write_len);
          if (TFS_SUCCESS != ret)
          {
            break;
          }
          file_.offset_ += write_len;
          done += write_len;
        }
      }

      return done > 0 ? done : ret;
    }

    int TfsFile::close()
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else if(!(file_.mode_ & T_READ) && !(file_.mode_ & T_UNLINK))
      {
        // only create & update need real close to ds
        ret = do_close();
      }

      return ret;
    }

    int TfsFile::unlink(const common::TfsUnlinkType action, int64_t& file_size)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      if (TFS_FILE_OPEN_YES != file_.status_)
      {
        ret = EXIT_NOT_OPEN_ERROR;
      }
      else
      {
        ret = do_unlink(action, file_size);
      }

      return ret;
    }

    void TfsFile::set_option_flag(const int32_t flag)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      file_.opt_flag_ |= flag;
    }

    const char* TfsFile::get_file_name()
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      return fsname_.get_name();
    }

    void TfsFile::wrap_file_info(TfsFileStat& file_stat, const FileInfoV2& file_info)
    {
      file_stat.file_id_ = file_info.id_;
      file_stat.offset_ = file_info.offset_;
      file_stat.size_ = file_info.size_ - FILEINFO_EXT_SIZE; // first version
      file_stat.usize_ = file_info.size_ - FILEINFO_EXT_SIZE;
      file_stat.modify_time_ = file_info.modify_time_;
      file_stat.create_time_ = file_info.create_time_;
      file_stat.flag_ = file_info.status_;
      file_stat.crc_ = file_info.crc_;
    }

    int TfsFile::do_open()
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
        GetBlockInfoMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_mode(file_.mode_);
        ret = send_msg_to_server(ns_addr_, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (GET_BLOCK_INFO_RESP_MESSAGE_V2 != resp_msg->getPCode())
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            TBSYS_LOG(WARN, "open file fail. status: %d", ret);
          }
        }
        else
        {
          GetBlockInfoRespMessageV2* response = dynamic_cast<GetBlockInfoRespMessageV2*>(resp_msg);
          BlockMeta& meta = response->get_block_meta();
          fsname_.set_block_id(meta.block_id_);
          file_.version_ = meta.version_;
          TBSYS_LOG(DEBUG, "blockid: %"PRI64_PREFIX"u, replicas: %d, version: %d, family_id: %"PRI64_PREFIX"d",
              meta.block_id_, meta.size_, meta.version_, meta.family_info_.family_id_);
          for (int32_t i = 0; i < meta.size_; i++)
          {
            file_.ds_.push_back(meta.ds_[i]);
          }

          if (INVALID_FAMILY_ID != meta.family_info_.family_id_)
          {
            file_.family_info_ = meta.family_info_;
          }

          if (file_.is_valid())
          {
            file_.status_ = TFS_FILE_OPEN_YES;
          }
        }
      }

      return ret;
    }

    int TfsFile::do_stat(TfsFileStat& file_stat)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
        StatFileMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_attach_block_id(fsname_.get_block_id());
        msg.set_file_id(fsname_.get_file_id());
        msg.set_flag(file_.opt_flag_);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        ret = send_msg_to_server(file_.get_read_ds(), client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (STAT_FILE_RESP_MESSAGE_V2 != resp_msg->getPCode())
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            TBSYS_LOG(WARN, "stat file fail. status: %d", ret);
          }
        }
        else
        {
          StatFileRespMessageV2* response = dynamic_cast<StatFileRespMessageV2*>(resp_msg);
          const FileInfoV2& file_info = response->get_file_info();
          wrap_file_info(file_stat, file_info);
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::do_read(char* buf, const int64_t count, int64_t& read_len,
        common::TfsFileStat* file_stat)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
         // in the first version we simply ignore extend file info
        if (0 == file_.offset_)
        {
          file_.offset_ += FILEINFO_EXT_SIZE;
        }

        // read or readv2
        int32_t real_flag = file_.opt_flag_;
        if (NULL != file_stat)
        {
          real_flag |= READ_DATA_OPTION_WITH_FINFO;
        }

        ReadFileMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_attach_block_id(fsname_.get_block_id());
        msg.set_file_id(fsname_.get_file_id());
        msg.set_offset(file_.offset_);
        msg.set_length(count);
        msg.set_flag(real_flag);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        TBSYS_LOG(DEBUG, "family id: %"PRI64_PREFIX"d", file_.family_info_.family_id_);
        ret = send_msg_to_server(file_.get_read_ds(), client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (READ_FILE_RESP_MESSAGE_V2 != resp_msg->getPCode())
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            TBSYS_LOG(WARN, "read file fail. status: %d", ret);
          }
        }
        else
        {
          ReadFileRespMessageV2* response = dynamic_cast<ReadFileRespMessageV2*>(resp_msg);
          read_len = response->get_length();
          memcpy(buf, response->get_data(), read_len);
          file_.offset_ += read_len;
          if (NULL != file_stat)
          {
            wrap_file_info(*file_stat, response->get_file_info());
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::do_write(const char* buf, int64_t count)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
        WriteFileMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_attach_block_id(fsname_.get_block_id());
        msg.set_file_id(fsname_.get_file_id());
        msg.set_offset(file_.offset_);
        msg.set_length(count);
        msg.set_lease_id(file_.lease_id_);
        msg.set_master_id(file_.get_write_ds());
        msg.set_version(file_.version_);
        msg.set_flag(file_.opt_flag_);
        msg.set_ds(file_.ds_);
        msg.set_data(buf);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        ret = send_msg_to_server(file_.get_write_ds(), client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (WRITE_FILE_RESP_MESSAGE_V2 != resp_msg->getPCode())
        {
          if (STATUS_MESSAGE != resp_msg->getPCode())
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          else
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
            ret = smsg->get_status();
            TBSYS_LOG(WARN, "write file fail. status: %d %s", ret, smsg->get_error());
          }
        }
        else
        {
          WriteFileRespMessageV2* response = dynamic_cast<WriteFileRespMessageV2*>(resp_msg);
          file_.lease_id_ = response->get_lease_id();
          fsname_.set_file_id(response->get_file_id());
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::do_close()
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
        CloseFileMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_attach_block_id(fsname_.get_block_id());
        msg.set_file_id(fsname_.get_file_id());
        msg.set_lease_id(file_.lease_id_);
        msg.set_master_id(file_.get_write_ds());
        msg.set_ds(file_.ds_);
        msg.set_crc(file_.crc_);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        ret = send_msg_to_server(file_.get_write_ds(), client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (STATUS_MESSAGE != resp_msg->getPCode())
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
        else
        {
          StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
          ret = smsg->get_status();
          TBSYS_LOG(DEBUG, "close file status: %d %s", ret, smsg->get_error());
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::do_unlink(const int32_t action, int64_t& file_size)
    {
      int ret = TFS_SUCCESS;
      tbnet::Packet* resp_msg = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        TBSYS_LOG(WARN, "create new client fail.");
      }
      else
      {
        UnlinkFileMessageV2 msg;
        msg.set_block_id(fsname_.get_block_id());
        msg.set_attach_block_id(fsname_.get_block_id());
        msg.set_file_id(fsname_.get_file_id());
        msg.set_lease_id(file_.lease_id_);
        msg.set_master_id(file_.get_write_ds());
        msg.set_ds(file_.ds_);
        msg.set_action(action);
        msg.set_version(file_.version_);
        msg.set_flag(file_.opt_flag_);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        ret = send_msg_to_server(file_.get_write_ds(), client, &msg, resp_msg);
      }

      if (TFS_SUCCESS == ret)
      {
        if (STATUS_MESSAGE != resp_msg->getPCode())
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
        else
        {
          StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
          ret = smsg->get_status();
          TBSYS_LOG(DEBUG, "unlink file status: %d %s", ret, smsg->get_error());
          file_size = atol(smsg->get_error());
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }
  }
}

