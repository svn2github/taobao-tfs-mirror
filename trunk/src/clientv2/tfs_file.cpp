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
    File::File(): lease_id_(0), offset_(0), version_(-1),
    crc_(0), mode_(0), opt_flag_(0), read_index_(0), write_status_(WRITE_STATUS_OK),
    cache_hit_(CACHE_HIT_NONE)
    {
    }

    File::~File()
    {
    }

    bool File::has_family() const
    {
      return INVALID_FAMILY_ID != family_info_.family_id_;
    }

    bool File::check_read()
    {
      bool read_ok = false;
      if (ds_.size() > 0)
      {
        read_ok = true;
      }
      else if (has_family())
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info_.family_aid_info_);
        int32_t alive_data_num = family_info_.get_alive_data_num();
        int32_t alive_check_num = family_info_.get_alive_check_num();

        // if all data_num are alive, we should't got here
        // something must be wrong, let client retry to nameserver to get
        // correct information
        if ((alive_data_num + alive_check_num >= data_num) && (alive_data_num < data_num))
        {
          read_ok = true;
        }
      }

      return read_ok;
    }

    bool File::check_write()
    {
      return ds_.size() > 0;
    }

    int32_t File::get_read_retry_time() const
    {
      return ds_.size() > 0 ? ds_.size() : 1;
    }

    uint64_t File::get_write_ds() const
    {
      uint64_t server_id = 0;
      if (ds_.size() > 0)
      {
        server_id = ds_[0];
      }
      return server_id;
    }

    void File::set_read_index(const int32_t read_index)
    {
      if (ds_.size() > 0)
      {
        read_index_ = read_index;
        read_index_ %= ds_.size();
      }
   }

    void File::set_next_read_index()
    {
      if (ds_.size() > 0)
      {
        read_index_++;
        read_index_ %= ds_.size();
      }
    }

    uint64_t File::get_read_ds() const
    {
      uint64_t server_id = 0;
      if (ds_.size() > 0)
      {
        server_id = ds_[read_index_%ds_.size()];
      }
      else if(common::INVALID_FAMILY_ID != family_info_.family_id_)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_info_.family_aid_info_);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_info_.family_aid_info_);
        const int32_t member_num = data_num + check_num;

        // random choose an alive node to do degraded read
        if (member_num > 0)
        {
          int32_t target = rand() % member_num;
          for (int32_t i = 0; i < member_num; i++, target++)
          {
            target = target % member_num;
            if (common::INVALID_SERVER_ID != family_info_.members_[target].second)
            {
              server_id = family_info_.members_[target].second;
              break;
            }
          }
        }
      }
      return server_id;
    }

    TfsFile::TfsFile(): session_(NULL)
    {
    }

    TfsFile::~TfsFile()
    {
    }

    void TfsFile::set_session(TfsSession* session)
    {
      if (NULL != session)
      {
        session_ = session;
        fsname_.set_cluster_id(session->get_cluster_id());
      }
    }

    TfsSession* TfsFile::get_session()
    {
      return session_;
    }

    bool TfsFile::is_cache_hit() const
    {
      return file_.cache_hit_ != CACHE_HIT_NONE;
    }

    void TfsFile::transfer_mode(const int32_t mode, const bool create)
    {
      file_.mode_ = mode;
      if (mode & T_FORCE) // force read support
      {
        file_.opt_flag_ |= READ_DATA_OPTION_FLAG_FORCE;
      }

      if ((mode & T_READ) || (mode & T_STAT))
      {
        file_.mode_ = T_READ;
      }
      else if ((mode & T_WRITE) && !create)
      {
        file_.mode_ |= T_UPDATE;
      }
    }

    int TfsFile::open(const char* file_name, const char* suffix, const int32_t mode)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      transfer_mode(mode, NULL == file_name);
      if (((file_.mode_ & T_READ) || (file_.mode_ & T_UNLINK)) &&
        ((NULL == file_name) || (file_name[0] == '\0')))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        if (NULL != file_name)
        {
          fsname_.set_name(file_name, suffix, session_->get_cluster_id());
        }

        if (NULL != suffix)
        {
          fsname_.set_suffix(suffix);
        }

        ret = fsname_.is_valid() ? TFS_SUCCESS : EXIT_INVALID_FILE_NAME;
        if (TFS_SUCCESS == ret)
        {
          ret = open_ex();
        }
      }

      return ret;
    }

    int TfsFile::open(const uint64_t block_id, const uint64_t file_id, const int32_t mode)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      transfer_mode(mode, INVALID_BLOCK_ID == block_id);
      if (((file_.mode_ & T_READ) || (file_.mode_ & T_UNLINK)) &&
        ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        fsname_.set_block_id(block_id);
        fsname_.set_file_id(file_id);
        ret = open_ex();
      }

      return ret;
    }

    int64_t TfsFile::lseek(const int64_t offset, const int whence)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      if (0 == (file_.mode_ & T_READ))
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
      int64_t ret = TFS_SUCCESS;
      bool retry = true;
      while  (retry && TFS_SUCCESS == ret)
      {
        ret = stat_once(file_stat);
        TBSYS_LOG(DEBUG, "stat request, ret: %"PRI64_PREFIX"d", ret);
        retry = is_cache_hit();
        if (retry)
        {
          // cache hit && block moved to other dataserver
          retry = (EXIT_NO_LOGICBLOCK_ERROR == ret);
          if (retry)
          {
            ret = open_ex();
          }
        }
      }
      return ret;
    }

    int64_t TfsFile::read(void* buf, const int64_t count, common::TfsFileStat* file_stat)
    {
      int64_t ret = TFS_SUCCESS;
      bool retry = true;
      while (retry)
      {
        ret = read_once(buf, count, file_stat);
        retry = is_cache_hit();
        if (retry)
        {
          // cache hit && block moved to other dataserver
          retry = (EXIT_NO_LOGICBLOCK_ERROR == ret);
          if (retry)
          {
            open_ex(); // reopen file
          }
        }
        TBSYS_LOG(DEBUG, "read request, ret: %"PRI64_PREFIX"d", ret);
      }
      return ret;
    }

    int64_t TfsFile::stat_once(common::TfsFileStat& file_stat)
    {
      ScopedRWLock scoped_lock(rw_lock_, READ_LOCKER);
      int ret = TFS_SUCCESS;
      if (!file_.check_read())
      {
        ret = EXIT_READ_FILE_ERROR;
      }
      else if (file_.ds_.size() > 0)
      {
        file_.set_read_index(fsname_.get_file_id() % file_.ds_.size());
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t retry = file_.get_read_retry_time();
        while (retry--)
        {
          ret = stat_ex(file_stat);
          if (TFS_SUCCESS != ret)
          {
            file_.set_next_read_index();
          }
          else
          {
            break;
          }
        }
      }

      // check cache, if expired, remove it
      session_->expire_block_cache(fsname_.get_block_id(), file_.cache_hit_, ret);

      return ret;
    }

    int64_t TfsFile::read_once(void* buf, const int64_t count, common::TfsFileStat* file_stat)
    {
      int ret = TFS_SUCCESS;
      int64_t done = 0;
      if (!file_.check_read())
      {
        ret = EXIT_READ_FILE_ERROR;
      }
      else if (file_.ds_.size() > 0)
      {
        file_.set_read_index(fsname_.get_file_id() % file_.ds_.size());
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t read_len = 0;
        int64_t real_read_len = 0;
        while (done < count)
        {
          int32_t retry = file_.get_read_retry_time();
          read_len = ((count - done) < MAX_READ_SIZE) ? (count - done) : MAX_READ_SIZE;
          while (retry--)
          {
            ret = read_ex((char*)buf + done, read_len, real_read_len, file_stat);
            if (TFS_SUCCESS != ret)
            {
              file_.set_next_read_index();
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

      // check cache, if expired, remove it
      session_->expire_block_cache(fsname_.get_block_id(), file_.cache_hit_, ret);

      // TFS_ERROR shouldn't return to upper layer
      ret = (ret != TFS_ERROR) ? ret : EXIT_READ_FILE_ERROR;
      return (ret != TFS_SUCCESS) ? ret : done;
    }

    int64_t TfsFile::write(const void* buf, const int64_t count)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      int64_t done = 0;
      if(!file_.check_write())
      {
        ret = EXIT_WRITE_FILE_ERROR;
      }
      else
      {
        int64_t write_len = 0;
        while (done < count)
        {
          write_len = ((count - done) < MAX_READ_SIZE) ? (count - done) : MAX_READ_SIZE;
          ret = write_ex((char*)buf + done, write_len);
          if (TFS_SUCCESS != ret)
          {
            break;
          }
          file_.offset_ += write_len;
          done += write_len;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        file_.crc_ = Func::crc(file_.crc_, static_cast<const char*>(buf), count);
      }
      else
      {
        // tell close should do nothing because write fail
        file_.write_status_ = WRITE_STATUS_FAIL;
      }

      // TFS_ERROR shouldn't return to upper layer
      ret = (ret != TFS_ERROR) ? ret : EXIT_WRITE_FILE_ERROR;
      return (ret != TFS_SUCCESS) ? ret : done;
    }

    int TfsFile::close(const int32_t status)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = (file_.write_status_ == WRITE_STATUS_OK) ? TFS_SUCCESS : EXIT_CLOSE_FILE_ERROR;
      if ((TFS_SUCCESS == ret) && !(file_.mode_ & T_READ) && !(file_.mode_ & T_UNLINK))
      {
        // only create & update need real close to ds
        if (!file_.check_write())
        {
          ret = EXIT_CLOSE_FILE_ERROR;
        }
        else
        {
          ret = close_ex(status);
        }
      }

      return ret;
    }

    int TfsFile::unlink(const common::TfsUnlinkType action, int64_t& file_size)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
      int ret = TFS_SUCCESS;
      if (!file_.check_write())
      {
        ret = EXIT_UNLINK_FILE_ERROR;
      }
      else
      {
        ret = unlink_ex(action, file_size, false);  // real unlink
      }

      // tell close should do nothing because unlink fail
      if (TFS_SUCCESS != ret)
      {
        file_.write_status_ = WRITE_STATUS_FAIL;
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

    int TfsFile::open_ex()
    {
      file_.ds_.clear();
      file_.family_info_.family_id_ = INVALID_FAMILY_ID;
      uint64_t block_id = fsname_.get_block_id();
      int ret = session_->get_block_info(block_id, file_, file_.mode_);
      if (TFS_SUCCESS == ret)
      {
        // block id may allocated by ns, update it
        fsname_.set_block_id(block_id);

        TBSYS_LOG(DEBUG, "server replica %zd", file_.ds_.size());
        for (uint32_t i = 0; i < file_.ds_.size(); i++)
        {
          TBSYS_LOG(DEBUG, "dataserver %d address %s",
            i, tbsys::CNetUtil::addrToString(file_.ds_[i]).c_str());
        }
        TBSYS_LOG(DEBUG, "family id %"PRI64_PREFIX"d", file_.family_info_.family_id_);
        if (INVALID_FAMILY_ID != file_.family_info_.family_id_)
        {
          const int data_num = GET_DATA_MEMBER_NUM(file_.family_info_.family_aid_info_);
          const int check_num = GET_CHECK_MEMBER_NUM(file_.family_info_.family_aid_info_);
          for (int32_t i = 0; i < data_num + check_num; i++)
          {
            TBSYS_LOG(DEBUG, "block: %"PRI64_PREFIX"u, server: %s",
                file_.family_info_.members_[i].first,
                tbsys::CNetUtil::addrToString(file_.family_info_.members_[i].second).c_str());
            TBSYS_LOG(DEBUG, "%dth dataserver: %s", i, tbsys::CNetUtil::addrToString(file_.ds_[i]).c_str());
          }
        }
      }

      return ret;
    }

    int TfsFile::stat_ex(TfsFileStat& file_stat)
    {
      int ret = TFS_SUCCESS;
      uint64_t server = 0;
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
        server = file_.get_read_ds();
        ret = send_msg_to_server(server, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "stat file %s fail. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d",
            fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
            tbsys::CNetUtil::addrToString(server).c_str(), ret);
      }
      else
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
            TBSYS_LOG(WARN, "stat file %s fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, ret: %d",
                fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
                tbsys::CNetUtil::addrToString(server).c_str(), smsg->get_error(), ret);
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

    int TfsFile::read_ex(char* buf, const int64_t count, int64_t& read_len,
        common::TfsFileStat* file_stat)
    {
      int ret = TFS_SUCCESS;
      uint64_t server = 0;
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
        server = file_.get_read_ds();
        ret = send_msg_to_server(server, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "read file %s fail. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d",
            fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
            tbsys::CNetUtil::addrToString(server).c_str(), ret);
      }
      else
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
            TBSYS_LOG(WARN, "read file %s fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, ret: %d",
                fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
                tbsys::CNetUtil::addrToString(server).c_str(), smsg->get_error(), ret);
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

    int TfsFile::write_ex(const char* buf, int64_t count)
    {
      int ret = TFS_SUCCESS;
      uint64_t server = 0;
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
        server = file_.get_write_ds();
        ret = send_msg_to_server(server, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write file %s fail. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d",
           fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
           tbsys::CNetUtil::addrToString(server).c_str(), ret);
      }
      else
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
            TBSYS_LOG(WARN, "write file %s fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, ret: %d",
                fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
                tbsys::CNetUtil::addrToString(server).c_str(), smsg->get_error(), ret);
          }
        }
        else
        {
          WriteFileRespMessageV2* response = dynamic_cast<WriteFileRespMessageV2*>(resp_msg);
          file_.lease_id_ = response->get_lease_id();
          fsname_.set_block_id(response->get_block_id());
          fsname_.set_file_id(response->get_file_id());
          TBSYS_LOG(DEBUG, "write file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u",
              fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(), file_.lease_id_);
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::close_ex(const int32_t status)
    {
      int ret = TFS_SUCCESS;
      uint64_t server = 0;
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
        msg.set_status(status);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        server = file_.get_write_ds();
        ret = send_msg_to_server(server, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "close file %s fail. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, server: %s, ret: %d",
            fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
            tbsys::CNetUtil::addrToString(server).c_str(), ret);
      }
      else
      {
        if (STATUS_MESSAGE != resp_msg->getPCode())
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
        else
        {
          StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
          ret = smsg->get_status();
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "close file %s fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, server: %s, error msg: %s, ret: %d",
                fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
                tbsys::CNetUtil::addrToString(server).c_str(), smsg->get_error(), ret);
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }

    int TfsFile::unlink_ex(const int32_t action, int64_t& file_size, const bool prepare)
    {
      int ret = TFS_SUCCESS;
      uint64_t server = 0;
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
        msg.set_prepare_flag(prepare);
        if (file_.has_family())
        {
          msg.set_family_info(file_.family_info_);
        }
        server = file_.get_write_ds();
        ret = send_msg_to_server(server, client, &msg, resp_msg);
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "unlink file %s fail. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, server: %s, prepare: %s, ret: %d",
            fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
            tbsys::CNetUtil::addrToString(server).c_str(), prepare ? "true" : "false", ret);
      }
      else
      {
        if (STATUS_MESSAGE != resp_msg->getPCode())
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
        else
        {
          StatusMessage* smsg = dynamic_cast<StatusMessage*>(resp_msg);
          ret = smsg->get_status();
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "unlink file %s fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, server: %s, prepare: %s, error msg: %s, ret: %d",
                fsname_.get_name(), fsname_.get_block_id(), fsname_.get_file_id(),
                tbsys::CNetUtil::addrToString(server).c_str(), prepare ? "true" : "false",
                smsg->get_error(), ret);
          }
          else
          {
            if (prepare)
            {
              file_.lease_id_ = strtoll(smsg->get_error(), NULL, 10);
            }
            else
            {
              file_size = strtoll(smsg->get_error(), NULL, 10);
            }
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);

      return ret;
    }
  }
}

