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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "tfs_file.h"
#include "fsname.h"
#include "common/error_msg.h"
#include "common/func.h"
#include "message/client.h"
#include "message/client_pool.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;

#ifdef __CLIENT_METRICS__
ClientMetrics TfsFile::read_metrics_;
ClientMetrics TfsFile::write_metrics_;
ClientMetrics TfsFile::close_metrics_;
#endif

TfsFile::TfsFile() :
  session_(NULL), client_(NULL), file_number_(0), file_id_(0), last_elect_ds_id_(0), block_id_(0), crc_(0), mode_(0),
  offset_(0), pri_ds_index_(0), is_open_flag_(TFS_FILE_OPEN_FLAG_NO), option_flag_(0), eof_(TFS_FILE_EOF_FLAG_NO)
{
  ds_list_.clear();
  file_name_[0] = '\0';
  error_message_[0] = '\0';
  srand(time(NULL) + rand() + pthread_self());
}

TfsFile::~TfsFile()
{

}

void TfsFile::conv_name(const char *file_name, const char *suffix)
{
  block_id_ = 0;
  file_id_ = 0;
  if (file_name != NULL)
  {
    FSName fsname(file_name, suffix);
    fsname.set_cluster_id(session_->get_cluster_id());
    block_id_ = fsname.get_block_id();
    file_id_ = fsname.get_file_id();
    strcpy(file_name_, fsname.get_name());
  }
}

int TfsFile::tfs_open(const char *file_name, const char *suffix, const int32_t mode)
{
  mode_ = mode;
  conv_name(file_name, suffix);
  int32_t iret = tfs_open(block_id_, file_id_, mode_);
  if (iret != TFS_SUCCESS)
  {
    return iret;
  }

  FSName fsname;
  fsname.set_cluster_id(session_->get_cluster_id());
  fsname.set_block_id(block_id_);
  fsname.set_file_id(file_id_);
  fsname.set_prefix(suffix);
  file_id_ = fsname.get_file_id();
  strcpy(file_name_, fsname.get_name());
  return TFS_SUCCESS;
}

int TfsFile::tfs_open(const uint32_t block_id, const uint64_t file_id, const int32_t mode)
{
  if (session_ == NULL)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "not set session, session is null");
    return TFS_ERROR;
  }

  if ((block_id == 0) && (mode == READ_MODE || mode == UNLINK_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "%s no block, block_id(%u) file_id(%" PRI64_PREFIX "u)",
             file_name_, block_id, file_id);
    return TFS_ERROR;
  }

  mode_ = mode;
  block_id_ = block_id;
  file_id_ = file_id;
  ds_list_.clear();

  if (mode == READ_MODE)
  {
    if (session_->get_block_info(block_id_, ds_list_) != TFS_SUCCESS)
    {
      if (ds_list_.size() == 0)
      {
        snprintf(error_message_, ERR_MSG_SIZE, "tfs open fail, block(%u) no exist in nameserver", block_id_);
        return TFS_ERROR;
      }
    }
  }
  else if (mode == UNLINK_MODE)
  {
    if (session_->get_unlink_block_info(block_id_, ds_list_) != TFS_SUCCESS)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "tfs open fail, block(%u) no exist in nameserver", block_id_);
      return TFS_ERROR;
    }
  }
  else
  {
    uint32_t current_block_id = block_id_;
    int32_t flag = BLOCK_WRITE | BLOCK_CREATE;
    if ((mode_ & NEWBLK_MODE))
    {
      flag |= BLOCK_NEWBLK;
    }
    if ((mode_ & NOLEASE_MODE))
    {
      flag |= BLOCK_NOLEASE;
    }
    if (session_->create_block_info(current_block_id, ds_list_, flag, fail_servers_) != TFS_SUCCESS)
    {
      if (ds_list_.size() == 0 || current_block_id == 0)
      {
        snprintf(error_message_, ERR_MSG_SIZE, "create block(%u) fail in nameserver", block_id_);
        return TFS_ERROR;
      }
    }
    if (block_id_ == 0)
    {
      block_id_ = current_block_id;
      mode_ |= APPEND_MODE;
    }
  }
  if (block_id_ == 0 || ds_list_.size() == 0)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "block(%u),is invalid, dataserver size(%u)", block_id_,
             static_cast<uint32_t> (ds_list_.size()));
    return TFS_ERROR;
  }

  if ((mode_ & READ_MODE))
  {
    if (file_id_ == 0)
    {
      //TBSYS_LOG(WARN, "blockId(%u) read fileid is 0.", block_id_);
    }
    pri_ds_index_ = static_cast<int32_t> (file_id_ % ds_list_.size());
  }
  else
  {
    pri_ds_index_ = 0;
  }

  if (connect_ds() != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "connect to dataserver fail.");
    if ((mode_ & APPEND_MODE))
    {
      fail_servers_.push_back(ds_list_[0]);
    }
    return TFS_ERROR;
  }

  fail_servers_.clear();

  if ((mode_ & WRITE_MODE))
  {
    int32_t ret = create_filename();
    if (ret != TFS_SUCCESS)
    {
      CLIENT_POOL.release_client(client_);
      TBSYS_LOG(ERROR, "create file name faile(%d)", ret);
      return ret;
    }
    if (file_id_ == 0)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail,fileid == 0");
      CLIENT_POOL.release_client(client_);
      return TFS_ERROR;
    }
  }
  offset_ = 0;
  eof_ = TFS_FILE_EOF_FLAG_NO;
  crc_ = 0;
  is_open_flag_ = TFS_FILE_OPEN_FLAG_YES;
  return TFS_SUCCESS;
}

int TfsFile::connect_ds()
{
  int32_t iret = TFS_ERROR;
  uint32_t ds_list_size = ds_list_.size();
  uint32_t j = 0;
  uint64_t ds_id = 0;
  for (uint32_t i = 0; i < ds_list_size; i++)
  {
    j = (pri_ds_index_ + i) % ds_list_size;
    ds_id = ds_list_[j];
    if (ds_id == ULONG_LONG_MAX)
    {
      break;
    }

    client_ = CLIENT_POOL.get_client(ds_id);
    if (client_ == NULL)
    {
      break;
    }
    if (client_->connect() == TFS_SUCCESS)
    {
      pri_ds_index_ = j;
      last_elect_ds_id_ = ds_id;
      iret = TFS_SUCCESS;
      break;
    }

    CLIENT_POOL.release_client(client_);
    client_ = NULL;

    if (mode_ != READ_MODE)
      break;
  }

  return iret;
}

int TfsFile::connect_next_ds()
{
  client_->disconnect();
  CLIENT_POOL.release_client(client_);
  pri_ds_index_++;
  if (connect_ds() != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "retry connect datasrver fail");
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}

int TfsFile::tfs_reset_read()
{
  offset_ = 0;
  eof_ = TFS_FILE_EOF_FLAG_NO;
  crc_ = 0;
  return connect_next_ds();
}

int TfsFile::create_filename()
{
  CreateFilenameMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  Message *message = client_->call(&dsmessage);
  if (message == NULL)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "createfilename send request fail");
    return TFS_ERROR;
  }
  if (message->get_message_type() != RESP_CREATE_FILENAME_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*> (message);
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail, get error msg(%s), status(%d) from(%s)",
               msg->get_error(), msg->get_status(), tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
    }
    else
    {
      snprintf(error_message_, ERR_MSG_SIZE, "create file name fail");
    }
    tbsys::gDelete(message);
    return TFS_ERROR;
  }
  RespCreateFilenameMessage* msg = dynamic_cast<RespCreateFilenameMessage*> (message);
  file_id_ = msg->get_file_id();
  file_number_ = msg->get_file_number();
  tbsys::gDelete(message);
  return TFS_SUCCESS;
}

int TfsFile::tfs_read_v2(char *data, const int32_t len, FileInfo *file_info)
{
  if (eof_ == TFS_FILE_EOF_FLAG_YES)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "file is eof.");
    return 0;
  }
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return -1;
  }
  if (!(mode_ & READ_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't read.");
    return -1;
  }

  ReadDataMessageV2 dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_offset(offset_);
  dsmessage.set_length(len);

  int32_t ret_len = -1;
  int32_t retry = 2;
  while (retry--)
  {
    ret_len = -1;
    time_t now1 = Func::curr_time();
    Message *message = client_->call(&dsmessage);
    time_t now2 = Func::curr_time();
    if (now2 - now1 > 500000)
    {
      TBSYS_LOG    (WARN, "filename(%s), read file low (%" PRI64_PREFIX
                    "u)(ns), block_id(%u), file_id(%" PRI64_PREFIX "u), offset(%d),len(%d), from dataserver(%s), msg(%p)",
                    file_name_, now2 - now1, block_id_, file_id_, offset_,
                    len, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str(), message);
    }

#ifdef __CLIENT_METRICS__
    read_metrics_.stat("tfsRead", now2 - now1, 1, 5000);
#endif

    if (message != NULL)
    {
      if (message->get_message_type() == RESP_READ_DATA_MESSAGE_V2)
      {
        RespReadDataMessageV2 *msg = dynamic_cast<RespReadDataMessageV2*>(message);
        if (offset_ == 0 && msg->get_file_info() == NULL)
        {
          TBSYS_LOG(WARN, "tfs read, get file info error, filename(%s), fileid(%"PRI64_PREFIX"u), form dataserver(%s)",
                    file_name_, file_id_, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
          tbsys::gDelete(message);
          continue;
        }
        else
        {
          if (msg->get_file_info() != NULL)
          {
            memcpy(file_info, msg->get_file_info(), FILEINFO_SIZE);
          }
          if (file_info->id_ != file_id_)
          {
            TBSYS_LOG(WARN, "tfs read, file(%s  : %"PRI64_PREFIX"u  :%"PRI64_PREFIX"u) not match, from dataserver(%s)",
                      file_name_, file_info->id_, file_id_, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
            tbsys::gDelete(message);
            continue;
          }
        }

        ret_len = msg->get_length();

        if (ret_len >= 0)
        {
          if (ret_len < len)
          {
            eof_ = TFS_FILE_EOF_FLAG_YES;
          }
          if (ret_len > 0)
          {
            memcpy(data, msg->get_data(), ret_len);
            offset_ += ret_len;
          }
        }
        else
        {
#ifdef __CLIENT_METRICS__
          read_metrics_.incr_failed_count();
#endif
          snprintf(error_message_, ERR_MSG_SIZE, "tfs read read fail from dataserver(%s)ret_len(%d)",
                   tbsys::CNetUtil::addrToString(client_->get_mip()).c_str(), ret_len);
        }
      }
      else if (message->get_message_type() == STATUS_MESSAGE)
      {
#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif
        StatusMessage* msg = dynamic_cast<StatusMessage*>(message);
        snprintf(error_message_, ERR_MSG_SIZE, "tfs read, get status msg(%s)", msg->get_error());
        if (msg->get_status() == STATUS_MESSAGE_ACCESS_DENIED)
        {
          // if access denied, return directly
          tbsys::gDelete(message);
          return ret_len;
        }
      }
      else
      {
#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif
        snprintf(error_message_, ERR_MSG_SIZE, "message type is error.");
      }
      tbsys::gDelete(message);
    }
    else
    {
#ifdef __CLIENT_METRICS__
      read_metrics_.incr_timeout_count();
#endif
      snprintf(error_message_, ERR_MSG_SIZE, "read, send to dataserver fail.");
    }
    if (ret_len != -1)
      break;
    if (connect_next_ds() != TFS_SUCCESS)
      break;
  }
  return ret_len;
}

int TfsFile::tfs_read_scale_image(char *data, const int32_t len, const int32_t width, const int32_t height,
                                  FileInfo *finfo)
{
  if (eof_ == TFS_FILE_EOF_FLAG_YES)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "file is eof.");
    return 0;
  }

  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return -1;
  }
  if (!(mode_ & READ_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't read.");
    return -1;
  }

  if (width == 0 || height == 0)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "cannot set width or height 0.");
    return -1;
  }

  ReadScaleImageMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_offset(offset_);
  dsmessage.set_length(len);

  if (offset_ == 0)
  {
    ReadScaleImageMessage::ZoomData zoomd;
    zoomd.zoom_width_ = width;
    zoomd.zoom_height_ = height;
    zoomd.zoom_type_ = ReadScaleImageMessage::ZOOM_SPEC;
    dsmessage.set_zoom_data(zoomd);
  }

  int32_t ret_len = -1;
  uint32_t retry = ds_list_.size();
  while (retry--)
  {
    ret_len = -1;
    time_t now1 = Func::curr_time();
    Message *message = client_->call(&dsmessage);
    time_t now2 = Func::curr_time();
    if (now2 - now1 > 500000)
    {
      TBSYS_LOG    (WARN, "tfs file(%s), read file low (%" PRI64_PREFIX
                    "u)(ns), block_id(%u), file_id(%" PRI64_PREFIX "u), offset(%d), len(%d), dataserfer(%s),msg(%p)",
                    file_name_, now2-now1, block_id_, file_id_, offset_,
                    len, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str(), message);
    }

#ifdef __CLIENT_METRICS__
    read_metrics_.stat("tfsRead", now2 - now1, 1, 5000);
#endif

    if (message != NULL)
    {
      if (message->get_message_type() == RESP_READ_DATA_MESSAGE_V2)
      {
        RespReadDataMessageV2* msg = dynamic_cast<RespReadDataMessageV2*>(message);
        if (offset_ == 0 && msg->get_file_info() == NULL)
        {
          TBSYS_LOG(WARN, "tfs_read_scale_image, get file info error, filename(%s), fileid(%"PRI64_PREFIX"u) from dataserer(%s)",
                    file_name_, file_id_, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
          tbsys::gDelete(message);
          continue;
        }
        else
        {
          memcpy(finfo, msg->get_file_info(), FILEINFO_SIZE);
          if (finfo->id_ != file_id_)
          {
            TBSYS_LOG(WARN, "tfs_read_scale_image, fileid(%s  %"PRI64_PREFIX"u :%"PRI64_PREFIX"u) not match, from dataserver(%s)",
                      file_name_, finfo->id_, file_id_, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
            tbsys::gDelete(message);
            continue;
          }
        }

        ret_len = msg->get_length();

        if (ret_len >= 0)
        {
          if (ret_len < len)
          {
            eof_ = TFS_FILE_EOF_FLAG_YES;
          }
          if (ret_len > 0)
          {
            memcpy(data, msg->get_data(), ret_len);
            offset_ += ret_len;
          }
        }
        else
        {
#ifdef __CLIENT_METRICS__
          read_metrics_.incr_failed_count();
#endif
          snprintf(error_message_, ERR_MSG_SIZE, "tfs_read_scale_image, read fail from dataserver(%s), ret len(%d)",
                   tbsys::CNetUtil::addrToString(client_->get_mip()).c_str(), ret_len);
        }
      }
      else if (message->get_message_type() == STATUS_MESSAGE)
      {

#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif

        StatusMessage* msg = dynamic_cast<StatusMessage*>(message);
        snprintf(error_message_, ERR_MSG_SIZE, "tfs_read_scale_image, get status msg(%s)", msg->get_error());
        if (msg->get_status() == STATUS_MESSAGE_ACCESS_DENIED)
        {
          // if access denied, return directly
          tbsys::gDelete(message);
          return ret_len;
        }
      }
      else
      {
#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs_read_scale_image: message type is error.");
      }
      tbsys::gDelete(message);
    }
    else
    {
#ifdef __CLIENT_METRICS__
      read_metrics_.incr_timeout_count();
#endif
      snprintf(error_message_, ERR_MSG_SIZE, "tfs_read_scale_image: send msg to dataserver fail");
    }
    if (ret_len != -1)
      break;

    if (connect_next_ds() != TFS_SUCCESS)
      break;
  }
  return ret_len;
}

int64_t TfsFile::tfs_lseek(const int64_t offset, const int32_t whence)
{
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return -1;
  }
  if (!(mode_ & READ_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't read.");
    return -1;
  }

  switch (whence)
  {
  case TFS_SEEK_SET:
    offset_ = offset;
    return offset_;
  case TFS_SEEK_CUR:
    offset_ += offset;
    return offset_;
  default:
    break;
  }
  return -1;
}

int TfsFile::tfs_read(char *data, const int32_t len)
{
  if (eof_ == TFS_FILE_EOF_FLAG_YES)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "file is eof.");
    return 0;
  }
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return -1;
  }
  if (!(mode_ & READ_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't read.");
    return -1;
  }

  ReadDataMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_offset(offset_);
  dsmessage.set_length(len);

  int32_t ret_len = -1;
  int32_t retry = 0x02;
  while (retry--)
  {
    ret_len = -1;
    time_t now1 = Func::curr_time();

    Message *message = client_->call(&dsmessage);
    time_t now2 = Func::curr_time();

    if (now2 - now1 > 500000)
    {
      TBSYS_LOG    (WARN, "filename(%s) read file low (%" PRI64_PREFIX
                    "u)(ns), block_id(%u),file_id(%" PRI64_PREFIX "u), offset(%d), len(%d) from dataserver(%s)",
                    file_name_, now2 - now1, block_id_, file_id_, offset_,
                    len, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
    }

#ifdef __CLIENT_METRICS__
    read_metrics_.stat("tfsRead", now2 - now1, 1, 5000);
#endif

    if (message != NULL)
    {
      if (message->get_message_type() == RESP_READ_DATA_MESSAGE)
      {
        RespReadDataMessage *msg = dynamic_cast<RespReadDataMessage*>(message);
        ret_len = msg->get_length();
        if (ret_len >= 0)
        {
          if (ret_len < len)
          {
            eof_ = TFS_FILE_EOF_FLAG_YES;
          }
          if (ret_len > 0)
          {
            memcpy(data, msg->get_data(), ret_len);
            offset_ += ret_len;
          }
        }
        else
        {
#ifdef __CLIENT_METRICS__
          read_metrics_.incr_failed_count();
#endif
          snprintf(error_message_, ERR_MSG_SIZE, "read fail from dataserver(%s): ret_len:%d",
                   tbsys::CNetUtil::addrToString(client_->get_mip()).c_str(), ret_len);
        }
      }
      else if (message->get_message_type() == STATUS_MESSAGE)
      {
#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif
        StatusMessage* msg = dynamic_cast<StatusMessage*>(message);
        snprintf(error_message_, ERR_MSG_SIZE, "tfs read, get errorm msg(%s)", msg->get_error());
        if (msg->get_status() == STATUS_MESSAGE_ACCESS_DENIED)
        {
          // if access denied, return directly
          tbsys::gDelete(message);
          return ret_len;
        }
      }
      else
      {
#ifdef __CLIENT_METRICS__
        read_metrics_.incr_failed_count();
#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs read message type is error.");
      }
      tbsys::gDelete(message);
    }
    else
    {
#ifdef __CLIENT_METRICS__
      read_metrics_.incr_timeout_count();
#endif
      snprintf(error_message_, ERR_MSG_SIZE, "read, send msg to dataserver fail.");
    }

    if (ret_len != -1)
      break;
    if (connect_next_ds() != TFS_SUCCESS)
      break;
  }
  return ret_len;
}

int TfsFile::tfs_stat(FileInfo *file_info, const int32_t mode)
{
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return TFS_ERROR;
  }
  if (!(mode_ & READ_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't read.");
    return TFS_ERROR;
  }

  FileInfoMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_mode(mode);

  int ret = EXIT_GENERAL_ERROR;
  int32_t retry = 0x02;
  while (retry--)
  {
    Message *message = client_->call(&dsmessage);
    if (message == NULL)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "fstat, send msg to dataserver fail");
      if (connect_next_ds() != TFS_SUCCESS)
        break;
      continue;
    }
    if (message->get_message_type() != RESP_FILE_INFO_MESSAGE)
    {
      if (message->get_message_type() == STATUS_MESSAGE)
      {
        snprintf(error_message_, ERR_MSG_SIZE, "tfs stat, get error msg(%s)",
                 dynamic_cast<StatusMessage*> (message)->get_error());
      }
      else
      {
        snprintf(error_message_, ERR_MSG_SIZE, "message type is error");
      }
      tbsys::gDelete(message);
      break;
    }
    RespFileInfoMessage *msg = dynamic_cast<RespFileInfoMessage*> (message);
    if (msg->get_file_info() != NULL)
    {
      memcpy(file_info, msg->get_file_info(), FILEINFO_SIZE);
      if (file_info->id_ != file_id_)
      {
        TBSYS_LOG(WARN, "tfs stat fileid(%s : %"PRI64_PREFIX"u :%"PRI64_PREFIX"u not match", file_name_, file_info->id_, file_id_);
        tbsys::gDelete(message);
        continue;
      }
      ret = TFS_SUCCESS;
    }
    else
    {
      snprintf    (error_message_, ERR_MSG_SIZE, "%s not exist, block_id:%u, file_id:%" PRI64_PREFIX "u, server: %s",
                   file_name_, block_id_, file_id_, tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
    }
    tbsys::gDelete(message);
    break;
  }
  return ret;
}

int TfsFile::tfs_write(char *data, const int32_t len)
{
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs file(%s) don't open file", file_name_);
    return -2;
  }
  if (!(mode_ & WRITE_MODE))
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file mode isn't write.");
    return -1;
  }

  WriteDataMessage dsmessage;
  dsmessage.set_file_number(file_number_);
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_offset(offset_);
  dsmessage.set_length(len);
  dsmessage.set_ds_list(ds_list_);
  dsmessage.set_data(data);

#ifdef __CLIENT_METRICS__
  time_t now1 = Func::curr_time();
#endif
  Message *message = client_->call(&dsmessage);
#ifdef __CLIENT_METRICS__
  time_t now2 = Func::curr_time();
#endif

#ifdef __CLIENT_METRICS__
  write_metrics_.stat("tfsWrite", now2 - now1, 1, 500);
#endif

  int32_t iret = TFS_ERROR;
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage *msg = dynamic_cast<StatusMessage*> (message);
      if (msg->get_status() == STATUS_MESSAGE_OK)
      {
        crc_ = Func::crc(crc_, data, len);
        offset_ += len;
        iret = TFS_SUCCESS;
      }
      else
      {
#ifdef __CLIENT_METRICS__
        write_metrics_.incr_failed_count();
#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs write, get error msg(%s)", msg->get_error());
      }
    }
    tbsys::gDelete(message);
  }
  else
  {
#ifdef __CLIENT_METRICS__
    write_metrics_.incr_timeout_count();
#endif
  }

  if (iret != TFS_SUCCESS)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "tfs write, get error msg from dataserver(%s)",
             tbsys::CNetUtil::addrToString(client_->get_mip()).c_str());
    client_->disconnect();
    is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
    CLIENT_POOL.release_client(client_);
    return -1;
  }
  return len;
}

int TfsFile::tfs_close()
{
  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_NO)
  {
    TBSYS_LOG(INFO, "tfs close successful , buf tfs file not open");
    return TFS_SUCCESS;
  }

  if (mode_ & READ_MODE)
  {
    is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
    if (client_ != NULL)
      CLIENT_POOL.release_client(client_);
    TBSYS_LOG(DEBUG, "tfs close successful");
    return TFS_SUCCESS;
  }

  if (!(mode_ & WRITE_MODE))
  {
    TBSYS_LOG(INFO, "tfs close successful");
    return TFS_SUCCESS;
  }

  if (offset_ == 0)
  {
    is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
    if (client_ != NULL)
    {
      CLIENT_POOL.release_client(client_);
    }
    TBSYS_LOG(INFO, "tfs close successful, offset(%d)", offset_);
    return TFS_ERROR;
  }

  if (client_ == NULL)
  {
    TBSYS_LOG(ERROR, "tfs close fail, client is null");
    return TFS_ERROR;
  }

  CloseFileMessage dsmessage;
  dsmessage.set_file_number(file_number_);
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_option_flag(option_flag_);
  dsmessage.set_ds_list(ds_list_);
  dsmessage.set_crc(crc_);

#ifdef __CLIENT_METRICS__
  time_t now1 = Func::curr_time();
#endif

  Message *message = client_->call(&dsmessage);

#ifdef __CLIENT_METRICS__
  time_t now2 = Func::curr_time();
#endif

#ifdef __CLIENT_METRICS__
  close_metrics_.stat("tfsClose", now2 - now1, 1, 500);
#endif

  int32_t iret = TFS_ERROR;
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* msg = dynamic_cast<StatusMessage*> (message);
      if (msg->get_status() == STATUS_MESSAGE_OK)
      {
        iret = TFS_SUCCESS;
      }
      else
      {
#ifdef __CLIENT_METRICS__
        close_metrics_.incr_failed_count();
#endif
        snprintf(error_message_, ERR_MSG_SIZE, "tfs file close, get errow msg(%s)", msg->get_error());
        client_->disconnect();
      }
    }
    tbsys::gDelete(message);
  }
  else
  {
    snprintf  (error_message_, ERR_MSG_SIZE, "tfs file close,send msg to dataserver fail, errors(time out, blockid(%u) fileid(%" PRI64_PREFIX "u)",
               block_id_, file_id_);
#ifdef __CLIENT_METRICS__
    close_metrics_.incr_failed_count();
#endif
  }

#ifdef __CLIENT_METRICS__
  if (close_metrics_.total_count % 50 == 0)
  {
    TBSYS_LOG(DEBUG, "write file (%s) on server(%s)", file_name_, tbsys::CNetUtil::addrToString(last_elect_ds_id_).c_str());
  }
#endif

  is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
  CLIENT_POOL.release_client(client_);
  return iret;
}

// session
void TfsFile::set_session(TfsSession *session)
{
  session_ = session;
}

int TfsFile::get_file_crc_size(const char *filename, uint32_t& crc, int32_t& size)
{
  if (filename == NULL)
  {
    TBSYS_LOG(ERROR, "get file crc size fail, filename(%s)", filename);
    return TFS_ERROR;
  }
  crc = 0;
  size = 0;
  int fd = open(filename, O_RDONLY);
  if (fd == -1)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file(%s) error", filename);
    return TFS_ERROR;
  }

  char* data = new char[MAX_READ_SIZE];
  if (data == NULL)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "cannot allocate data buffer");
    return TFS_ERROR;
  }

  int32_t rlen;
  while ((rlen = read(fd, data, MAX_READ_SIZE)) > 0)
  {
    crc = Func::crc(crc, data, rlen);
    size += rlen;
  }
  tbsys::gDelete(data);
  close(fd);
  return TFS_SUCCESS;
}

const char *TfsFile::get_file_name()
{
  if (NULL == session_)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "not set session, session is null");
    return NULL;
  }

  FSName fsname;
  fsname.set_cluster_id(session_->get_cluster_id());
  fsname.set_block_id(block_id_);
  fsname.set_file_id(file_id_);
  strcpy(file_name_, fsname.get_name());
  return file_name_;
}

int TfsFile::stat(const char *filename, const char *suffix, FileInfo *file_info)
{
  int32_t iret = tfs_open(filename, suffix, READ_MODE);
  if (iret != TFS_SUCCESS)
    return iret;
  iret = tfs_stat(file_info);
  if (iret != TFS_SUCCESS)
  {
    //TODO
  }
  tfs_close();
  return iret;
}

int TfsFile::new_filename()
{
  int32_t retry = 3;
  int32_t ret = TFS_SUCCESS;
  while (retry--)
  {
    ret = tfs_open(0, (uint64_t) 0, WRITE_MODE | NOLEASE_MODE);
    if (ret == TFS_SUCCESS)
    {
      break;
    }
  }

  if (is_open_flag_ == TFS_FILE_OPEN_FLAG_YES)
  {
    is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
    CLIENT_POOL.release_client(client_);
  }
  return ret;
}

int TfsFile::unlink(const char *fileName, const char *suffix, int action /*=0x0(CloseFileMessage::DELETE)*/)
{
  conv_name(fileName, suffix);
  return unlink(block_id_, file_id_, action);
}

int TfsFile::unlink(const uint32_t block_id, const uint64_t file_id, const int32_t action)
{
  int32_t iret = tfs_open(block_id, file_id, UNLINK_MODE);
  if (iret != TFS_SUCCESS)
    return iret;
  UnlinkFileMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_ds_list(ds_list_);
  dsmessage.set_unlink_type(action);
  dsmessage.set_option_flag(option_flag_);

  iret = TFS_SUCCESS;
  Message *message = client_->call(&dsmessage);
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage *msg = dynamic_cast<StatusMessage*> (message);
      if (msg->get_status() != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(WARN, "block_id(%u), file_id(%"PRI64_PREFIX"u)%s", block_id, file_id, msg->get_error());
        iret = msg->get_status();
        snprintf(error_message_, ERR_MSG_SIZE, "unlink error, code(%d), errmsg(%s)", iret, msg->get_error());
      }
      else
      {
        iret = TFS_SUCCESS;
      }
    }
    tbsys::gDelete(message);
  }
  is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
  CLIENT_POOL.release_client(client_);
  return iret;
}

int TfsFile::rename(const char *filename, const char *old_prefix, const char *new_prefix)
{
  conv_name(filename, old_prefix);
  FSName nfname(filename, new_prefix, session_->get_cluster_id());
  nfname.set_cluster_id(session_->get_cluster_id());
  if (new_prefix == NULL)
  {
    nfname.set_prefix(0);
  }
  return rename(block_id_, file_id_, nfname.get_file_id());
}

int TfsFile::rename(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id)
{
  int32_t iret = tfs_open(block_id, file_id, UNLINK_MODE);
  if (iret != TFS_SUCCESS)
    return iret;

  RenameFileMessage dsmessage;
  dsmessage.set_block_id(block_id_);
  dsmessage.set_file_id(file_id_);
  dsmessage.set_new_file_id(new_file_id);
  dsmessage.set_ds_list(ds_list_);
  dsmessage.set_option_flag(option_flag_);

  Message *message = client_->call(&dsmessage);
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage *msg = dynamic_cast<StatusMessage*> (message);
      if (msg->get_status() == STATUS_MESSAGE_OK)
      {
        iret = TFS_SUCCESS;
      }
      else
      {
        snprintf(error_message_, ERR_MSG_SIZE, "%s", msg->get_error());
      }
    }
    tbsys::gDelete(message);
  }
  is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
  file_id_ = new_file_id;
  CLIENT_POOL.release_client(client_);
  return iret;
}

int TfsFile::get_file_list(const uint32_t block_id, FILE_INFO_LIST &l)
{
  int32_t iret = tfs_open(block_id, 0, READ_MODE);
  if (iret != TFS_SUCCESS)
    return iret;

  GetServerStatusMessage gssMessage;
  gssMessage.set_status_type(GSS_BLOCK_FILE_INFO);
  gssMessage.set_return_row(block_id);

  iret = TFS_ERROR;
  Message *message = client_->call(&gssMessage);
  if (message != NULL)
  {
    if (message->get_message_type() == BLOCK_FILE_INFO_MESSAGE)
    {
      BlockFileInfoMessage* msg = dynamic_cast<BlockFileInfoMessage*> (message);
      FILE_INFO_LIST* file_info_list = msg->get_fileinfo_list();
      const uint32_t file_info_list_size = file_info_list->size();
      for (uint32_t i = 0; i < file_info_list_size; i++)
      {
        FileInfo* fi = new FileInfo();
        memcpy(fi, file_info_list->at(i), sizeof(FileInfo));
        l.push_back(fi);
      }
      iret = TFS_SUCCESS;
    }
    else if (message->get_message_type() == STATUS_MESSAGE)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "get error msg(%s)", ((StatusMessage*) message)->get_error());
    }
    else
    {
      snprintf(error_message_, ERR_MSG_SIZE, "message type is error.");
    }
    tbsys::gDelete(message);
  }
  else
  {
    snprintf(error_message_, ERR_MSG_SIZE, "get file info list failed, send msg to dataserver fail.");
  }
  is_open_flag_ = TFS_FILE_OPEN_FLAG_NO;
  CLIENT_POOL.release_client(client_);
  return iret;
}

int TfsFile::save_file(const char *filename, const char *tfsname, const char *suffix)
{
  int fd = open(filename, O_RDONLY);
  if (fd == -1)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "open file(%s) error", filename);
    return TFS_ERROR;
  }
  int32_t ret = TFS_SUCCESS;
  int32_t retry = 2;
  while (retry--)
  {
    ret = tfs_open(tfsname, suffix, WRITE_MODE);
    if (ret == TFS_SUCCESS)
    {
      break;
    }
  }

  if (ret != TFS_SUCCESS)
  {
    close(fd);
    TBSYS_LOG(ERROR, "create new file error: %s", error_message_);
    return TFS_ERROR;
  }

  // cannot allocate on stack, perhaps cause stack overflow.
  // char data[MAX_READ_SIZE];
  char* data = new char[MAX_READ_SIZE];
  if (data == NULL)
  {
    snprintf(error_message_, ERR_MSG_SIZE, "cannot allocate data buffer");
    return TFS_ERROR;
  }
  int32_t rlen;
  while ((rlen = read(fd, data, MAX_READ_SIZE)) > 0)
  {
    if (tfs_write(data, rlen) != rlen)
    {
      ret = EXIT_GENERAL_ERROR;
      break;
    }
  }
  if (ret == TFS_SUCCESS)
  {
    ret = tfs_close();
  }
  tbsys::gDelete(data);
  close(fd);
  return ret;
}

int TfsFile::save_file(const char* tfsname, const char* suffix, char* data, const int32_t length)
{
  int32_t ret = TFS_SUCCESS;
  int32_t retry = 0x02;
  while (retry--)
  {
    ret = tfs_open(tfsname, suffix, WRITE_MODE);
    if (ret == TFS_SUCCESS)
    {
      break;
    }
  }

  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "create new file error(%s)", error_message_);
    return ret;
  }

  int32_t written = 0;
  int32_t datalen = length;
  int32_t perlen = 0;

  while (datalen > 0)
  {
    perlen = datalen < MAX_READ_SIZE ? datalen : MAX_READ_SIZE;
    if (tfs_write(data + written, perlen) != perlen)
    {
      ret = TFS_ERROR;
      break;
    }
    written += perlen;
    datalen -= perlen;
  }

  if (ret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "tfsWrite filename(%s) suffix(%s) failed with error(%s)", tfsname, suffix, error_message_);
  }
  else
  {
    ret = tfs_close();
  }
  return ret;
}
