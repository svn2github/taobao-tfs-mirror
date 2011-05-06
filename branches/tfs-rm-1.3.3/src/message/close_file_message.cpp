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
#include "close_file_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    CloseFileMessage::CloseFileMessage() :
      block_(NULL), file_info_(NULL), option_flag_(0), version_(0), lease_id_(0), has_lease_(false)
    {
      _packetHeader._pcode = CLOSE_FILE_MESSAGE;
      memset(&close_file_info_, 0, sizeof(CloseFileInfo));
      close_file_info_.mode_ = CLOSE_FILE_MASTER;
    }

    CloseFileMessage::~CloseFileMessage()
    {
    }

    int CloseFileMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&close_file_info_), sizeof(CloseFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t size = 0;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t file_size = 0;
      if (get_int32(&data, &len, &file_size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&block_), size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (file_size > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&file_info_), file_size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      get_int32(&data, &len, &option_flag_);
      has_lease_ = parse_special_ds(ds_, version_, lease_id_);

      return TFS_SUCCESS;
    }

    int32_t CloseFileMessage::message_length()
    {
      int32_t len = sizeof(CloseFileInfo) + get_vint64_len(ds_) + INT_SIZE * 2;
      if (block_ != NULL)
      {
        len += BLOCKINFO_SIZE;
      }
      if (file_info_ != NULL)
      {
        len += FILEINFO_SIZE;
      }
      len += INT_SIZE;
      if (has_lease_)
      {
        len += INT64_SIZE * 3;
      }
      return len;
    }

    int CloseFileMessage::build(char* data, int32_t len)
    {
      int32_t size = 0;
      int32_t file_size = 0;
      if (block_ != NULL)
      {
        size = BLOCKINFO_SIZE;
      }
      if (file_info_ != NULL)
      {
        file_size = FILEINFO_SIZE;
      }

      if (has_lease_)
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_id_));
      }

      if (set_object(&data, &len, &close_file_info_, sizeof(CloseFileInfo)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, file_size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (set_object(&data, &len, block_, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (file_size > 0)
      {
        if (set_object(&data, &len, file_info_, file_size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (set_int32(&data, &len, option_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      // reparse, avoid push verion&lease again when clone twice;
      has_lease_ = parse_special_ds(ds_, version_, lease_id_);
      return TFS_SUCCESS;
    }

    char* CloseFileMessage::get_name()
    {
      return "closefilemessage";
    }

    Message* CloseFileMessage::create(const int32_t type)
    {
      CloseFileMessage* req_cf_msg = new CloseFileMessage();
      req_cf_msg->set_message_type(type);
      return req_cf_msg;
    }
  }
}
