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
#include "file_info_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    // FileInfoMessage
    FileInfoMessage::FileInfoMessage() :
      block_id_(0), file_id_(0), mode_(0)
    {
      _packetHeader._pcode = FILE_INFO_MESSAGE;
    }

    FileInfoMessage::~FileInfoMessage()
    {
    }

    int FileInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&file_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&mode_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t FileInfoMessage::message_length()
    {
      int32_t len = INT_SIZE + INT64_SIZE + INT_SIZE;
      return len;
    }

    int FileInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, file_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, mode_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* FileInfoMessage::get_name()
    {
      return "fileinfomessage";
    }

    Message* FileInfoMessage::create(const int32_t type)
    {
      FileInfoMessage* req_fi_msg = new FileInfoMessage();
      req_fi_msg->set_message_type(type);
      return req_fi_msg;
    }

    // RespFileInfoMessage
    RespFileInfoMessage::RespFileInfoMessage() :
      file_info_(NULL)
    {
      _packetHeader._pcode = RESP_FILE_INFO_MESSAGE;
    }

    RespFileInfoMessage::~RespFileInfoMessage()
    {
    }

    int RespFileInfoMessage::parse(char* data, int32_t len)
    {
      int32_t size;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (get_object(&data, &len,  reinterpret_cast<void**>(&file_info_), FILEINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RespFileInfoMessage::message_length()
    {
      int32_t len = INT_SIZE;
      if (file_info_ != NULL)
      {
        len += FILEINFO_SIZE;
      }
      return len;
    }

    int RespFileInfoMessage::build(char* data, int32_t len)
    {
      int32_t size = 0;
      if (file_info_ != NULL)
      {
        size = FILEINFO_SIZE;
      }
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size > 0)
      {
        if (set_object(&data, &len, file_info_, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* RespFileInfoMessage::get_name()
    {
      return "respfileinfomessage";
    }

    Message* RespFileInfoMessage::create(const int32_t type)
    {
      RespFileInfoMessage* resp_fi_msg = new RespFileInfoMessage();
      resp_fi_msg->set_message_type(type);
      return resp_fi_msg;
    }
  }
}
