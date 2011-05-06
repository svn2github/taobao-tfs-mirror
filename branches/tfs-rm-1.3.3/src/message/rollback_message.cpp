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
#include "rollback_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    RollbackMessage::RollbackMessage() :
      act_type_(0), block_info_(NULL), file_info_(NULL)
    {
      _packetHeader._pcode = ROLLBACK_MESSAGE;
    }

    RollbackMessage::~RollbackMessage()
    {
    }

    int RollbackMessage::parse(char* data, int32_t len)
    {
      int32_t block_len = 0;
      int32_t file_info_len = 0;

      if (get_int32(&data, &len, &act_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &block_len) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &file_info_len) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (block_len > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**>(&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (file_info_len > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**>(&file_info_), FILEINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RollbackMessage::message_length()
    {
      int32_t block_len = 0;
      if (block_info_ != NULL)
      {
        block_len = BLOCKINFO_SIZE;
      }
      int32_t file_info_len = 0;
      if (file_info_ != NULL)
      {
        file_info_len = FILEINFO_SIZE;
      }
      int32_t len = INT_SIZE * 3 + block_len + file_info_len;
      return len;
    }

    int RollbackMessage::build(char* data, int32_t len)
    {
      int32_t block_len = 0;
      if (block_info_ != NULL)
      {
        block_len = BLOCKINFO_SIZE;
      }
      int32_t file_info_len = 0;
      if (file_info_ != NULL)
      {
        file_info_len = FILEINFO_SIZE;
      }
      if (set_int32(&data, &len, act_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, block_len) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, file_info_len) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (block_len > 0)
      {
        if (set_object(&data, &len, block_info_, block_len) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (file_info_len > 0)
      {
        if (set_object(&data, &len, file_info_, file_info_len) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* RollbackMessage::get_name()
    {
      return "rollbackmessage";
    }

    Message* RollbackMessage::create(const int32_t type)
    {
      RollbackMessage* req_r_msg = new RollbackMessage();
      req_r_msg->set_message_type(type);
      return req_r_msg;
    }
  }
}
