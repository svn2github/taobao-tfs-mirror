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
#include "create_filename_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    // CreateFilenameMessage 
    CreateFilenameMessage::CreateFilenameMessage() :
      block_id_(0), file_id_(0)
    {
      _packetHeader._pcode = CREATE_FILENAME_MESSAGE;
    }

    CreateFilenameMessage::~CreateFilenameMessage()
    {
    }

    int CreateFilenameMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      get_int64(&data, &len, reinterpret_cast<int64_t*> (&file_id_));
      return TFS_SUCCESS;
    }

    int32_t CreateFilenameMessage::message_length()
    {
      int32_t len = INT_SIZE + INT64_SIZE;
      return len;
    }

    int CreateFilenameMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, file_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* CreateFilenameMessage::get_name()
    {
      return "createfilenamemessage";
    }

    Message* CreateFilenameMessage::create(const int32_t type)
    {
      CreateFilenameMessage* req_cf_msg = new CreateFilenameMessage();
      req_cf_msg->set_message_type(type);
      return req_cf_msg;
    }

    // RespCreateFilenameMessage
    RespCreateFilenameMessage::RespCreateFilenameMessage() :
      block_id_(0), file_id_(0), file_number_(0)
    {
      _packetHeader._pcode = RESP_CREATE_FILENAME_MESSAGE;
    }

    RespCreateFilenameMessage::~RespCreateFilenameMessage()
    {
    }

    int RespCreateFilenameMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&file_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&file_number_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t RespCreateFilenameMessage::message_length()
    {
      int32_t len = INT_SIZE + INT64_SIZE * 2;
      return len;
    }

    int RespCreateFilenameMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, file_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, file_number_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* RespCreateFilenameMessage::get_name()
    {
      return "respcreatefilenamemessage";
    }

    Message* RespCreateFilenameMessage::create(const int32_t type)
    {
      RespCreateFilenameMessage* resp_cf_msg = new RespCreateFilenameMessage();
      resp_cf_msg->set_message_type(type);
      return resp_cf_msg;
    }
  }
}
