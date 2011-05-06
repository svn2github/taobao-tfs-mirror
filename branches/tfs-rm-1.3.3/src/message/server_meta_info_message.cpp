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
#include "server_meta_info_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ServerMetaInfoMessage::ServerMetaInfoMessage() :
      status_(0), str_(NULL)
    {
      _packetHeader._pcode = SERVER_META_INFO_MESSAGE;
    }

    ServerMetaInfoMessage::~ServerMetaInfoMessage()
    {

    }

    Message* ServerMetaInfoMessage::create(const int32_t type)
    {
      ServerMetaInfoMessage* req_sii_msg = new ServerMetaInfoMessage();
      req_sii_msg->set_message_type(type);
      return req_sii_msg;
    }

    int ServerMetaInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_string(&data, &len, &str_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int ServerMetaInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_string(&data, &len, str_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ServerMetaInfoMessage::message_length()
    {
      return INT_SIZE + get_string_len(str_);
    }

    char* ServerMetaInfoMessage::get_name()
    {
      return "serverinodeinfomessage";
    }

    RespServerMetaInfoMessage::RespServerMetaInfoMessage() :
      server_meta_info_(NULL), length_(-1), alloc_(false) 
    {
      _packetHeader._pcode = RESP_SERVER_META_INFO_MESSAGE;
    }

    RespServerMetaInfoMessage::~RespServerMetaInfoMessage()
    {
      if ((server_meta_info_ != NULL) && (alloc_ == true))
      {
        ::free( server_meta_info_);
        server_meta_info_ = NULL;
      }
    }

    void RespServerMetaInfoMessage::alloc_data()
    {
      if ((server_meta_info_ != NULL) && (alloc_ == true))
      {
        ::free( server_meta_info_);
        server_meta_info_ = NULL;
      }
      server_meta_info_ = (ServerMetaInfo*) malloc(sizeof(ServerMetaInfo));
      length_ = sizeof(ServerMetaInfo);
      alloc_ = true;
    }

    int RespServerMetaInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_object(&data, &len, reinterpret_cast<void**>(&server_meta_info_), length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int RespServerMetaInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((length_ > 0) && (server_meta_info_ != NULL))
      {
        if (set_object(&data, &len, server_meta_info_, length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    Message* RespServerMetaInfoMessage::create(const int32_t type)
    {
      RespServerMetaInfoMessage* resp_sii_msg = new RespServerMetaInfoMessage();
      resp_sii_msg->set_message_type(type);
      return resp_sii_msg;
    }

    char* RespServerMetaInfoMessage::get_name()
    {
      return "RespServerMetaInfoMessage";
    }

    void RespServerMetaInfoMessage::set_data(const ServerMetaInfo* server_meta_info)
    {
      memcpy(server_meta_info_, server_meta_info, sizeof(ServerMetaInfo));
      length_ = sizeof(ServerMetaInfo);
    }

    int32_t RespServerMetaInfoMessage::message_length()
    {
      int32_t len = INT_SIZE;
      if ((length_ > 0) && (server_meta_info_ != NULL))
      {
        len += length_;
      }
      return len;
    }
  }
}
