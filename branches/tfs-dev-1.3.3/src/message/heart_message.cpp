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
#include "heart_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    // RespHeartMessage
    RespHeartMessage::RespHeartMessage() :
      status_(0), sync_mirror_status_(0)
    {
      _packetHeader._pcode = RESP_HEART_MESSAGE;
      expire_blocks_.clear();
    }

    RespHeartMessage::~RespHeartMessage()
    {
    }

    int RespHeartMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &sync_mirror_status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint32(&data, &len, expire_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      //	TBSYS_LOG(DEBUG,"heart status = %d,len = %d",status_,len);
      return TFS_SUCCESS;
    }

    int32_t RespHeartMessage::message_length()
    {
      int32_t len = INT_SIZE * 2 + get_vint_len(expire_blocks_) + get_vint_len(new_blocks_);
      return len;
    }

    int RespHeartMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, sync_mirror_status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint32(&data, &len, expire_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* RespHeartMessage::get_name()
    {
      return "respheartmessage";
    }

    Message* RespHeartMessage::create(const int32_t type)
    {
      RespHeartMessage* resp_h_msg = new RespHeartMessage();
      resp_h_msg->set_message_type(type);
      return resp_h_msg;
    }

    MasterAndSlaveHeartMessage::MasterAndSlaveHeartMessage()
    {
      _packetHeader._pcode = MASTER_AND_SLAVE_HEART_MESSAGE;
      ::memset(&ns_identity_, 0, sizeof(ns_identity_));
    }

    MasterAndSlaveHeartMessage::~MasterAndSlaveHeartMessage()
    {

    }

    int MasterAndSlaveHeartMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, &ns_identity_, sizeof(ns_identity_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, &ns_identity_, sizeof(ns_identity_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t MasterAndSlaveHeartMessage::message_length()
    {
      return sizeof(ns_identity_);
    }
    char* MasterAndSlaveHeartMessage::get_name()
    {
      return "masterandslaveheartmessage";
    }

    Message* MasterAndSlaveHeartMessage::create(const int32_t type)
    {
      MasterAndSlaveHeartMessage* req_mash_msg = new MasterAndSlaveHeartMessage();
      req_mash_msg->set_message_type(type);
      return req_mash_msg;
    }

    MasterAndSlaveHeartResponseMessage::MasterAndSlaveHeartResponseMessage()
    {
      _packetHeader._pcode = MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE;
      ::memset(&ns_identity_, 0, sizeof(ns_identity_));
    }

    MasterAndSlaveHeartResponseMessage::~MasterAndSlaveHeartResponseMessage()
    {

    }

    int MasterAndSlaveHeartResponseMessage::parse(char* data, int32_t len)
    {
      if (get_object_copy(&data, &len, &ns_identity_, sizeof(ns_identity_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (ns_identity_.flags_ == HEART_GET_DATASERVER_LIST_FLAGS_YES)
      {
        if (get_vint64(&data, &len, ds_list_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartResponseMessage::build(char* data, int32_t len)
    {
      if (set_object(&data, &len, &ns_identity_, sizeof(ns_identity_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (ns_identity_.flags_ == HEART_GET_DATASERVER_LIST_FLAGS_YES)
      {
        if (set_vint64(&data, &len, ds_list_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t MasterAndSlaveHeartResponseMessage::message_length()
    {
      int32_t length = sizeof(ns_identity_);
      if (ns_identity_.flags_ == HEART_GET_DATASERVER_LIST_FLAGS_YES)
      {
        length += get_vint64_len(ds_list_);
      }
      return length;
    }

    char* MasterAndSlaveHeartResponseMessage::get_name()
    {
      return "masterandslaveheartresponemessage";
    }

    Message* MasterAndSlaveHeartResponseMessage::create(const int32_t type)
    {
      MasterAndSlaveHeartResponseMessage* resp_mash_msg = new MasterAndSlaveHeartResponseMessage();
      resp_mash_msg->set_message_type(type);
      return resp_mash_msg;
    }

    HeartBeatAndNSHeartMessage::HeartBeatAndNSHeartMessage() :
      flags_(0)
    {
      _packetHeader._pcode = HEARTBEAT_AND_NS_HEART_MESSAGE;
    }

    HeartBeatAndNSHeartMessage::~HeartBeatAndNSHeartMessage()
    {

    }
    int HeartBeatAndNSHeartMessage::parse(char* data, int32_t len)
    {
      return get_int32(&data, &len, &flags_);
    }

    int HeartBeatAndNSHeartMessage::build(char* data, int32_t len)
    {
      return set_int32(&data, &len, flags_);
    }

    int32_t HeartBeatAndNSHeartMessage::message_length()
    {
      return INT_SIZE;
    }

    char* HeartBeatAndNSHeartMessage::get_name()
    {
      return "HeartBeatAndNSHeartMessage";
    }

    Message* HeartBeatAndNSHeartMessage::create(const int32_t type)
    {
      HeartBeatAndNSHeartMessage* req_hbansh_msg = new HeartBeatAndNSHeartMessage();
      req_hbansh_msg->set_message_type(type);
      return req_hbansh_msg;
    }

    OwnerCheckMessage::OwnerCheckMessage() :
      start_time_(0)
    {
      _packetHeader._pcode = OWNER_CHECK_MESSAGE;
    }

    OwnerCheckMessage::~OwnerCheckMessage()
    {

    }

    int OwnerCheckMessage::parse(char* , int32_t)
    {
      return TFS_SUCCESS;
    }

    int OwnerCheckMessage::build(char* , int32_t)
    {
      return TFS_SUCCESS;
    }

    int OwnerCheckMessage::message_length()
    {
      return TFS_SUCCESS;
    }

    char* OwnerCheckMessage::get_name()
    {
      return "wonercheckmessage";
    }
  }
}
