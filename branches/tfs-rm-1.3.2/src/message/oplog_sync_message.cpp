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
#include <Memory.hpp>
#include "oplog_sync_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    OpLogSyncMessage::OpLogSyncMessage() :
      alloc_(false), length_(0), data_(NULL)
    {
      _packetHeader._pcode = OPLOG_SYNC_MESSAGE;
    }

    OpLogSyncMessage::~OpLogSyncMessage()
    {
      if ((data_ != NULL) && alloc_)
      {
        tbsys::gDeleteA(data_);
      }
    }

    void OpLogSyncMessage::set_data(const char* data, int32_t length)
    {
      assert(length > 0);
      assert(data != NULL);
      tbsys::gDeleteA(data_);
      length_ = length;
      data_ = new char[length + 1];
      memcpy(data_, data, length);
      alloc_ = true;
    }

    int OpLogSyncMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (length_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&data_), length_) == TFS_ERROR)
          return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t OpLogSyncMessage::message_length()
    {
      int32_t i_len = INT_SIZE;
      if (length_ > 0 && data_ != NULL)
      {
        i_len += length_;
      }
      return i_len;
    }

    int OpLogSyncMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, length_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((length_ > 0) && (data_ != NULL))
      {
        if (set_object(&data, &len, data_, length_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* OpLogSyncMessage::get_name()
    {
      return "oplogsyncmessage";
    }

    Message* OpLogSyncMessage::create(const int32_t type)
    {
      OpLogSyncMessage* req_cls_msg = new OpLogSyncMessage();
      req_cls_msg->set_message_type(type);
      return req_cls_msg;
    }

    OpLogSyncResponeMessage::OpLogSyncResponeMessage() :
      complete_flag_(OPLOG_SYNC_MSG_COMPLETE_YES)
    {
      _packetHeader._pcode = OPLOG_SYNC_RESPONSE_MESSAGE;
    }

    OpLogSyncResponeMessage::~OpLogSyncResponeMessage()
    {

    }

    int OpLogSyncResponeMessage::parse(char* data, int32_t len)
    {
      assert(data != NULL);
      assert(len > 0);
      complete_flag_ = data[0];
      return TFS_SUCCESS;
    }

    int32_t OpLogSyncResponeMessage::message_length()
    {
      return sizeof(uint8_t);
    }

    int OpLogSyncResponeMessage::build(char* data, int32_t len)
    {
      assert(data != NULL);
      assert(len > 0);
      data[0] = complete_flag_;
      return TFS_SUCCESS;
    }

    char* OpLogSyncResponeMessage::get_name()
    {
      return "oplogsyncresponemessage";
    }

    Message* OpLogSyncResponeMessage::create(const int32_t type)
    {
      OpLogSyncResponeMessage* resp_ols_msg = new OpLogSyncResponeMessage();
      resp_ols_msg->set_message_type(type);
      return resp_ols_msg;
    }
  }
}
