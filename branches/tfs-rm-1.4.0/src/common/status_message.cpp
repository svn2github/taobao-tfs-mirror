/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: status_packet.cpp 186 2011-04-28 16:07:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "status_message.h"
namespace tfs
{
  namespace common 
  {
    StatusMessage::StatusMessage() :
      length_(0),
      status_(0)
    {
      memset(msg_, 0, sizeof(msg_));
      _packetHeader._pcode = STATUS_MESSAGE;
    }

    StatusMessage::StatusMessage(const int32_t status, const char* const str)
    {
      _packetHeader._pcode = STATUS_MESSAGE;
      set_message(status, str);
    }

    void StatusMessage::set_message(const int32_t status, const char* const str)
    {
      status_ = status;
      if (NULL != str)
      {
        length_ = Serialization::get_string_length(str);
        if (length_ > MAX_ERROR_MSG_LENGTH + 1)
        {
          length_ = MAX_ERROR_MSG_LENGTH + 1;
        }
        TBSYS_LOG(DEBUG, "LENGTH: %d", length_);
        memcpy(msg_, str, length_ - 1);//include '\0'
        msg_[length_ - 1] = '\0';
      }
    }

    StatusMessage::~StatusMessage()
    {

    }

    int StatusMessage::serialize(Stream& output) const
    {
      int32_t iret = output.set_int32(status_);
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_string(msg_);
      }
      return iret;
    }

    int StatusMessage::deserialize(Stream& input)
    {
      int32_t iret = input.get_int32(&status_);
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_string(msg_, MAX_ERROR_MSG_LENGTH);
        if (TFS_SUCCESS == iret)
        {
          msg_[MAX_ERROR_MSG_LENGTH] = '\0';
        }
      }
      return iret;
    }

    int64_t StatusMessage::length() const
    {
      return INT_SIZE + Serialization::get_string_length(msg_);
    }

    const char* StatusMessage::get_error() const
    {
      return msg_;
    }

    int32_t StatusMessage::get_status() const
    {
      return status_;
    }
  }
}
