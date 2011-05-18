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
#include "status_packet.h"
namespace tfs
{
  namespace common 
  {
    StatusPacket::StatusPacket() :
      length_(0),
      status_(0)
    {
      _packetHeader._pcode = STATUS_PACKET;
    }

    StatusPacket::StatusPacket(const int32_t status, const char* const str)
    {
      _packetHeader._pcode = STATUS_PACKET;
      set_message(status, str);
    }

    void StatusPacket::set_message(const int32_t status, const char* const str)
    {
      status_ = status;
      if (NULL != str)
      {
        length_ = strlen(str);
        if (length_ > MAX_ERROR_MSG_LENGTH - 1)
        {
          length_ = MAX_ERROR_MSG_LENGTH - 1;
        }
        memcpy(msg_, str, length_);
        msg_[length_] = '\0';
      }
    }

    StatusPacket::~StatusPacket()
    {

    }

    int StatusPacket::serialize(Stream& output)
    {
      int32_t iret = output.set_int32(status_);
      if (TFS_SUCCESS == iret)
      {
        iret = output.set_int32(length_);
        if (length_ > 0)
        {
          if (TFS_SUCCESS == iret)
          {
            iret = output.set_bytes(msg_, length_);
          }
        }
      }
      return iret;
    }

    int StatusPacket::deserialize(Stream& input)
    {
      int32_t iret = input.get_int32(&status_);
      if (TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&length_);
        if (TFS_SUCCESS == iret)
        {
          if (length_ > MAX_ERROR_MSG_LENGTH)
          {
            iret = TFS_ERROR;
            TBSYS_LOG(ERROR, "error msg length: %d less than : %d", length_, MAX_ERROR_MSG_LENGTH);
          }
          else
          {
            if (length_ > 0)
            {
              iret = input.get_bytes(msg_, length_); 
            }
          }
        }
      }
      return iret;
    }

    int64_t StatusPacket::length() const
    {
      int64_t tmp = length_ > 0 ? INT_SIZE + length_ : INT_SIZE;
      return INT_SIZE + tmp;
    }

    void StatusPacket::dump() const
    {
      TBSYS_LOG(DEBUG, "status: %d, error msg: %s", status_, NULL == msg_ ? "null" : msg_); 
    }

    BasePacket* StatusPacket::create(const int32_t type)
    {
      return new (std::nothrow) StatusPacket();
    }

    const char* StatusPacket::get_error() const
    {
      return msg_;
    }

    int32_t StatusPacket::get_status() const
    {
      return status_;
    }
  }
}
