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
#include "status_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    StatusMessage::StatusMessage() :
      status_(0), str_(NULL)
    {
      _packetHeader._pcode = STATUS_MESSAGE;
    }

    StatusMessage::StatusMessage(const int32_t status, char* const str)
    {
      _packetHeader._pcode = STATUS_MESSAGE;
      set_message(status, str);
    }

    void StatusMessage::set_message(const int32_t status, char* const str)
    {
      status_ = status;
      str_ = str;
    }

    StatusMessage::~StatusMessage()
    {
    }

    int StatusMessage::parse(char* data, int32_t len)
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

    int32_t StatusMessage::message_length()
    {
      int32_t len = INT_SIZE + get_string_len(str_);
      return len;
    }

    int StatusMessage::build(char* data, int32_t len)
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

    char* StatusMessage::get_name()
    {
      return "statusmessage";
    }

    Message* StatusMessage::create(const int32_t type)
    {
      StatusMessage* req_s_msg = new StatusMessage();
      req_s_msg->set_message_type(type);
      return req_s_msg;
    }

    char* StatusMessage::get_error() const
    {
      return str_;
    }

    int32_t StatusMessage::get_status() const
    {
      return status_;
    }
  }
}
