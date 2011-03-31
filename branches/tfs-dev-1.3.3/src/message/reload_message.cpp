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
#include "reload_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ReloadConfigMessage::ReloadConfigMessage() :
      flag_(0)
    {
      _packetHeader._pcode = RELOAD_CONFIG_MESSAGE;
    }

    ReloadConfigMessage::ReloadConfigMessage(const int32_t flag)
    {
      _packetHeader._pcode = RELOAD_CONFIG_MESSAGE;
      set_message(flag);
    }

    void ReloadConfigMessage::set_message(const int32_t flag)
    {
      flag_ = flag;
    }

    ReloadConfigMessage::~ReloadConfigMessage()
    {
    }

    int ReloadConfigMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ReloadConfigMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return len;
    }

    int ReloadConfigMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* ReloadConfigMessage::get_name()
    {
      return "reloadconfigmessage";
    }

    Message* ReloadConfigMessage::create(const int32_t type)
    {
      ReloadConfigMessage* req_rc_msg = new ReloadConfigMessage();
      req_rc_msg->set_message_type(type);
      return req_rc_msg;
    }

    void ReloadConfigMessage::set_switch_cluster_flag(const int32_t flag)
    {
      flag_ = flag;
    }

    int32_t ReloadConfigMessage::get_switch_cluster_flag() const
    {
      return flag_;
    }
  }
}
