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
namespace tfs
{
  namespace message
  {
    ReloadConfigMessage::ReloadConfigMessage() :
      flag_(0)
    {
      _packetHeader._pcode = common::RELOAD_CONFIG_MESSAGE;
    }

    ReloadConfigMessage::~ReloadConfigMessage()
    {

    }

    int ReloadConfigMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&flag_);
    }

    int64_t ReloadConfigMessage::length() const
    {
      return common::INT_SIZE;
    }

    int ReloadConfigMessage::serialize(common::Stream& input)
    {
      return input.set_int32(flag_);
    }

    common::BasePacket* ReloadConfigMessage::create(const int32_t type)
    {
      return new ReloadConfigMessage();
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
