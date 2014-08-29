/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqign.zyd@taobao.com>
 *      - initial release
 *
 */

#include "client_ns_keepalive_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ClientNsKeepaliveMessage::ClientNsKeepaliveMessage():
      flag_(0)
    {
      _packetHeader._pcode= CLIENT_NS_KEEPALIVE_MESSAGE;
    }

    ClientNsKeepaliveMessage::~ClientNsKeepaliveMessage()
    {

    }

    int ClientNsKeepaliveMessage::deserialize(Stream& input)
    {
      return input.get_int32(&flag_);
    }

    int64_t ClientNsKeepaliveMessage::length() const
    {
      return INT_SIZE;
    }

    int ClientNsKeepaliveMessage::serialize(Stream& output) const
    {
      return output.set_int32(flag_);
    }

    ClientNsKeepaliveResponseMessage::ClientNsKeepaliveResponseMessage()
    {
      _packetHeader._pcode= CLIENT_NS_KEEPALIVE_RESPONSE_MESSAGE;
    }

    ClientNsKeepaliveResponseMessage::~ClientNsKeepaliveResponseMessage()
    {
    }

    int ClientNsKeepaliveResponseMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = config_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(config_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&interval_);
      }
      if (TFS_SUCCESS == ret)
      {
        int32_t size = 0;
        ret = input.get_int32(&size);
      }
      if (TFS_SUCCESS == ret && input.get_data_length() > 0)
      {
        data_.writeBytes(input.get_data(), input.get_data_length());
      }
      return ret;
    }

    int64_t ClientNsKeepaliveResponseMessage::length() const
    {
      return config_.length() + INT_SIZE * 2  + data_.getDataLen();
    }

    int ClientNsKeepaliveResponseMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = config_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(config_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        output.set_int32(interval_);
      }
      if (TFS_SUCCESS == ret)
      {
        // help nginx client to decode
        ret = output.set_int32(data_.getDataLen() / INT64_SIZE);
      }
      if (TFS_SUCCESS == ret && data_.getDataLen() > 0)
      {
        ret = output.set_bytes(data_.getData(), data_.getDataLen());
      }
      return ret;
    }
  }
}
