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
#include "client_cmd_message.h"
namespace tfs
{
  namespace message
  {
    int ClientCmdInformation::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, cmd_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int64(data, data_len, pos, value1_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, value3_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int32(data, data_len, pos, value4_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::set_int64(data, data_len, pos, value2_);
      }
      return iret;
    }

    int ClientCmdInformation::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&cmd_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value1_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&value3_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&value4_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value2_));
      }
      return iret;
    }

    int64_t ClientCmdInformation::length() const
    {
      return common::INT_SIZE * 3 + common::INT64_SIZE * 2;
    }

    ClientCmdMessage::ClientCmdMessage()
    {
      _packetHeader._pcode = common::CLIENT_CMD_MESSAGE;
      memset(&info_, 0, sizeof(ClientCmdInformation));
    }

    ClientCmdMessage::~ClientCmdMessage()
    {

    }

    int ClientCmdMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.pour(info_.length());
      }
      return iret;
    }

    int64_t ClientCmdMessage::length() const
    {
      return info_.length();
    }

    int ClientCmdMessage::serialize(common::Stream& output) const 
    {
      int64_t pos = 0;
      int32_t iret = info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        output.pour(info_.length());
      }
      return iret;
    }
  }
}
