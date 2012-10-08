/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: unlink_file_message.cpp 381 2011-05-30 08:07:39Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "unlink_file_message.h"
namespace tfs
{
  namespace message
  {
    int UnlinkFileInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &is_server_);
      }
      return ret;
    }

    int UnlinkFileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (common::TFS_SUCCESS  == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, is_server_);
      }
      return ret;
    }
    int64_t UnlinkFileInfo::length() const
    {
      return common::INT_SIZE * 2 + common::INT64_SIZE;
    }

    UnlinkFileMessage::UnlinkFileMessage() :
      option_flag_(0), version_(0), lease_id_(common::INVALID_LEASE_ID)
    {
      _packetHeader._pcode = common::UNLINK_FILE_MESSAGE;
      memset(&unlink_file_info_, 0, sizeof(UnlinkFileInfo));
    }

    UnlinkFileMessage::~UnlinkFileMessage()
    {
    }

    int UnlinkFileMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = unlink_file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(unlink_file_info_.length());
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(dataservers_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        input.get_int32(&option_flag_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        BasePacket::parse_special_ds(dataservers_, version_, lease_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&lease_id_ext_));
      }
      return ret;
    }

    int64_t UnlinkFileMessage::length() const
    {
      int64_t len = common::INT64_SIZE + unlink_file_info_.length() + common::Serialization::get_vint64_length(dataservers_) + common::INT_SIZE;
      if (has_lease())
      {
        len += common::INT64_SIZE * 3;
      }
      return len;
    }

    int UnlinkFileMessage::serialize(common::Stream& output) const
    {
      if (has_lease())
      {
        dataservers_.push_back(ULONG_LONG_MAX);
        dataservers_.push_back(static_cast<uint64_t> (version_));
        dataservers_.push_back(static_cast<uint64_t> (lease_id_));
      }

      int64_t pos = 0;
      int32_t ret = unlink_file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        output.pour(unlink_file_info_.length());
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(dataservers_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(option_flag_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        BasePacket::parse_special_ds(dataservers_, version_, lease_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(lease_id_ext_);
      }
      return ret;
    }

    UnlinkFileResponseMessage::UnlinkFileResponseMessage():
      lease_id_(common::INVALID_LEASE_ID),
      server_(common::INVALID_SERVER_ID),
      status_(common::TFS_ERROR)
    {
      _packetHeader._pcode = common::RSP_UNLINK_FILE_MESSAGE;
    }

    int UnlinkFileResponseMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(reinterpret_cast<int64_t*>(&lease_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&server_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == ret)
          input.drain(info_.length());
      }
      return ret;
    }

    int UnlinkFileResponseMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(lease_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
          output.pour(info_.length());
      }
      return ret;
    }

    int64_t UnlinkFileResponseMessage::length() const
    {
      return common::INT64_SIZE + common::INT_SIZE  + info_.length();
    }
  }
}
