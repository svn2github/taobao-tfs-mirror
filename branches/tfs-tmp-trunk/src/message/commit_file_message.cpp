/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Authors:
*   linqing <linqing.zyd@taobao.com>
*      - initial release
*
*/

#include <commit_file_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    CommitFileMessage::CommitFileMessage()
    {
      _packetHeader._pcode = COMMIT_FILE_MESSAGE;
    }

    CommitFileMessage::~CommitFileMessage()
    {
    }

    int CommitFileMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(file_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(lease_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(ds_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(crc_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }

      return ret;
    }

    int CommitFileMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&file_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&lease_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(ds_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(reinterpret_cast<int32_t *>(&crc_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      return ret;

    }

    int64_t CommitFileMessage::length() const
    {
      return 3 * INT64_SIZE + 2 * INT_SIZE +
             Serialization::get_vint64_length(ds_);
    }

    CommitBlockUpdateMessage::CommitBlockUpdateMessage()
    {
      _packetHeader._pcode = COMMIT_BLOCK_UPDATE_MESSAGE;
    }

    CommitBlockUpdateMessage::~CommitBlockUpdateMessage()
    {
    }

    int CommitBlockUpdateMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(block_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(oper_);
      }

      return ret;
    }

    int CommitBlockUpdateMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(block_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&server_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(reinterpret_cast<int32_t *>(&oper_));
      }

      return ret;
    }

    int64_t CommitBlockUpdateMessage::length() const
    {
      return block_info_.length() + INT64_SIZE + INT_SIZE;
    }

  }
}
