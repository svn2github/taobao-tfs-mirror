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

#include <write_file_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    WriteFileMessage::WriteFileMessage():
      lease_id_(INVALID_LEASE_ID), data_(NULL), flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = WRITE_FILE_MESSAGE;
    }

    WriteFileMessage::~WriteFileMessage()
    {
    }

    int WriteFileMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_seg_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        ret = output.pour(file_seg_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(ds_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(lease_id_);
      }

      if (TFS_SUCCESS == ret && file_seg_.length_ > 0)
      {
        ret = output.set_bytes(data_, file_seg_.length_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }

      return ret;
    }

    int WriteFileMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_seg_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        ret = input.drain(file_seg_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(ds_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&lease_id_));
      }

      if (TFS_SUCCESS == ret && file_seg_.length_ > 0)
      {
        data_ = input.get_data();
        input.drain(file_seg_.length_);
      }

      if (TFS_SUCCESS == ret)
      {
        input.get_int32(&flag_);
      }

      return ret;
    }

    int64_t WriteFileMessage::length() const
    {
      int64_t len = file_seg_.length() +
                    Serialization::get_vint64_length(ds_) +
                    INT64_SIZE + INT_SIZE;
      if (file_seg_.length_ > 0)
      {
        len += (INT_SIZE + file_seg_.length_);
      }

      return len;
    }

    WriteFileRespMessage::WriteFileRespMessage():
      file_id_(0), lease_id_(INVALID_LEASE_ID)
    {
      _packetHeader._pcode = WRITE_FILE_RESP_MESSAGE;
    }

    WriteFileRespMessage::~WriteFileRespMessage()
    {
    }

    int WriteFileRespMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(file_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(lease_id_);
      }
      return ret;
    }

    int WriteFileRespMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&file_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&lease_id_));
      }
      return ret;
    }

    int64_t WriteFileRespMessage::length() const
    {
      return 2 * INT64_SIZE;
    }

    SlaveDsRespMessage::SlaveDsRespMessage():
      server_id_(INVALID_SERVER_ID)
    {
      _packetHeader._pcode = SLAVE_DS_RESP_MESSAGE;
    }

    SlaveDsRespMessage::~SlaveDsRespMessage()
    {
    }

    int SlaveDsRespMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(server_id_);
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(block_info_.length());
        }
      }
      return ret;
    }

    int SlaveDsRespMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(server_id_));
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(block_info_.length());
        }
      }
      return ret;
    }

    int64_t SlaveDsRespMessage::length() const
    {
      return (INT64_SIZE + block_info_.length());
    }

  }
}
