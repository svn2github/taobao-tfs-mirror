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

#include <read_file_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    StatFileMessage::StatFileMessage():
      block_id_(INVALID_BLOCK_ID), file_id_(0), flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = STAT_FILE_MESSAGE;
    }

    StatFileMessage::~StatFileMessage()
    {
    }

    int StatFileMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(file_id_);
      }

      int32_t tmp_flag = 0;
      if (TFS_SUCCESS == ret)
      {
        if (INVALID_FAMILY_ID != family_info_.family_id_)
        {
          tmp_flag = (flag_ | MF_WITH_FAMILY);
        }
        ret = output.set_int32(tmp_flag);
      }

      if ((TFS_SUCCESS == ret) && (tmp_flag & MF_WITH_FAMILY))
      {
        int64_t pos = 0;
        ret = family_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(family_info_.length());
        }
      }

      return ret;
    }

    int StatFileMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&file_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      if ((TFS_SUCCESS == ret) && (flag_ & MF_WITH_FAMILY))
      {
        int64_t pos = 0;
        ret = family_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(family_info_.length());
        }
      }

      return ret;

    }

    int64_t StatFileMessage::length() const
    {
      int64_t len = 2 * INT64_SIZE + INT_SIZE;
      if (INVALID_FAMILY_ID != family_info_.family_id_)
      {
        len += family_info_.length();
      }
      return len;
    }

    StatFileRespMessage::StatFileRespMessage()
    {
      _packetHeader._pcode = STAT_FILE_RESP_MESSAGE;
    }

    StatFileRespMessage::~StatFileRespMessage()
    {
    }

    int StatFileRespMessage::serialize(Stream& output) const
    {
      int ret = StatusMessage::serialize(output);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        int64_t pos = 0;
        ret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(file_info_.length());
        }
      }
      return ret;
    }

    int StatFileRespMessage::deserialize(Stream& input)
    {
      int ret = StatusMessage::deserialize(input);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        int64_t pos = 0;
        ret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(file_info_.length());
        }
      }
      return ret;
    }

    int64_t StatFileRespMessage::length() const
    {
      int64_t len = StatusMessage::length();
      if (STATUS_MESSAGE_OK == status_)
      {
        len += file_info_.length();
      }
      return len;
    }

    ReadFileMessage::ReadFileMessage():
      flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = READ_FILE_MESSAGE;
    }

    ReadFileMessage::~ReadFileMessage()
    {
    }

    int ReadFileMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_seg_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_seg_.length());
      }

      int32_t tmp_flag = flag_;
      if (TFS_SUCCESS == ret)
      {
        if (INVALID_FAMILY_ID != family_info_.family_id_)
        {
          tmp_flag = (flag_ | MF_WITH_FAMILY);
        }
        ret = output.set_int32(tmp_flag);
      }

      if ((TFS_SUCCESS == ret) && (tmp_flag & MF_WITH_FAMILY))
      {
        ret = family_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(family_info_.length());
        }
      }

      return ret;
    }

    int ReadFileMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_seg_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_seg_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      if ((TFS_SUCCESS == ret) && (flag_ & MF_WITH_FAMILY))
      {
        int64_t pos = 0;
        ret = family_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(family_info_.length());
        }
      }

      return ret;
    }


    int64_t ReadFileMessage::length() const
    {
      int64_t len = file_seg_.length() + INT_SIZE;
      if (INVALID_FAMILY_ID != family_info_.family_id_)
      {
        len += family_info_.length();
      }
      return len;
    }

    ReadFileRespMessage::ReadFileRespMessage():
      data_(NULL), length_(0)
    {
      _packetHeader._pcode = READ_FILE_RESP_MESSAGE;
    }

    ReadFileRespMessage::~ReadFileRespMessage()
    {
      if (NULL != data_)
      {
        tbsys::gDeleteA(data_);
      }
    }

    int ReadFileRespMessage::serialize(Stream& output) const
    {
      int ret = StatusMessage::serialize(output);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        ret = output.set_int32(length_);
        if (TFS_SUCCESS == ret)
        {
          if ((data_ != NULL) && (length_ > 0))
          {
            ret = output.set_bytes(data_, length_);
          }
        }
      }
      return ret;
    }

    int ReadFileRespMessage::deserialize(Stream& input)
    {
      int ret = StatusMessage::deserialize(input);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        ret = input.get_int32(&length_);
        if (TFS_SUCCESS == ret && length_ > 0)
        {
          data_ = input.get_data();
          input.drain(length_);
        }
      }
      return ret;
    }

    int64_t ReadFileRespMessage::length() const
    {
      int64_t len = StatusMessage::length();
      if (STATUS_MESSAGE_OK == status_)
      {
        len += (INT_SIZE + length_);
      }
      return len;
    }

    ReadFileV2Message::ReadFileV2Message()
    {
      _packetHeader._pcode = READ_FILE_V2_MESSAGE;
    }

    ReadFileV2Message::~ReadFileV2Message()
    {
    }

    int ReadFileV2Message::serialize(Stream& output) const
    {
      return ReadFileMessage::serialize(output);
    }

    int ReadFileV2Message::deserialize(Stream& input)
    {
      return ReadFileMessage::deserialize(input);
    }

    int64_t ReadFileV2Message::length() const
    {
      return ReadFileMessage::length();
    }

    ReadFileV2RespMessage::ReadFileV2RespMessage()
    {
      _packetHeader._pcode = READ_FILE_V2_RESP_MESSAGE;
    }

    ReadFileV2RespMessage::~ReadFileV2RespMessage()
    {

    }

    int ReadFileV2RespMessage::serialize(Stream& output) const
    {
      int ret = ReadFileRespMessage::serialize(output);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        int64_t pos = 0;
        ret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(file_info_.length());
        }
      }
      return ret;
    }

    int ReadFileV2RespMessage::deserialize(Stream& input)
    {
      int ret = ReadFileRespMessage::deserialize(input);
      if ((TFS_SUCCESS == ret) && (STATUS_MESSAGE_OK == status_))
      {
        int64_t pos = 0;
        ret = file_info_.serialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(file_info_.length());
        }
      }
      return ret;
    }

    int64_t ReadFileV2RespMessage::length() const
    {
      int64_t len = ReadFileRespMessage::length();
      if (STATUS_MESSAGE_OK == status_)
      {
        len += file_info_.length();
      }
      return len;
    }

  }
}
