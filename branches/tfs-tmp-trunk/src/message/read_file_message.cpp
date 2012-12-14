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
      int64_t pos = 0;
      int ret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_info_.length());
      }
      return ret;
    }

    int StatFileRespMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_info_.length());
      }
      return ret;
    }

    int64_t StatFileRespMessage::length() const
    {
      return file_info_.length();
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

    ReadFileRespMessage::ReadFileRespMessage() :
      data_(NULL), length_(-1), alloc_(false)
    {
      _packetHeader._pcode = READ_FILE_RESP_MESSAGE;
    }

    ReadFileRespMessage::~ReadFileRespMessage()
    {
      if ((NULL != data_ ) && (alloc_))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* ReadFileRespMessage::alloc_data(const int32_t len)
    {
      if (len < 0)
      {
        return NULL;
      }
      if (len == 0)
      {
        length_ = len;
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      length_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    int ReadFileRespMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(length_);
      if (TFS_SUCCESS == ret)
      {
        if ((length_ > 0) && (NULL != data_))
        {
          ret = output.set_bytes(data_, length_);
        }
      }

      if (TFS_SUCCESS == ret)
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

    int ReadFileRespMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32(&length_);
      if (TFS_SUCCESS == ret)
      {
        if (length_ > 0)
        {
          data_ = input.get_data();
          input.drain(length_);
        }
      }

      if (TFS_SUCCESS == ret)
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

    int64_t ReadFileRespMessage::length() const
    {
      int64_t len = common::INT_SIZE + file_info_.length();
      if ((length_ > 0) && (NULL != data_))
      {
        len += length_;
      }
      return len;
    }

  }
}
