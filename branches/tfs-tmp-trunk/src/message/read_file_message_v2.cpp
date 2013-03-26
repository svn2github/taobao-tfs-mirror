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

#include <read_file_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    StatFileMessageV2::StatFileMessageV2():
      block_id_(INVALID_BLOCK_ID), attach_block_id_(INVALID_BLOCK_ID),
      file_id_(0), flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = STAT_FILE_MESSAGE_V2;
    }

    StatFileMessageV2::~StatFileMessageV2()
    {
    }

    int StatFileMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(file_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(attach_block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }

      if (TFS_SUCCESS == ret)
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

    int StatFileMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&file_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&attach_block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      if (TFS_SUCCESS == ret)
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

    int64_t StatFileMessageV2::length() const
    {
      return 3 * INT64_SIZE + INT_SIZE + family_info_.length();
    }

    StatFileRespMessageV2::StatFileRespMessageV2()
    {
      _packetHeader._pcode = STAT_FILE_RESP_MESSAGE_V2;
    }

    StatFileRespMessageV2::~StatFileRespMessageV2()
    {
    }

    int StatFileRespMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_info_.length());
      }
      return ret;
    }

    int StatFileRespMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_info_.length());
      }
      return ret;
    }

    int64_t StatFileRespMessageV2::length() const
    {
      return file_info_.length();
    }

    ReadFileMessageV2::ReadFileMessageV2():
      attach_block_id_(INVALID_BLOCK_ID), flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = READ_FILE_MESSAGE_V2;
    }

    ReadFileMessageV2::~ReadFileMessageV2()
    {
    }

    int ReadFileMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_seg_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_seg_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(attach_block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }

      if (TFS_SUCCESS == ret)
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

    int ReadFileMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_seg_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_seg_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&attach_block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      if (TFS_SUCCESS == ret)
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


    int64_t ReadFileMessageV2::length() const
    {
      return file_seg_.length() + INT64_SIZE + INT_SIZE + family_info_.length();
    }

    ReadFileRespMessageV2::ReadFileRespMessageV2() :
      data_(NULL), length_(-1), alloc_(false)
    {
      _packetHeader._pcode = READ_FILE_RESP_MESSAGE_V2;
    }

    ReadFileRespMessageV2::~ReadFileRespMessageV2()
    {
      if ((NULL != data_ ) && (alloc_))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* ReadFileRespMessageV2::alloc_data(const int32_t len)
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

    int ReadFileRespMessageV2::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(length_);
      }

      if (TFS_SUCCESS == ret)
      {
        if ((length_ > 0) && (NULL != data_))
        {
          ret = output.set_bytes(data_, length_);
        }
      }

      return ret;
    }

    int ReadFileRespMessageV2::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = file_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&length_);
      }

      if (TFS_SUCCESS == ret)
      {
        if (length_ > 0)
        {
          data_ = input.get_data();
          input.drain(length_);
        }
      }

      return ret;
    }

    int64_t ReadFileRespMessageV2::length() const
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
