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

#include <read_data_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ReadRawdataMessageV2::ReadRawdataMessageV2(): degrade_(0)
    {
      _packetHeader._pcode = READ_RAWDATA_MESSAGE_V2;
    }

    ReadRawdataMessageV2::~ReadRawdataMessageV2()
    {
    }

    int ReadRawdataMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_seg_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(file_seg_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(degrade_);
      }

      return ret;
    }

    int ReadRawdataMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_seg_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(file_seg_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        // ignore return value for compatible
        input.get_int8(&degrade_);
      }
      return ret;
    }

    int64_t ReadRawdataMessageV2::length() const
    {
      return file_seg_.length() + INT8_SIZE;
    }

    ReadRawdataRespMessageV2::ReadRawdataRespMessageV2() :
      data_(NULL), length_(-1), alloc_(false)
    {
      _packetHeader._pcode = READ_RAWDATA_RESP_MESSAGE_V2;
    }

    ReadRawdataRespMessageV2::~ReadRawdataRespMessageV2()
    {
      if ((NULL != data_ ) && (alloc_))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* ReadRawdataRespMessageV2::alloc_data(const int32_t len)
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

    int ReadRawdataRespMessageV2::serialize(common::Stream& output) const
    {
      int ret = output.set_int32(length_);
      if (TFS_SUCCESS == ret)
      {
        if ((length_ > 0) && (NULL != data_))
        {
          ret = output.set_bytes(data_, length_);
        }
      }

      return ret;
    }

    int ReadRawdataRespMessageV2::deserialize(common::Stream& input)
    {
      int ret = input.get_int32(&length_);
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

    int64_t ReadRawdataRespMessageV2::length() const
    {
      int64_t len = common::INT_SIZE;
      if ((length_ > 0) && (NULL != data_))
      {
        len += length_;
      }
      return len;
    }

  }
}
