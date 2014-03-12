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
#include <write_data_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    WriteRawdataMessageV2::WriteRawdataMessageV2():
      data_(NULL), tmp_flag_(0), new_flag_(0)
    {
      _packetHeader._pcode = WRITE_RAWDATA_MESSAGE_V2;
    }

    WriteRawdataMessageV2::~WriteRawdataMessageV2()
    {
    }

    int WriteRawdataMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = file_seg_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        ret = output.pour(file_seg_.length());
      }

      if (TFS_SUCCESS == ret && file_seg_.length_ > 0)
      {
        ret = output.set_bytes(data_, file_seg_.length_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(tmp_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(new_flag_);
      }

      return ret;
    }

    int WriteRawdataMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = file_seg_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        ret = input.drain(file_seg_.length());
      }

      if (TFS_SUCCESS == ret && file_seg_.length_ > 0)
      {
        data_ = input.get_data();
        input.drain(file_seg_.length_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&tmp_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&new_flag_);
      }

      return ret;
    }

    int64_t WriteRawdataMessageV2::length() const
    {
      int64_t len = file_seg_.length() + INT8_SIZE * 2;
      if (file_seg_.length_ > 0)
      {
        len += file_seg_.length_;
      }

      return len;
    }

  }
}
