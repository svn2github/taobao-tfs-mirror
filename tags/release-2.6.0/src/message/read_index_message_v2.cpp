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

#include <read_index_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ReadIndexMessageV2::ReadIndexMessageV2()
    {
      _packetHeader._pcode = READ_INDEX_MESSAGE_V2;
    }

    ReadIndexMessageV2::~ReadIndexMessageV2()
    {
    }

    int ReadIndexMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        output.set_int64(attach_block_id_);
      }
      return ret;
    }

    int ReadIndexMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t* >(&attach_block_id_));
      }
      return ret;
    }

    int64_t ReadIndexMessageV2::length() const
    {
      return INT64_SIZE * 2;
    }

    ReadIndexRespMessageV2::ReadIndexRespMessageV2()
    {
      _packetHeader._pcode = READ_INDEX_RESP_MESSAGE_V2;
    }

    ReadIndexRespMessageV2::~ReadIndexRespMessageV2()
    {
    }

    int ReadIndexRespMessageV2::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int ret = index_data_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(index_data_.length());
      }
      return ret;
    }

    int ReadIndexRespMessageV2::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = index_data_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(index_data_.length());
      }
      return ret;
    }

    int64_t ReadIndexRespMessageV2::length() const
    {
      return  index_data_.length();
    }

  }
}
