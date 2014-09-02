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

#include <write_index_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    WriteIndexMessageV2::WriteIndexMessageV2():
      tmp_flag_(0), partial_flag_(0), cluster_flag_(0)
    {
      _packetHeader._pcode = WRITE_INDEX_MESSAGE_V2;
    }

    WriteIndexMessageV2::~WriteIndexMessageV2()
    {
    }

    int WriteIndexMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        output.set_int64(attach_block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = index_data_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(index_data_.length());
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(tmp_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(partial_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(cluster_flag_);
      }

      return ret;
    }

    int WriteIndexMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t* >(&attach_block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = index_data_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(index_data_.length());
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&tmp_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&partial_flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&cluster_flag_);
      }

      return ret;
    }

    int64_t WriteIndexMessageV2::length() const
    {
      return INT64_SIZE * 2 + index_data_.length() + 3 * INT8_SIZE;
    }

  }
}
