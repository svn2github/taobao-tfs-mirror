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

#include <close_file_message_v2.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    CloseFileMessageV2::CloseFileMessageV2():
      attach_block_id_(INVALID_BLOCK_ID),
      lease_id_(0), master_id_(0), crc_(0), flag_(0), status_(-1), tmp_(0)
    {
      _packetHeader._pcode = CLOSE_FILE_MESSAGE_V2;
    }

    CloseFileMessageV2::~CloseFileMessageV2()
    {
    }

    int CloseFileMessageV2::serialize(Stream& output) const
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
        ret = output.set_int64(lease_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(master_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(crc_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(status_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(tmp_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(ds_);
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

    int CloseFileMessageV2::deserialize(Stream& input)
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
        ret = input.get_int64(reinterpret_cast<int64_t *>(&lease_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&master_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(reinterpret_cast<int32_t *>(&crc_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&flag_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&status_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&tmp_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(ds_);
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

    int64_t CloseFileMessageV2::length() const
    {
      return 5 * INT64_SIZE + 3 * INT_SIZE + INT8_SIZE +
             Serialization::get_vint64_length(ds_)
             + family_info_.length();
    }

  }
}
