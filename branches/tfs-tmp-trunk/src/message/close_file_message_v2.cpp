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
    CloseFileMessageV2::CloseFileMessageV2()
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
        ret = output.set_vint64(ds_);
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
        ret = input.get_vint64(ds_);
      }

      return ret;

    }

    int64_t CloseFileMessageV2::length() const
    {
      return 4 * INT64_SIZE + 2 * INT_SIZE +
             Serialization::get_vint64_length(ds_);
    }


  }
}
