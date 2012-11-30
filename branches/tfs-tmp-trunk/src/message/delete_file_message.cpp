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

#include <delete_file_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    DeleteFileMessage::DeleteFileMessage():
      block_id_(INVALID_BLOCK_ID), file_id_(0),
      action_(0), flag_(INVALID_FLAG)
    {
      _packetHeader._pcode = DELETE_FILE_MESSAGE;
    }

    DeleteFileMessage::~DeleteFileMessage()
    {
    }

    int DeleteFileMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(file_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(action_);
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

    int DeleteFileMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&file_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&action_);
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

    int64_t DeleteFileMessage::length() const
    {
      int64_t len = 2 * INT64_SIZE + 2 * INT_SIZE +
        Serialization::get_vint64_length(ds_);
      if (INVALID_FAMILY_ID != family_info_.family_id_)
      {
        len += family_info_.length();
      }
      return len;
    }

  }
}
