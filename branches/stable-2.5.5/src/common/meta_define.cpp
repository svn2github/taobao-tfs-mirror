/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */

#include "meta_define.h"
#include "stream.h"

namespace tfs
{
  namespace common
  {
    FragMeta::FragMeta() :
      file_id_(0), offset_(0), block_id_(0), size_(0)
    {
    }

    FragMeta::FragMeta(uint32_t block_id, uint64_t file_id, int64_t offset, int32_t size) :
      file_id_(file_id), offset_(offset), block_id_(block_id), size_(size)
    {
    }

    int64_t FragMeta::get_length()
    {
      return 2 * sizeof(int64_t) + 2 * sizeof(int32_t);
    }

    bool FragMeta::operator < (const FragMeta& right) const
    {
      return offset_ < right.offset_;
    }

    int FragMeta::serialize(char* data, const int64_t buff_len, int64_t& pos) const
    {
      int ret = common::TFS_ERROR;
      if (buff_len - pos >= get_length())
      {
        common::Serialization::set_int32(data, buff_len, pos, block_id_);
        common::Serialization::set_int64(data, buff_len, pos, file_id_);
        common::Serialization::set_int64(data, buff_len, pos, offset_);
        common::Serialization::set_int32(data, buff_len, pos, size_);
        ret = common::TFS_SUCCESS;
      }
      return ret;
    }

    int FragMeta::deserialize(char* data, const int64_t data_len, int64_t& pos)
    {
      int ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &size_);
      }
      return ret;
    }

    int FragMeta::serialize(common::Stream& output) const
    {
      int ret = common::TFS_ERROR;
      ret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(file_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }
      return ret;
    }

    int FragMeta::deserialize(common::Stream& input)
    {
      int ret = input.get_int32(reinterpret_cast<int32_t*>(&block_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&file_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
      }
      return ret;
    }
  }
}


