/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: resolve_block_version_conflict_message.cpp 384 2012-08-21 09:47:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "common/stream.h"
#include "common/serialization.h"

#include "resolve_block_version_conflict_message.h"

namespace tfs
{
  namespace message
  {
    ResolveBlockVersionConflictMessage::ResolveBlockVersionConflictMessage():
      block_(common::INVALID_BLOCK_ID)
    {
      _packetHeader._pcode = common::REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    ResolveBlockVersionConflictMessage::~ResolveBlockVersionConflictMessage()
    {

    }

    int ResolveBlockVersionConflictMessage::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int32_t ret = input.get_int32(reinterpret_cast<int32_t*>(&block_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size);
      }
      if (common::TFS_SUCCESS == ret)
      {
        std::pair<uint64_t, common::BlockInfo> item;
        for (int32_t index = 0; index < size && common::TFS_SUCCESS == ret; ++index)
        {
          ret = input.get_int64(reinterpret_cast<int64_t*>(&item.first));
          if (common::TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = item.second.deserialize(input.get_data(), input.get_data_length(), pos);
            if (common::TFS_SUCCESS == ret)
              input.drain(item.second.length());
          }
          if (common::TFS_SUCCESS == ret)
          {
            members_.push_back(item);
          }
        }
      }
      return ret;
    }

    int ResolveBlockVersionConflictMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(block_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(members_.size());
      }
      if (common::TFS_SUCCESS == ret)
      {
        std::vector<std::pair<uint64_t, common::BlockInfo> >::const_iterator iter = members_.begin();
        for (; iter != members_.end() && common::TFS_SUCCESS == ret; ++iter)
        {
          ret = output.set_int64(iter->first);
          if (common::TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = iter->second.serialize(output.get_free(), output.get_free_length(), pos);
            if (common::TFS_SUCCESS == ret)
              output.pour(iter->second.length());
          }
        }
      }
      return ret;
    }

    int64_t ResolveBlockVersionConflictMessage::length() const
    {
      int64_t length = common::INT_SIZE * 2;
      if (!members_.empty())
      {
        common::BlockInfo info;
        length += (members_.size() * (info.length() + common::INT64_SIZE));
      }
      return length;
    }

    ResolveBlockVersionConflictResponseMessage::ResolveBlockVersionConflictResponseMessage():
      status_(common::TFS_ERROR)
    {
      _packetHeader._pcode = common::RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    ResolveBlockVersionConflictResponseMessage::~ResolveBlockVersionConflictResponseMessage()
    {

    }

    int ResolveBlockVersionConflictResponseMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&status_);
    }

    int ResolveBlockVersionConflictResponseMessage::serialize(common::Stream& output) const
    {
      return output.set_int32(status_);
    }

    int64_t ResolveBlockVersionConflictResponseMessage::length() const
    {
      return common::INT_SIZE;
    }
  }/** message **/
}/** tfs **/
