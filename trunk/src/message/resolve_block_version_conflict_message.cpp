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
    using namespace common;
    NsReqResolveBlockVersionConflictMessage::NsReqResolveBlockVersionConflictMessage():
      block_(INVALID_BLOCK_ID),
      size_(0)
    {
      seqno_ = 0;
      expire_time_ = 0;
      _packetHeader._pcode = NS_REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    NsReqResolveBlockVersionConflictMessage::~NsReqResolveBlockVersionConflictMessage()
    {

    }

    int NsReqResolveBlockVersionConflictMessage::deserialize(Stream& input)
    {
      int32_t ret = input.get_int64(reinterpret_cast<int64_t*>(&seqno_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&expire_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&block_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
        if (TFS_SUCCESS == ret)
        {
          ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; (index < size_) && (TFS_SUCCESS == ret); index++)
        {
          ret = input.get_int64(reinterpret_cast<int64_t*>(&members_[index]));
        }
      }

      return ret;
    }

    int NsReqResolveBlockVersionConflictMessage::serialize(Stream& output) const
    {
      int32_t ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(block_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; (index < size_) && (TFS_SUCCESS == ret); index++)
        {
          ret = output.set_int64(members_[index]);
        }
      }
      return ret;
    }

    int64_t NsReqResolveBlockVersionConflictMessage::length() const
    {
      return (2 + size_) * INT64_SIZE + 2 * INT_SIZE;
    }

    NsReqResolveBlockVersionConflictResponseMessage::NsReqResolveBlockVersionConflictResponseMessage():
      status_(TFS_ERROR)
    {
      _packetHeader._pcode = NS_RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    NsReqResolveBlockVersionConflictResponseMessage::~NsReqResolveBlockVersionConflictResponseMessage()
    {

    }

    int NsReqResolveBlockVersionConflictResponseMessage::deserialize(Stream& input)
    {
      return input.get_int32(&status_);
    }

    int NsReqResolveBlockVersionConflictResponseMessage::serialize(Stream& output) const
    {
      return output.set_int32(status_);
    }

    int64_t NsReqResolveBlockVersionConflictResponseMessage::length() const
    {
      return INT_SIZE;
    }

    ResolveBlockVersionConflictMessage::ResolveBlockVersionConflictMessage():
      block_(INVALID_BLOCK_ID),
      size_(0)
    {
      seqno_ = 0;
      expire_time_ = 0;
      _packetHeader._pcode = REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    ResolveBlockVersionConflictMessage::~ResolveBlockVersionConflictMessage()
    {

    }

    int ResolveBlockVersionConflictMessage::deserialize(Stream& input)
    {
      int32_t ret = input.get_int64(reinterpret_cast<int64_t*>(&seqno_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&expire_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&block_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size_);
        if (TFS_SUCCESS == ret)
        {
          ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; (i < size_) && (TFS_SUCCESS == ret); i++)
        {
          ret = input.get_int64(reinterpret_cast<int64_t*>(&members_[i].first));
          if (TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = members_[i].second.deserialize(input.get_data(), input.get_data_length(), pos);
            if (TFS_SUCCESS == ret)
            {
              input.drain(members_[i].second.length());
            }
          }
        }
      }

      return ret;
    }

    int ResolveBlockVersionConflictMessage::serialize(Stream& output) const
    {
      int32_t ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(block_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; (i < size_) && (TFS_SUCCESS == ret); i++)
        {
          ret = output.set_int64(members_[i].first);
          if (TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = members_[i].second.serialize(output.get_free(), output.get_free_length(), pos);
            if (TFS_SUCCESS == ret)
            {
              output.pour(members_[i].second.length());
            }
          }
        }
      }
      return ret;
    }

    int64_t ResolveBlockVersionConflictMessage::length() const
    {
      int64_t length = (INT_SIZE + INT64_SIZE) * 2;
      BlockInfoV2 info;
      length += size_ * (info.length() + INT64_SIZE);
      return length;
    }

    ResolveBlockVersionConflictResponseMessage::ResolveBlockVersionConflictResponseMessage():
      status_(TFS_ERROR)
    {
      _packetHeader._pcode = RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE;
    }

    ResolveBlockVersionConflictResponseMessage::~ResolveBlockVersionConflictResponseMessage()
    {

    }

    int ResolveBlockVersionConflictResponseMessage::deserialize(Stream& input)
    {
      return input.get_int32(&status_);
    }

    int ResolveBlockVersionConflictResponseMessage::serialize(Stream& output) const
    {
      return output.set_int32(status_);
    }

    int64_t ResolveBlockVersionConflictResponseMessage::length() const
    {
      return INT_SIZE;
    }
  }/** message **/
}/** tfs **/
