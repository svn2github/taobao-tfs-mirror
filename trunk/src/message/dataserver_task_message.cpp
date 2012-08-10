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
#include "dataserver_task_message.h"

namespace tfs
{
  namespace message
  {
    DsCompactBlockMessage::DsCompactBlockMessage() :
      block_id_(0), source_id_(0)
    {
      seqno_ = 0;
      _packetHeader._pcode = common::DS_COMPACT_BLOCK_MESSAGE;
    }

    DsCompactBlockMessage::~DsCompactBlockMessage()
    {

    }

    int DsCompactBlockMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(&seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&source_id_));
      }
      return ret;
    }

    int64_t DsCompactBlockMessage::length() const
    {
      return common::INT_SIZE + 2 * common::INT64_SIZE;
    }

    int DsCompactBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(block_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(source_id_);
      }
      return ret;
    }

    DsReplicateBlockMessage::DsReplicateBlockMessage() :
      block_id_(0), source_id_(0), dest_id_(0)
    {
      seqno_ = 0;
      _packetHeader._pcode = common::DS_REPLICATE_BLOCK_MESSAGE;
    }

    DsReplicateBlockMessage::~DsReplicateBlockMessage()
    {

    }

    int DsReplicateBlockMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(&seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&source_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&dest_id_));
      }
      return ret;
    }

    int64_t DsReplicateBlockMessage::length() const
    {
      return common::INT_SIZE + 3 * common::INT64_SIZE;
    }

    int DsReplicateBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(block_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(source_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(dest_id_);
      }

      return ret;
    }

    DsTaskResponseMessage::DsTaskResponseMessage() :
      status_(0)
    {
      _packetHeader._pcode = common::DS_TASK_RESPONSE_MESSAGE;
    }

    DsTaskResponseMessage::~DsTaskResponseMessage()
    {

    }

    int DsTaskResponseMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int64(&seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&ds_id_));
      }
      return ret;
    }

    int64_t DsTaskResponseMessage::length() const
    {
      return common::INT_SIZE + 2 * common::INT64_SIZE;
    }

    int DsTaskResponseMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(ds_id_);
      }

      return ret;
    }

  }
}
