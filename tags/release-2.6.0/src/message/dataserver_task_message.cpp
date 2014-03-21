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
        ret = input.get_int64( reinterpret_cast<int64_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&source_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&expire_time_);
      }
      return ret;
    }

    int64_t DsCompactBlockMessage::length() const
    {
      return 2 * common::INT_SIZE + 3 * common::INT64_SIZE;
    }

    int DsCompactBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(block_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(source_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      return ret;
    }

    DsReplicateBlockMessage::DsReplicateBlockMessage():
      source_id_(0)
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
        ret = input.get_int32(&expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*> (&source_id_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = repl_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          input.drain(repl_info_.length());
        }
      }
      return ret;
    }

    int64_t DsReplicateBlockMessage::length() const
    {
      return common::INT_SIZE + 2 * common::INT64_SIZE + repl_info_.length();
    }

    int DsReplicateBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int64(seqno_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(source_id_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = repl_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          output.pour(repl_info_.length());
        }
      }
      return ret;
    }

    RespDsReplicateBlockMessage::RespDsReplicateBlockMessage() :
      status_(0)
    {
      _packetHeader._pcode = common::RESP_DS_REPLICATE_BLOCK_MESSAGE;
    }

    RespDsReplicateBlockMessage::~RespDsReplicateBlockMessage()
    {

    }

    int RespDsReplicateBlockMessage::deserialize(common::Stream& input)
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

    int64_t RespDsReplicateBlockMessage::length() const
    {
      return common::INT_SIZE + 2 * common::INT64_SIZE;
    }

    int RespDsReplicateBlockMessage::serialize(common::Stream& output) const
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

    RespDsCompactBlockMessage::RespDsCompactBlockMessage() :
      status_(0)
    {
      _packetHeader._pcode = common::RESP_DS_COMPACT_BLOCK_MESSAGE;
    }

    RespDsCompactBlockMessage::~RespDsCompactBlockMessage()
    {

    }

    int RespDsCompactBlockMessage::deserialize(common::Stream& input)
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
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          input.drain(info_.length());
        }
      }

      return ret;
    }

    int64_t RespDsCompactBlockMessage::length() const
    {
      return common::INT_SIZE + 2 * common::INT64_SIZE;
    }

    int RespDsCompactBlockMessage::serialize(common::Stream& output) const
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
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          output.pour(info_.length());
        }
      }

      return ret;
    }

  }
}
