/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: replicate_block_message.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "replicate_block_message.h"

namespace tfs
{
  namespace message
  {
    ReplicateBlockMessage::ReplicateBlockMessage() :
      status_(0)
    {
      expire_time_ = 0;
      _packetHeader._pcode = common::REPLICATE_BLOCK_MESSAGE;
      memset(&repl_block_, 0, sizeof(common::ReplBlock));
    }

    ReplicateBlockMessage::~ReplicateBlockMessage()
    {
    }

    int ReplicateBlockMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32(&status_);
      if (common::TFS_SUCCESS == ret)
      {
        ret =  input.get_int32(&expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = repl_block_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          input.drain(repl_block_.length());
        }
      }
      if (common::TFS_SUCCESS == ret
        && input.get_data_length() > 0 )
      {
        ret = input.get_int64(&seqno_);
      }
      return ret;
    }

    int ReplicateBlockMessage::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = common::Serialization::get_int32(data, data_len, pos, &status_);
      if (common::TFS_SUCCESS == ret)
      {
        ret =  common::Serialization::get_int32(data, data_len, pos, &expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = repl_block_.deserialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == ret
        && pos + common::INT64_SIZE <= data_len)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &seqno_);
      }
      return ret;
    }

    int64_t ReplicateBlockMessage::length() const
    {
      return common::INT_SIZE * 2 + common::INT64_SIZE + repl_block_.length();
    }

    int ReplicateBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(status_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = repl_block_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          output.pour(repl_block_.length());
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      return ret;
    }

    void ReplicateBlockMessage::dump(void) const
    {
      TBSYS_LOG(INFO, "seqno: %"PRI64_PREFIX"d, command: %d, expire: %d, block: %u, source: %s, target: %s, start_time: %d, is_move: %s, server_count: %d",
        seqno_, status_, expire_time_, repl_block_.block_id_,
        tbsys::CNetUtil::addrToString(repl_block_.source_id_).c_str(),
        tbsys::CNetUtil::addrToString(repl_block_.destination_id_).c_str(),
        repl_block_.start_time_,
        repl_block_.is_move_ == common::REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move", repl_block_.server_count_);
    }
  }
}
