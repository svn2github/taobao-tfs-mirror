/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
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
      command_(0), expire_(0)
    {
      _packetHeader._pcode = common::REPLICATE_BLOCK_MESSAGE;
      memset(&repl_block_, 0, sizeof(common::ReplBlock));
    }

    ReplicateBlockMessage::~ReplicateBlockMessage()
    {
    }

    int ReplicateBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&command_);
      if (common::TFS_SUCCESS == iret)
      {
        iret =  input.get_int32(&expire_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = repl_block_.deserialize(input.get_data(), input.get_data_length(), pos); 
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(repl_block_.length());
        }
      }
      return iret;
    }

    int ReplicateBlockMessage::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = common::Serialization::get_int32(data, data_len, pos, &command_);
      if (common::TFS_SUCCESS == iret)
      {
        iret =  common::Serialization::get_int32(data, data_len, pos, &expire_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = repl_block_.deserialize(data, data_len, pos); 
      }
      return iret;
    }

    int64_t ReplicateBlockMessage::length() const
    {
      return common::INT_SIZE * 2 + repl_block_.length();
    }

    int ReplicateBlockMessage::serialize(common::Stream& output) const 
    {
      int32_t iret = output.set_int32(command_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(expire_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = repl_block_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(repl_block_.length());
        }
      }
      return iret;
    }

    void ReplicateBlockMessage::dump(void) const
    {
      TBSYS_LOG(DEBUG, "command(%d) expire(%u) id(%u) source_id(%llu) dest_id(%llu) start_time(%d) is_move(%d) server_count(%d)",
          command_, expire_, repl_block_.block_id_, repl_block_.source_id_, repl_block_.destination_id_,
          repl_block_.start_time_, repl_block_.is_move_, repl_block_.server_count_);
    }
  }
}
