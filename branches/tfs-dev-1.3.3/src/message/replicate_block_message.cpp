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

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ReplicateBlockMessage::ReplicateBlockMessage() :
      command_(0), expire_(0)
    {
      _packetHeader._pcode = REPLICATE_BLOCK_MESSAGE;
      memset(&repl_block_, 0, sizeof(ReplBlock));
    }

    ReplicateBlockMessage::~ReplicateBlockMessage()
    {
    }

    int ReplicateBlockMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &command_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &expire_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_object_copy(&data, &len, reinterpret_cast<void*> (&repl_block_), sizeof(ReplBlock)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ReplicateBlockMessage::message_length()
    {
      int32_t len = INT_SIZE * 2 + sizeof(ReplBlock);
      return len;
    }

    int ReplicateBlockMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, command_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, expire_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_object(&data, &len, &repl_block_, sizeof(ReplBlock)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int ReplicateBlockMessage::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > buf_len))
      {
        return -1;
      }
      memcpy((buf+pos), &command_, INT_SIZE);
      pos += INT_SIZE;
      memcpy((buf+pos), &expire_, INT_SIZE);
      pos += INT_SIZE;
      memcpy((buf+pos), &repl_block_, sizeof(ReplBlock));
      pos += sizeof(ReplBlock);
      return 0;
    }
    
    int ReplicateBlockMessage::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > data_len))
      {
        return -1;
      }
      memcpy(&command_, (buf+pos), INT_SIZE);
      pos += INT_SIZE;
      memcpy(&expire_, (buf+pos), INT_SIZE);
      pos += INT_SIZE;
      memcpy(&repl_block_, (buf+pos), sizeof(ReplBlock));
      pos += sizeof(ReplBlock);
      return 0; 
    }
    
    int64_t ReplicateBlockMessage::get_serialize_size(void) const
    {
      return sizeof(int32_t) + sizeof(int32_t) + sizeof(ReplBlock);
    }
    
    void ReplicateBlockMessage::dump(void) const
    {
      TBSYS_LOG(DEBUG, "command(%d) expire(%u) id(%u) source_id(%llu) dest_id(%llu) start_time(%d) is_move(%d) server_count(%d)",
          command_, expire_, repl_block_.block_id_, repl_block_.source_id_, repl_block_.destination_id_,
          repl_block_.start_time_, repl_block_.is_move_, repl_block_.server_count_);
    }

    char* ReplicateBlockMessage::get_name()
    {
      return "replicateblockmessage";
    }

    Message* ReplicateBlockMessage::create(const int32_t type)
    {
      ReplicateBlockMessage* req_rb_msg = new ReplicateBlockMessage();
      req_rb_msg->set_message_type(type);
      return req_rb_msg;
    }
  }
}
