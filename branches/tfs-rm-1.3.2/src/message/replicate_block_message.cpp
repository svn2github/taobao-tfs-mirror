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
