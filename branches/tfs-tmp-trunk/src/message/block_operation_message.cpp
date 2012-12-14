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

#include <block_operation_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    CreateBlockMessage::CreateBlockMessage()
    {
      _packetHeader._pcode = CREATE_BLOCK_MESSAGE;
    }

    CreateBlockMessage::~CreateBlockMessage()
    {
    }

    int CreateBlockMessage::serialize(Stream& output) const
    {
      return output.set_vint64(block_ids_);
    }

    int CreateBlockMessage::deserialize(Stream& input)
    {
      return input.get_vint64(block_ids_);
    }

    int64_t CreateBlockMessage::length() const
    {
      return Serialization::get_vint64_length(block_ids_);
    }

    DeleteBlockMessage::DeleteBlockMessage():
      block_id_(INVALID_BLOCK_ID)
    {
      _packetHeader._pcode = DELETE_BLOCK_MESSAGE;
    }

    DeleteBlockMessage::~DeleteBlockMessage()
    {
    }

    int DeleteBlockMessage::serialize(Stream& output) const
    {
      return output.set_int64(block_id_);
    }

    int DeleteBlockMessage::deserialize(Stream& input)
    {
      return input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
    }

    int64_t DeleteBlockMessage::length() const
    {
      return INT64_SIZE;
    }

  }
}
