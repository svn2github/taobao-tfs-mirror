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
#include "compact_block_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    CompactBlockMessage::CompactBlockMessage() :
      preserve_time_(0), block_id_(0)
    {
      _packetHeader._pcode = COMPACT_BLOCK_MESSAGE;
    }

    CompactBlockMessage::~CompactBlockMessage()
    {
    }

    int CompactBlockMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &preserve_time_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, static_cast<int32_t*> (&is_owner_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t CompactBlockMessage::message_length()
    {
      int32_t len = INT_SIZE * 3;
      return len;
    }

    int CompactBlockMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, preserve_time_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, is_owner_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* CompactBlockMessage::get_name()
    {
      return "compactblockmessage";
    }

    Message* CompactBlockMessage::create(const int32_t type)
    {
      CompactBlockMessage* req_cb_msg = new CompactBlockMessage();
      req_cb_msg->set_message_type(type);
      return req_cb_msg;
    }

    // CompactBlockCompleteMessage 
    CompactBlockCompleteMessage::CompactBlockCompleteMessage() :
      block_id_(0), success_(COMPACT_COMPLETE_STATUS_SUCCESS), server_id_(0), block_info_(NULL), flag_(0)
    {
      _packetHeader._pcode = BLOCK_COMPACT_COMPLETE_MESSAGE;
    }

    CompactBlockCompleteMessage::~CompactBlockCompleteMessage()
    {
    }

    int CompactBlockCompleteMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&success_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&server_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if ((len < static_cast<int32_t> (sizeof(uint8_t))) || (len > TFS_MALLOC_MAX_SIZE))
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&flag_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, ds_list_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_object(&data, &len, reinterpret_cast<void**> (&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t CompactBlockCompleteMessage::message_length()
    {
      int32_t len = INT_SIZE * 4 + INT64_SIZE + BLOCKINFO_SIZE;
      if (ds_list_.size() > 0)
      {
        len += ds_list_.size() * INT64_SIZE;
      }
      return len;
    }

    int CompactBlockCompleteMessage::build(char* data, int32_t len)
    {
      if (block_info_ == NULL)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, success_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int64(&data, &len, server_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, ds_list_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_object(&data, &len, block_info_, BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* CompactBlockCompleteMessage::get_name()
    {
      return "blockwritecompletemessage";
    }

    Message* CompactBlockCompleteMessage::create(const int32_t type)
    {
      CompactBlockCompleteMessage* req_cbc_msg = new CompactBlockCompleteMessage();
      req_cbc_msg->set_message_type(type);
      return req_cbc_msg;
    }
  }
}
