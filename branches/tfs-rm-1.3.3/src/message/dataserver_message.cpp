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
#include "dataserver_message.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    // SetDataserverMessage 
    SetDataserverMessage::SetDataserverMessage() :
      ds_(NULL), has_block_(HAS_BLOCK_FLAG_NO)
    {
      _packetHeader._pcode = SET_DATASERVER_MESSAGE;
      blocks_.clear();
    }

    SetDataserverMessage::~SetDataserverMessage()
    {
    }

    // DataServerStatInfo, block_count, block_id, block_version, ...
    int SetDataserverMessage::parse(char* data, int32_t len)
    {
      if (get_object(&data, &len, reinterpret_cast<void**> (&ds_), sizeof(DataServerStatInfo))
          == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&has_block_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (has_block_ > 0)
      {
        int32_t size = 0;
        if (get_int32(&data, &len, &size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        BlockInfo* block_info;
        int32_t i = 0;
        for (i = 0; i < size; i++)
        {
          if (get_object(&data, &len, reinterpret_cast<void**> (&block_info), BLOCKINFO_SIZE)
              == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          blocks_.push_back(*block_info);
        }
      }
      return TFS_SUCCESS;
    }

    int32_t SetDataserverMessage::message_length()
    {
      int32_t len = sizeof(DataServerStatInfo) + INT_SIZE;
      if (has_block_ > 0)
      {
        len += INT_SIZE;
        len += blocks_.size() * BLOCKINFO_SIZE;
      }
      return len;
    }

    int SetDataserverMessage::build(char* data, int32_t len)
    {
      if (ds_ == NULL)
      {
        return TFS_ERROR;
      }

      if (set_object(&data, &len, reinterpret_cast<void**> (ds_), sizeof(DataServerStatInfo))
          == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, has_block_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (has_block_)
      {
        // size
        int32_t size = blocks_.size();
        if (set_int32(&data, &len, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        for (int32_t i = 0; i < size; i++)
        {
          if (set_object(&data, &len, &(blocks_.at(i)), BLOCKINFO_SIZE) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
        }
      }
      return TFS_SUCCESS;
    }

    char* SetDataserverMessage::get_name()
    {
      return "setdataservermessage";
    }

    Message* SetDataserverMessage::create(const int32_t type)
    {
      SetDataserverMessage* req_sd_msg = new SetDataserverMessage();
      req_sd_msg->set_message_type(type);
      return req_sd_msg;
    }

    void SetDataserverMessage::set_ds(DataServerStatInfo* ds)
    {
      ds_ = ds;
    }

    void SetDataserverMessage::add_block(BlockInfo* block_info)
    {
      blocks_.push_back(*block_info);
    }

    // SuspectDataserverMessage  
    SuspectDataserverMessage::SuspectDataserverMessage()
    {
      _packetHeader._pcode = SUSPECT_DATASERVER_MESSAGE;
      server_id_ = 0;
    }

    SuspectDataserverMessage::~SuspectDataserverMessage()
    {
    }

    int SuspectDataserverMessage::parse(char* data, int32_t len)
    {
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&server_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t SuspectDataserverMessage::message_length()
    {
      return INT64_SIZE;
    }

    int SuspectDataserverMessage::build(char* data, int32_t len)
    {
      if (set_int64(&data, &len, server_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* SuspectDataserverMessage::get_name()
    {
      return "suspectdataservermessage";
    }

    Message* SuspectDataserverMessage::create(const int32_t type)
    {
      SuspectDataserverMessage* req_sd_msg = new SuspectDataserverMessage();
      req_sd_msg->set_message_type(type);
      return req_sd_msg;
    }
  }
}
