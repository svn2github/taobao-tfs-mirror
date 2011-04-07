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
      block_id_(0), success_(PLAN_STATUS_END), server_id_(0), flag_(0)
    {
      memset(&block_info_, 0, sizeof(block_info_));
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
      memcpy(&block_info_, data, sizeof(block_info_));
      len += sizeof(block_info_);

      /*if (get_object(&data, &len, reinterpret_cast<void**> (&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }*/
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
      if (set_object(&data, &len, &block_info_, BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* CompactBlockCompleteMessage::get_name()
    {
      return "blockwritecompletemessage";
    }

    int CompactBlockCompleteMessage::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > buf_len))
      {
        return -1;
      }
      memcpy((buf+pos), &block_id_, INT_SIZE);
      pos += INT_SIZE;
      memcpy((buf+pos), &success_, INT_SIZE);
      pos += INT_SIZE;
      memcpy((buf+pos), &server_id_, INT64_SIZE);
      pos += INT64_SIZE;
      buf[pos] = flag_;
      pos += sizeof(uint8_t);
      memcpy((buf+pos), &block_info_, sizeof(block_info_));
      pos += sizeof(block_info_);
      buf[pos] = ds_list_.size();
      pos += sizeof(uint8_t);
      for (uint32_t i = 0; i < ds_list_.size(); ++i)
      {
        memcpy((buf+pos), &ds_list_[i], INT64_SIZE);
        pos += INT64_SIZE;
      }
      return 0;
    }
    int CompactBlockCompleteMessage::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      if ((buf == NULL)
          || (pos + get_serialize_size() > data_len))
      {
        return -1;
      }
      memcpy(&block_id_, (buf+pos), INT_SIZE);
      pos += INT_SIZE;
      memcpy(&success_, (buf+pos), INT_SIZE);
      pos += INT_SIZE;
      memcpy(&server_id_,(buf+pos), INT64_SIZE);
      pos += INT64_SIZE;
      flag_ = buf[pos];
      pos += sizeof(uint8_t);
      memcpy(&block_info_, (buf+pos), sizeof(BlockInfo));
      pos += sizeof(block_info_);
      int32_t size = buf[pos];
      pos += sizeof(uint8_t);
      uint64_t ds_id = 0;
      for (int32_t i = 0; i < size; ++i)
      {
        memcpy(&ds_id, (buf+pos), INT64_SIZE);
        pos += INT64_SIZE;
        ds_list_.push_back(ds_id);
      }
      return 0;
    }
    int64_t CompactBlockCompleteMessage::get_serialize_size(void) const
    {
      return INT_SIZE + INT_SIZE + sizeof(uint8_t) + INT64_SIZE + sizeof(BlockInfo) + sizeof(uint8_t) + ds_list_.size() * INT64_SIZE;
    }
    
    void CompactBlockCompleteMessage::dump(void) const
    {
      std::string ipstr;
      size_t iSize = ds_list_.size();
      for (size_t i = 0; i < iSize; ++i)
      {
        ipstr += tbsys::CNetUtil::addrToString(ds_list_[i]);
        ipstr += "/";
      }
      TBSYS_LOG(DEBUG, "block(%u) success(%d) serverid(%lld) flag(%d) id(%u) version(%u)"
          "file_count(%u) size(%u) delfile_count(%u) del_size(%u) seqno(%u), ds_list(%u), dataserver(%s)",
          block_id_, success_, server_id_, flag_, block_info_.block_id_, block_info_.version_, block_info_.file_count_, block_info_.size_,
          block_info_.del_file_count_, block_info_.del_size_, block_info_.seq_no_, ds_list_.size(), ipstr.c_str());
    }
    

    Message* CompactBlockCompleteMessage::create(const int32_t type)
    {
      CompactBlockCompleteMessage* req_cbc_msg = new CompactBlockCompleteMessage();
      req_cbc_msg->set_message_type(type);
      return req_cbc_msg;
    }
  }
}
