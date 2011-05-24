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

namespace tfs
{
  namespace message
  {
    CompactBlockMessage::CompactBlockMessage() :
      preserve_time_(0), block_id_(0)
    {
      _packetHeader._pcode = common::COMPACT_BLOCK_MESSAGE;
    }

    CompactBlockMessage::~CompactBlockMessage()
    {

    }

    int CompactBlockMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&preserve_time_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&is_owner_);
      }
      return iret;
    }

    int64_t CompactBlockMessage::length() const
    {
      return common::INT_SIZE * 3;
    }

    int CompactBlockMessage::serialize(common::Stream& output)
    {
      int32_t iret = output.set_int32(preserve_time_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(block_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(is_owner_);
      }
      return iret;
    }
    common::BasePacket* CompactBlockMessage::create(const int32_t type)
    {
      return new CompactBlockMessage();
    }

    CompactBlockCompleteMessage::CompactBlockCompleteMessage() :
      block_id_(0), success_(common::PLAN_STATUS_END), server_id_(0), flag_(0)
    {
      memset(&block_info_, 0, sizeof(block_info_));
      _packetHeader._pcode = common::BLOCK_COMPACT_COMPLETE_MESSAGE;
    }

    CompactBlockCompleteMessage::~CompactBlockCompleteMessage()
    {
    }

    int CompactBlockCompleteMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(&success_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_int32(reinterpret_cast<int32_t*>(&flag_));
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = input.get_vint64(ds_list_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          input.drain(block_info_.length());
        }
      }
      return iret;
    }

    int64_t CompactBlockCompleteMessage::length() const
    {
      return  common::INT_SIZE * 3 + common::INT64_SIZE + block_info_.length() + common::Serialization::get_vint64_length(ds_list_);
    }

    int CompactBlockCompleteMessage::serialize(common::Stream& output)
    {
      int32_t iret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(success_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int64(server_id_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_int32(flag_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        iret = output.set_vint64(ds_list_);
      }
      if (common::TFS_SUCCESS == iret)
      {
        int64_t pos = 0;
        iret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(block_info_.length());
        }
      }
      return iret;
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
    
    common::BasePacket* CompactBlockCompleteMessage::create(const int32_t type)
    {
      return new CompactBlockCompleteMessage();
    }
  }
}
