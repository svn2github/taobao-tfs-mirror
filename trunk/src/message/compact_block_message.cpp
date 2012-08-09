/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: compact_block_message.cpp 983 2011-10-31 09:59:33Z duanfei $
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
    NsRequestCompactBlockMessage::NsRequestCompactBlockMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = common::COMPACT_BLOCK_MESSAGE;
      servers_.clear();
    }

    NsRequestCompactBlockMessage::~NsRequestCompactBlockMessage()
    {

    }

    int NsRequestCompactBlockMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32( reinterpret_cast<int32_t*> (&block_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(servers_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&seqno_);
      }
      return ret;
    }

    int64_t NsRequestCompactBlockMessage::length() const
    {
      return common::INT_SIZE + common::Serialization::get_vint64_length(servers_);
    }

    int NsRequestCompactBlockMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(block_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(servers_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      return ret;
    }

    DsCommitCompactBlockCompleteToNsMessage::DsCommitCompactBlockCompleteToNsMessage() :
      status_(common::PLAN_STATUS_FAILURE)
    {
      memset(&block_info_, 0, sizeof(block_info_));
      _packetHeader._pcode = common::BLOCK_COMPACT_COMPLETE_MESSAGE;
    }

    DsCommitCompactBlockCompleteToNsMessage::~DsCommitCompactBlockCompleteToNsMessage()
    {

    }

    int DsCommitCompactBlockCompleteToNsMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(block_info_.length());
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int64(&seqno_);
      }
      return ret;
    }

    int DsCommitCompactBlockCompleteToNsMessage::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = block_info_.deserialize(data, data_len, pos);
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int8(data, data_len, pos, &status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &seqno_);
      }
      return ret;
    }

    int64_t DsCommitCompactBlockCompleteToNsMessage::length() const
    {
      return  common::INT8_SIZE + block_info_.length();
    }

    int DsCommitCompactBlockCompleteToNsMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t ret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        output.pour(block_info_.length());
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret =  output.set_int8(status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(seqno_);
      }
      return ret;
    }
    void DsCommitCompactBlockCompleteToNsMessage::dump(void) const
    {

    }
  }
}
