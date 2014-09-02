/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: $
 *
 * Authors:
 *   qixiao.zs <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#include "common/stream.h"
#include "common/serialization.h"
#include "common/kv_rts_define.h"

#include "kv_rts_message.h"

using namespace tfs::common;


namespace tfs
{
  namespace message
  {
    KvRtsMsHeartMessage::KvRtsMsHeartMessage()
    {
      _packetHeader._pcode = common::REQ_KV_RT_MS_KEEPALIVE_MESSAGE;
    }

    KvRtsMsHeartMessage::~KvRtsMsHeartMessage()
    {

    }

    int KvRtsMsHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = base_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        input.drain(base_info_.length());
      }
      return iret;
    }

    int KvRtsMsHeartMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = base_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        output.pour(base_info_.length());
      }
      return iret;
    }

    int64_t KvRtsMsHeartMessage::length() const
    {
      return base_info_.length();
    }

    //rsp heart
    KvRtsMsHeartResponseMessage::KvRtsMsHeartResponseMessage()
    {
      _packetHeader._pcode = common::RSP_KV_RT_MS_KEEPALIVE_MESSAGE;
    }

    KvRtsMsHeartResponseMessage::~KvRtsMsHeartResponseMessage()
    {

    }

    int KvRtsMsHeartResponseMessage::deserialize(common::Stream& input)
    {
      int32_t iret = input.get_int32(&heart_interval_);
      return iret;
    }

    int KvRtsMsHeartResponseMessage::serialize(common::Stream& output) const
    {
      int32_t iret = output.set_int32(heart_interval_);
      return iret;
    }

    int64_t KvRtsMsHeartResponseMessage::length() const
    {
      return INT_SIZE;
    }
    //rsp table
    GetTableFromKvRtsResponseMessage::GetTableFromKvRtsResponseMessage()
    {
      _packetHeader._pcode = common::RSP_KV_RT_GET_TABLE_MESSAGE;
    }

    GetTableFromKvRtsResponseMessage::~GetTableFromKvRtsResponseMessage()
    {
    }

    int GetTableFromKvRtsResponseMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t iret = kv_meta_table_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        input.drain(kv_meta_table_.length());
      }
      return iret;
    }

    int GetTableFromKvRtsResponseMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t iret = kv_meta_table_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == iret)
      {
        output.pour(kv_meta_table_.length());
      }
      return iret;
    }

    int64_t GetTableFromKvRtsResponseMessage::length() const
    {
      return kv_meta_table_.length();
    }

    GetTableFromKvRtsMessage::GetTableFromKvRtsMessage():
      reserve_(0)
    {
      _packetHeader._pcode = common::REQ_KV_RT_GET_TABLE_MESSAGE;
    }

    GetTableFromKvRtsMessage::~GetTableFromKvRtsMessage()
    {

    }

    int GetTableFromKvRtsMessage::deserialize(common::Stream& input)
    {
      return input.get_int8(&reserve_);
    }

    int GetTableFromKvRtsMessage::serialize(common::Stream& output) const
    {
      return output.set_int8(reserve_);
    }

    int64_t GetTableFromKvRtsMessage::length() const
    {
      return common::INT8_SIZE;
    }
  }/** message **/
}/** tfs **/
