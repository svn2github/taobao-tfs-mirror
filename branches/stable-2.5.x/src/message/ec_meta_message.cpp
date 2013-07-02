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

#include <ec_meta_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    QueryEcMetaMessage::QueryEcMetaMessage()
    {
      _packetHeader._pcode = QUERY_EC_META_MESSAGE;
    }

    QueryEcMetaMessage::~QueryEcMetaMessage()
    {
    }

    int QueryEcMetaMessage::serialize(Stream& output) const
    {
      return output.set_int64(block_id_);
    }

    int QueryEcMetaMessage::deserialize(Stream& input)
    {
      return input.get_int64(reinterpret_cast<int64_t* >(&block_id_));
    }

    int64_t QueryEcMetaMessage::length() const
    {
      return INT64_SIZE;
    }

    QueryEcMetaRespMessage::QueryEcMetaRespMessage()
    {
      _packetHeader._pcode = QUERY_EC_META_RESP_MESSAGE;
    }

    QueryEcMetaRespMessage::~QueryEcMetaRespMessage()
    {
    }

    int QueryEcMetaRespMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int ret = ec_meta_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(ec_meta_.length());
      }
      return ret;
    }

    int QueryEcMetaRespMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = ec_meta_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(ec_meta_.length());
      }
      return ret;
    }

    int64_t QueryEcMetaRespMessage::length() const
    {
      return  ec_meta_.length();
    }

    CommitEcMetaMessage::CommitEcMetaMessage()
    {
      _packetHeader._pcode = COMMIT_EC_META_MESSAGE;
    }

    CommitEcMetaMessage::~CommitEcMetaMessage()
    {
    }

    int CommitEcMetaMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(switch_flag_);
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        int ret = ec_meta_.serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(ec_meta_.length());
        }
      }
      return ret;
    }

    int CommitEcMetaMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&switch_flag_);
      }
      if (TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        int ret = ec_meta_.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(ec_meta_.length());
        }
      }
      return ret;
    }

    int64_t CommitEcMetaMessage::length() const
    {
      return INT64_SIZE + INT8_SIZE + ec_meta_.length();
    }

  }
}
