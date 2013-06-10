/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_message.cpp 706 2012-04-12 14:24:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include "checkserver_message.h"
namespace tfs
{
  namespace message
  {
    using namespace common;

    int CheckBlockRequestMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int ret = range_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(range_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(group_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(group_seq_);
      }
      return ret;
    }

    int CheckBlockRequestMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int ret = range_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(range_.length());
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&group_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&group_seq_);
      }
      return ret;
    }

    int64_t CheckBlockRequestMessage::length() const
    {
      return range_.length() + 2 * INT_SIZE;
    }

    int CheckBlockResponseMessage::serialize(common::Stream& output) const
    {
      return output.set_vint64(blocks_);
    }

    int CheckBlockResponseMessage::deserialize(common::Stream& input)
    {
      return input.get_vint64(blocks_);
    }

    int64_t CheckBlockResponseMessage::length() const
    {
      return Serialization::get_vint64_length(blocks_);
    }

    int ReportCheckBlockMessage::serialize(common::Stream& output) const
    {
      int ret = output.set_int64(seqno_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(blocks_);
      }
      return ret;

    }

    int ReportCheckBlockMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(&seqno_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t*>(&server_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(blocks_);
      }
      return ret;
    }

    int64_t ReportCheckBlockMessage::length() const
    {
      return 2 * INT64_SIZE + Serialization::get_vint64_length(blocks_);
    }

 }/** end namespace message **/
}/** end namespace tfs **/
