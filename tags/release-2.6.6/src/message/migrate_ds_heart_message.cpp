/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_ds_heart_message.cpp 384 2013-09-08 09:47:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include "common/stream.h"
#include "common/serialization.h"

#include "migrate_ds_heart_message.h"

namespace tfs
{
  namespace message
  {
    MigrateDsHeartMessage::MigrateDsHeartMessage()
    {
      memset(&information_, 0, sizeof(information_));
      _packetHeader._pcode = common::REQ_MIGRATE_DS_HEARTBEAT_MESSAGE;
    }

    MigrateDsHeartMessage::~MigrateDsHeartMessage()
    {

    }

    int MigrateDsHeartMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = information_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(information_.length());
      }
      return ret;
    }

    int64_t MigrateDsHeartMessage::length() const
    {
      return information_.length();
    }

    int MigrateDsHeartMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t ret = information_.id_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = information_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          output.pour(information_.length());
        }
      }
      return ret;
    }

    MigrateDsHeartResponseMessage::MigrateDsHeartResponseMessage():
      ret_value_(common::TFS_ERROR)
    {
      _packetHeader._pcode = common::RSP_MIGRATE_DS_HEARTBEAT_MESSAGE;
    }

    MigrateDsHeartResponseMessage::~MigrateDsHeartResponseMessage()
    {

    }

    int MigrateDsHeartResponseMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&ret_value_);
    }

    int MigrateDsHeartResponseMessage::serialize(common::Stream& output) const
    {
      return output.set_int32(ret_value_);
    }

    int64_t MigrateDsHeartResponseMessage::length() const
    {
      return common::INT_SIZE;
    }
  }/** end namespace message **/
}/** end namespace tfs **/
