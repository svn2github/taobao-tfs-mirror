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

#include "get_dataserver_stat_info_message.h"

namespace tfs
{
  namespace message
  {
    GetDsStatInfoMessage::GetDsStatInfoMessage()
    {
      memset(&information_, 0, sizeof(information_));
      _packetHeader._pcode = common::DS_STAT_INFO_MESSAGE;
    }

    GetDsStatInfoMessage::~GetDsStatInfoMessage()
    {

    }

    int GetDsStatInfoMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = information_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(information_.length());
      }
      return ret;
    }

    int64_t GetDsStatInfoMessage::length() const
    {
      return information_.length();
    }

    int GetDsStatInfoMessage::serialize(common::Stream& output) const
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

  }/** end namespace message **/
}/** end namespace tfs **/
