/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_file_entry_message.cpp 346 2013-08-29 10:18:07Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "sync_file_entry_message.h"

namespace tfs
{
  namespace message
  {
    SyncFileEntryMessage::SyncFileEntryMessage() :
      count_(0)
    {
      _packetHeader._pcode = common::REQ_SYNC_FILE_ENTRY_MESSAGE;
    }

    SyncFileEntryMessage::~SyncFileEntryMessage()
    {

    }

    int SyncFileEntryMessage::deserialize(common::Stream& input)
    {
      int32_t ret = input.get_int32(&count_);
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        for (int32_t index = 0; index < count_ && common::TFS_SUCCESS == ret; ++index)
        {
          pos = 0;
          ret = entry_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == ret)
          {
            input.drain(entry_[index].length());
          }
        }
      }
      return ret;
    }

    int64_t SyncFileEntryMessage::length() const
    {
      return common::INT_SIZE + count_ * entry_[0].length();
    }

    int SyncFileEntryMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(count_);
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        for (int32_t index = 0; index < count_ && common::TFS_SUCCESS == ret; ++index)
        {
          pos = 0;
          ret = entry_[index].serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == ret)
          {
            output.pour(entry_[index].length());
          }
        }
      }
      return ret;
    }

    SyncFileEntryResponseMessage::SyncFileEntryResponseMessage():
      result_(common::TFS_SUCCESS)
    {
      _packetHeader._pcode = common::RSP_SYNC_FILE_ENTRY_MESSAGE;
    }

    SyncFileEntryResponseMessage::~SyncFileEntryResponseMessage()
    {

    }

    int SyncFileEntryResponseMessage::deserialize(common::Stream& input)
    {
      return input.get_int32(&result_);
    }

    int64_t SyncFileEntryResponseMessage::length() const
    {
      return common::INT_SIZE;
    }

    int SyncFileEntryResponseMessage::serialize(common::Stream& output) const
    {
      return output.set_int32(result_);
    }
  }/** end namespace message **/
}/** end namespace tfs **/
