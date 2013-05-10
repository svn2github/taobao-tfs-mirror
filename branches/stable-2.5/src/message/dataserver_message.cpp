/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataserver_message.cpp 706 2011-08-12 08:24:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "dataserver_message.h"
namespace tfs
{
  namespace message
  {
    SetDataserverMessage::SetDataserverMessage()
    {
      _packetHeader._pcode = common::SET_DATASERVER_MESSAGE;
      memset(&information_, 0, sizeof(information_));
    }

    SetDataserverMessage::~SetDataserverMessage()
    {

    }

    int SetDataserverMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = information_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(information_.length());
      }
      return ret;
    }

    int64_t SetDataserverMessage::length() const
    {
      return information_.length();
    }

    int SetDataserverMessage::serialize(common::Stream& output) const
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

    CallDsReportBlockRequestMessage::CallDsReportBlockRequestMessage():
      server_(0), flag_(0)
    {
      _packetHeader._pcode = common::REQ_CALL_DS_REPORT_BLOCK_MESSAGE;
    }

    CallDsReportBlockRequestMessage::~CallDsReportBlockRequestMessage()
    {

    }

    int CallDsReportBlockRequestMessage::deserialize(common::Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t*>(&server_));
      if (common::TFS_SUCCESS == ret)
      {
        // don't care return value for compaitable
        input.get_int32(&flag_);
      }
      return ret;
    }

    int64_t CallDsReportBlockRequestMessage::length() const
    {
      return common::INT64_SIZE + common::INT_SIZE;
    }

    int CallDsReportBlockRequestMessage::serialize(common::Stream& output) const
    {
      int ret = output.set_int64(server_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }
      return ret;
    }

    ReportBlocksToNsRequestMessage::ReportBlocksToNsRequestMessage():
      blocks_ext_(NULL),
      server_(common::INVALID_SERVER_ID),
      block_count_(0)
    {
      _packetHeader._pcode = common::REQ_REPORT_BLOCKS_TO_NS_MESSAGE;
    }

    ReportBlocksToNsRequestMessage::~ReportBlocksToNsRequestMessage()
    {
      tbsys::gDeleteA(blocks_ext_);
    }

    int ReportBlocksToNsRequestMessage::deserialize(common::Stream& input)
    {
      int32_t ret =input.get_int64(reinterpret_cast<int64_t*>(&server_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        int64_t pos = 0;
        blocks_ext_ = new (std::nothrow)common::BlockInfoV2[block_count_];
        for (int32_t index = 0; index < block_count_ && common::TFS_SUCCESS == ret; ++index)
        {
          pos = 0;
          blocks_ext_[index].deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == ret)
            input.drain(blocks_ext_[index].length());
        }
      }
      return ret;
    }

    int64_t ReportBlocksToNsRequestMessage::length() const
    {
      common::BlockInfoV2 info;
      return common::INT64_SIZE + common::INT_SIZE + block_count_ * info.length();
    }

    int ReportBlocksToNsRequestMessage::serialize(common::Stream& output) const
    {
      int32_t ret = server_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < block_count_ && common::TFS_SUCCESS == ret; ++index)
        {
          int64_t pos = 0;
          ret = blocks_ext_[index].serialize(output.get_free(),output.get_free_length(), pos);
          if (common::TFS_SUCCESS == ret)
            output.pour(blocks_ext_[index].length());
        }
      }
      return ret;
    }

    ReportBlocksToNsResponseMessage::ReportBlocksToNsResponseMessage():
    server_(0),
    status_(0)
    {
      _packetHeader._pcode = common::RSP_REPORT_BLOCKS_TO_NS_MESSAGE;
    }

    ReportBlocksToNsResponseMessage::~ReportBlocksToNsResponseMessage()
    {

    }

    int ReportBlocksToNsResponseMessage::deserialize(common::Stream& input)
    {
      int32_t ret =input.get_int64(reinterpret_cast<int64_t*>(&server_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&status_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_vint64(expire_blocks_);
      }
      return ret;
    }

    int64_t ReportBlocksToNsResponseMessage::length() const
    {
      return common::INT64_SIZE + common::INT8_SIZE + common::Serialization::get_vint64_length(expire_blocks_);
    }

    int ReportBlocksToNsResponseMessage::serialize(common::Stream& output) const
    {
      int32_t ret = server_ == 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_);
        if (common::TFS_SUCCESS == ret)
        {
          ret = output.set_int8(status_);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_vint64(expire_blocks_);
      }
      return ret;
    }
  }/** end namespace message **/
}/** end namespace tfs **/
