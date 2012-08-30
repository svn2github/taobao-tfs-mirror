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
    SetDataserverMessage::SetDataserverMessage() :
      has_block_(common::HAS_BLOCK_FLAG_NO)
    {
      _packetHeader._pcode = common::SET_DATASERVER_MESSAGE;
      memset(&ds_, 0, sizeof(ds_));
      blocks_.clear();
    }

    SetDataserverMessage::~SetDataserverMessage()
    {

    }

    int SetDataserverMessage::deserialize(common::Stream& input)
    {
      int64_t pos = 0;
      int32_t ret = ds_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == ret)
      {
        input.drain(ds_.length());
        ret = input.get_int32(reinterpret_cast<int32_t*> (&has_block_));
        if (common::TFS_SUCCESS == ret)
        {
          if (has_block_  == common::HAS_BLOCK_FLAG_YES)
          {
            int32_t size = 0;
            ret = input.get_int32(&size);
            if (common::TFS_SUCCESS == ret)
            {
              common::BlockInfo info;
              for (int32_t i = 0; i < size; ++i)
              {
                pos  = 0;
                ret = info.deserialize(input.get_data(), input.get_data_length(), pos);
                if (common::TFS_SUCCESS == ret)
                {
                  input.drain(info.length());
                  blocks_.push_back(info);
                }
                else
                {
                  break;
                }
              }
            }
          }
        }
      }
      return ret;
    }

    int64_t SetDataserverMessage::length() const
    {
      int64_t len = ds_.length() + common::INT_SIZE;
      if (has_block_ > 0)
      {
        len += common::INT_SIZE;
        common::BlockInfo info;
        len += blocks_.size() * info.length();
      }
      return len;
    }

    int SetDataserverMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t ret = ds_.id_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = ds_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == ret)
        {
          output.pour(ds_.length());
          ret = output.set_int32(has_block_);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        if (has_block_ == common::HAS_BLOCK_FLAG_YES)
        {
          ret = output.set_int32(blocks_.size());
          if (common::TFS_SUCCESS == ret)
          {
            std::vector<common::BlockInfo>::const_iterator iter = blocks_.begin();
            for (; iter != blocks_.end(); ++iter)
            {
              pos = 0;
              ret = const_cast<common::BlockInfo*>((&(*iter)))->serialize(output.get_free(), output.get_free_length(), pos);
              if (common::TFS_SUCCESS == ret)
                output.pour((*iter).length());
              else
                break;
            }
          }
        }
      }
      return ret;
    }

    void SetDataserverMessage::set_ds(common::DataServerStatInfo* ds)
    {
      if (NULL != ds)
        ds_ = *ds;
    }

    void SetDataserverMessage::add_block(common::BlockInfo* block_info)
    {
      if (NULL != block_info)
        blocks_.push_back(*block_info);
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
      server_(common::INVALID_SERVER_ID),
      flag_(common::REPORT_BLOCK_NORMAL),
      //flag_(common::REPORT_BLOCK_EXT),
      type_(common::REPORT_BLOCK_TYPE_ALL)
    {
      _packetHeader._pcode = common::REQ_REPORT_BLOCKS_TO_NS_MESSAGE;
      blocks_.clear();
      blocks_ext_.clear();
    }

    ReportBlocksToNsRequestMessage::~ReportBlocksToNsRequestMessage()
    {

    }

    int ReportBlocksToNsRequestMessage::deserialize(common::Stream& input)
    {
      /*
       * new ns can't compatible old ds
       * ns update after all ds finish update
       * after update, new ns just receive extend block report
       */
      int32_t ret =input.get_int64(reinterpret_cast<int64_t*>(&server_));
      if (common::TFS_SUCCESS == ret)
      {
        int32_t size = 0;
        int64_t pos = 0;
        ret = input.get_int32(&size);
        if (common::TFS_SUCCESS == ret)
        {
          if (common::REPORT_BLOCK_NORMAL == flag_)
          {
            common::BlockInfo info;
            for (int32_t i = 0; i < size; ++i)
            {
              pos  = 0;
              ret = info.deserialize(input.get_data(), input.get_data_length(), pos);
              if (common::TFS_SUCCESS == ret)
              {
                input.drain(info.length());
                blocks_.insert(info);
              }
              else
              {
                break;
              }
            }
          }
          else if (common::REPORT_BLOCK_EXT == flag_)
          {
            common::BlockInfoExt info;
            for (int32_t i = 0; i < size; ++i)
            {
              pos  = 0;
              ret = info.deserialize(input.get_data(), input.get_data_length(), pos);
              if (common::TFS_SUCCESS == ret)
              {
                input.drain(info.length());
                blocks_ext_.insert(info);
              }
              else
              {
                break;
              }
            }
          }
          else
          {
            ret = common::TFS_ERROR;
          }
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&type_);
      }
      return ret;
    }

    int64_t ReportBlocksToNsRequestMessage::length() const
    {
      /*
       * new ns can't compatible old ds
       * ns update after all ds finish update
       * after update, new ns just receive extend block report
       */
      if (common::REPORT_BLOCK_NORMAL == flag_)
      {
        common::BlockInfo info;
        return common::INT64_SIZE + common::INT_SIZE + blocks_.size() * info.length() + common::INT8_SIZE;
      }
      else if (common::REPORT_BLOCK_EXT == flag_)
      {
        common::BlockInfoExt info;
        return common::INT64_SIZE + common::INT_SIZE + blocks_ext_.size() * info.length() + common::INT8_SIZE;
      }

      return common::TFS_ERROR;
    }

    int ReportBlocksToNsRequestMessage::serialize(common::Stream& output) const
    {
      int64_t pos = 0;
      int32_t ret = server_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_);
      }

      if (common::TFS_SUCCESS == ret)
      {
        if (common::REPORT_BLOCK_NORMAL == flag_)
        {
          ret = output.set_int32(blocks_.size());
          if (common::TFS_SUCCESS == ret)
          {
            std::set<common::BlockInfo>::const_iterator iter = blocks_.begin();
            for (; iter != blocks_.end(); ++iter)
            {
              pos = 0;
              ret = const_cast<common::BlockInfo*>((&(*iter)))->serialize(output.get_free(),
                  output.get_free_length(), pos);
              if (common::TFS_SUCCESS == ret)
                output.pour((*iter).length());
              else
                break;
            }
          }
        }
        else if (common::REPORT_BLOCK_EXT == flag_)
        {
          ret = output.set_int32(blocks_ext_.size());
          if (common::TFS_SUCCESS == ret)
          {
            std::set<common::BlockInfoExt>::const_iterator iter = blocks_ext_.begin();
            for (; iter != blocks_ext_.end(); ++iter)
            {
              pos = 0;
              ret = const_cast<common::BlockInfoExt*>((&(*iter)))->serialize(output.get_free(),
                  output.get_free_length(), pos);
              if (common::TFS_SUCCESS == ret)
                output.pour((*iter).length());
              else
                break;
            }
          }
        }
        else
        {
          ret = common::TFS_ERROR;
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int8(type_);
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
        ret = input.get_vint32(expire_blocks_);
      }
      return ret;
    }

    int64_t ReportBlocksToNsResponseMessage::length() const
    {
      return common::INT64_SIZE + common::INT8_SIZE + common::Serialization::get_vint32_length(expire_blocks_);
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
        ret = output.set_vint32(expire_blocks_);
      }
      return ret;
    }
  }/** end namespace message **/
}/** end namespace tfs **/
