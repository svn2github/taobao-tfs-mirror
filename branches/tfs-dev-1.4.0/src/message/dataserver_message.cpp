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
      int32_t iret = ds_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (common::TFS_SUCCESS == iret)
      {
        input.drain(ds_.length());
        iret = input.get_int32(reinterpret_cast<int32_t*> (&has_block_));
        if (common::TFS_SUCCESS == iret)
        {
          if (has_block_  == common::HAS_BLOCK_FLAG_YES)
          {
            int32_t size = 0;
            iret = input.get_int32(&size);
            if (common::TFS_SUCCESS == iret)
            {
              common::BlockInfo info;
              for (int32_t i = 0; i < size; ++i)
              {
                pos  = 0;
                iret = info.deserialize(input.get_data(), input.get_data_length(), pos);
                if (common::TFS_SUCCESS == iret)
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
      return iret;
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
      int32_t iret = ds_.id_ <= 0 ? common::TFS_ERROR : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == iret)
      {
        iret = ds_.serialize(output.get_free(), output.get_free_length(), pos);
        if (common::TFS_SUCCESS == iret)
        {
          output.pour(ds_.length());
          iret = output.set_int32(has_block_);
        }
      }
      if (common::TFS_SUCCESS == iret)
      {
        if (has_block_ == common::HAS_BLOCK_FLAG_YES)
        {
          iret = output.set_int32(blocks_.size());
          if (common::TFS_SUCCESS == iret)
          {
            std::vector<common::BlockInfo>::const_iterator iter = blocks_.begin();
            for (; iter != blocks_.end(); ++iter)
            {
              pos = 0;
              iret = const_cast<common::BlockInfo*>((&(*iter)))->serialize(output.get_free(), output.get_free_length(), pos);
              if (common::TFS_SUCCESS == iret)
                output.pour((*iter).length());
              else
                break;
            }
          }
        }
      }
      return iret;
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

    /*SuspectDataserverMessage::SuspectDataserverMessage():
      server_id_(0)
    {
      _packetHeader._pcode = common::SUSPECT_DATASERVER_MESSAGE;
    }

    SuspectDataserverMessage::~SuspectDataserverMessage()
    {
    }

    int SuspectDataserverMessage::deserialize(common::Stream& input)
    {
      return input.get_int64(reinterpret_cast<int64_t*> (&server_id_));
    }

    int64_t SuspectDataserverMessage::length() const
    {
      return common::INT64_SIZE;
    }

    int SuspectDataserverMessage::serialize(common::Stream& output) const 
    {
      return output.set_int64(server_id_);
    }*/
  }
}
