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

#include "get_dataserver_all_blocks_header.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {

    GetAllBlocksHeaderMessage::GetAllBlocksHeaderMessage():
      value_(0)
    {
      _packetHeader._pcode = GET_ALL_BLOCKS_HEADER_MESSAGE;
    }

    GetAllBlocksHeaderMessage::~GetAllBlocksHeaderMessage()
    {
    }

    int GetAllBlocksHeaderMessage::serialize(Stream& output) const
    {
      int32_t ret = output.set_int32(value_);
      return ret;
    }

    int GetAllBlocksHeaderMessage::deserialize(Stream& input)
    {
      int ret = input.get_int32(&value_);
      return ret;
    }

    int64_t GetAllBlocksHeaderMessage::length() const
    {
      return INT_SIZE;
    }


    GetAllBlocksHeaderRespMessage::GetAllBlocksHeaderRespMessage()
    {
      _packetHeader._pcode = GET_ALL_BLOCKS_HEADER_RESP_MESSAGE;
    }

    GetAllBlocksHeaderRespMessage::~GetAllBlocksHeaderRespMessage()
    {
    }

    int GetAllBlocksHeaderRespMessage::serialize(Stream& output) const
    {
      int32_t size = all_blocks_header_.size();
      int32_t ret = output.set_int32(size);
      std::vector<common::IndexHeaderV2>::const_iterator it = all_blocks_header_.begin();
      for ( ; it != all_blocks_header_.end() && TFS_SUCCESS == ret; ++it)
      {
        int64_t pos = 0;
        ret = it->serialize(output.get_free(), output.get_free_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          output.pour(it->length());
        }
      }
      return ret;
    }

    int GetAllBlocksHeaderRespMessage::deserialize(Stream& input)
    {
      int32_t size = 0;
      int32_t ret = input.get_int32(&size);
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2 header;
        for (int32_t i = 0; i < size && TFS_SUCCESS == ret; ++i)
        {
          int64_t pos = 0;
          ret = header.deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(header.length());
            all_blocks_header_.push_back(header);
          }
        }
      }

      return ret;
    }

    int64_t GetAllBlocksHeaderRespMessage::length() const
    {
      IndexHeaderV2 header;
      return INT_SIZE + all_blocks_header_.size() * header.length();
    }

  }
}
