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

#include <open_file_message.h>

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    OpenFileMessage::OpenFileMessage():
      block_id_(INVALID_BLOCK_ID), mode_(0)
    {
      _packetHeader._pcode = OPEN_FILE_MESSAGE;
    }

    OpenFileMessage::~OpenFileMessage()
    {
    }

    int OpenFileMessage::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(mode_);
      }
      return ret;
    }

    int OpenFileMessage::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      return ret;
    }

    int64_t OpenFileMessage::length() const
    {
      return INT_SIZE + INT64_SIZE;
    }

    OpenFileRespMessage::OpenFileRespMessage()
    {
      _packetHeader._pcode = OPEN_FILE_RESP_MESSAGE;
    }

    OpenFileRespMessage::~OpenFileRespMessage()
    {
    }

    int OpenFileRespMessage::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = block_meta_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(block_meta_.length());
      }
      return ret;
    }

    int OpenFileRespMessage::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = block_meta_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(block_meta_.length());
      }
      return ret;
    }

    int64_t OpenFileRespMessage::length() const
    {
      return block_meta_.length();
    }

    BatchOpenFileMessage::BatchOpenFileMessage()
    {
      _packetHeader._pcode = BATCH_OPEN_FILE_MESSAGE;
    }

    BatchOpenFileMessage::~BatchOpenFileMessage()
    {
    }

    int BatchOpenFileMessage::serialize(Stream& output) const
    {
      int ret = output.set_vint64(block_ids_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(mode_);
      }
      return ret;
    }

    int BatchOpenFileMessage::deserialize(Stream& input)
    {
      int ret = input.get_vint64(block_ids_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      return ret;
    }

    int64_t BatchOpenFileMessage::length() const
    {
      return Serialization::get_vint64_length(block_ids_) + INT_SIZE;
    }

    BatchOpenFileRespMessage::BatchOpenFileRespMessage()
    {
      _packetHeader._pcode = BATCH_OPEN_FILE_RESP_MESSAGE;
    }

    BatchOpenFileRespMessage::~BatchOpenFileRespMessage()
    {
    }

    int BatchOpenFileRespMessage::serialize(Stream& output) const
    {
      int ret = output.set_int32(block_metas_.size());
      if (TFS_SUCCESS == ret)
      {
        std::map<uint64_t, BlockMeta>::const_iterator iter = block_metas_.begin();
        for ( ; iter != block_metas_.end() && TFS_SUCCESS == ret; iter++)
        {
          ret = output.set_int64(iter->first);
          if (TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = iter->second.serialize(output.get_free(), output.get_free_length(), pos);
            if (TFS_SUCCESS == ret)
            {
              output.pour(iter->second.length());
            }
          }
        }
      }
      return ret;
    }

    int BatchOpenFileRespMessage::deserialize(Stream& input)
    {
      int32_t map_size = 0;
      int ret = input.get_int32(&map_size);
      for (int i = 0; i < map_size && TFS_SUCCESS == ret; i++)
      {
        uint64_t block_id;
        BlockMeta block_meta;
        ret = input.get_int64(reinterpret_cast<int64_t *>(block_id));
        if (TFS_SUCCESS == ret)
        {
          int64_t pos = 0;
          ret = block_meta.deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_meta.length());
            block_metas_.insert(std::make_pair(block_id, block_meta));
          }
        }
      }
      return ret;
    }

    int64_t BatchOpenFileRespMessage::length() const
    {
      int64_t len = INT_SIZE;
      std::map<uint64_t, BlockMeta>::const_iterator iter = block_metas_.begin();
      for ( ; iter != block_metas_.end(); iter++)
      {
        len += (INT64_SIZE + iter->second.length());
      }
      return len;
    }

  }
}
