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

#include "block_info_message_v2.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    GetBlockInfoMessageV2::GetBlockInfoMessageV2():
      block_id_(INVALID_BLOCK_ID), mode_(0)
    {
      _packetHeader._pcode = GET_BLOCK_INFO_MESSAGE_V2;
    }

    GetBlockInfoMessageV2::~GetBlockInfoMessageV2()
    {
    }

    int GetBlockInfoMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(mode_);
      }
      return ret;
    }

    int GetBlockInfoMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      return ret;
    }

    int64_t GetBlockInfoMessageV2::length() const
    {
      return INT_SIZE + INT64_SIZE;
    }

    GetBlockInfoRespMessageV2::GetBlockInfoRespMessageV2()
    {
      _packetHeader._pcode = GET_BLOCK_INFO_RESP_MESSAGE_V2;
    }

    GetBlockInfoRespMessageV2::~GetBlockInfoRespMessageV2()
    {
    }

    int GetBlockInfoRespMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = block_meta_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(block_meta_.length());
      }
      return ret;
    }

    int GetBlockInfoRespMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = block_meta_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(block_meta_.length());
      }
      return ret;
    }

    int64_t GetBlockInfoRespMessageV2::length() const
    {
      return block_meta_.length();
    }

    BatchGetBlockInfoMessageV2::BatchGetBlockInfoMessageV2()
    {
      _packetHeader._pcode = BATCH_GET_BLOCK_INFO_MESSAGE_V2;
    }

    BatchGetBlockInfoMessageV2::~BatchGetBlockInfoMessageV2()
    {
    }

    int BatchGetBlockInfoMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_vint64(block_ids_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(mode_);
      }
      return ret;
    }

    int BatchGetBlockInfoMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_vint64(block_ids_);
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      return ret;
    }

    int64_t BatchGetBlockInfoMessageV2::length() const
    {
      return Serialization::get_vint64_length(block_ids_) + INT_SIZE;
    }

    BatchGetBlockInfoRespMessageV2::BatchGetBlockInfoRespMessageV2()
    {
      _packetHeader._pcode = BATCH_GET_BLOCK_INFO_RESP_MESSAGE_V2;
    }

    BatchGetBlockInfoRespMessageV2::~BatchGetBlockInfoRespMessageV2()
    {
    }

    int BatchGetBlockInfoRespMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int32(block_metas_.size());
      if (TFS_SUCCESS == ret)
      {
        std::vector<BlockMeta>::const_iterator iter = block_metas_.begin();
        for ( ; iter != block_metas_.end() && TFS_SUCCESS == ret; iter++)
        {
          int64_t pos = 0;
          ret = iter->serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(iter->length());
          }
        }
      }
      return ret;
    }

    int BatchGetBlockInfoRespMessageV2::deserialize(Stream& input)
    {
      int32_t map_size = 0;
      int ret = input.get_int32(&map_size);
      for (int i = 0; i < map_size && TFS_SUCCESS == ret; i++)
      {
        BlockMeta block_meta;
        int64_t pos = 0;
        ret = block_meta.deserialize(input.get_data(), input.get_data_length(), pos);
        if (TFS_SUCCESS == ret)
        {
          input.drain(block_meta.length());
          block_metas_.push_back(block_meta);
        }
      }
      return ret;
    }

    int64_t BatchGetBlockInfoRespMessageV2::length() const
    {
      int64_t len = INT_SIZE;
      std::vector<BlockMeta>::const_iterator iter = block_metas_.begin();
      for ( ; iter != block_metas_.end(); iter++)
      {
        len += iter->length();
      }
      return len;
    }

    NewBlockMessageV2::NewBlockMessageV2()
    {
      _packetHeader._pcode = NEW_BLOCK_MESSAGE_V2;
    }

    NewBlockMessageV2::~NewBlockMessageV2()
    {
    }

    int NewBlockMessageV2::serialize(Stream& output) const
    {
      return output.set_vint64(block_ids_);
    }

    int NewBlockMessageV2::deserialize(Stream& input)
    {
      return input.get_vint64(block_ids_);
    }

    int64_t NewBlockMessageV2::length() const
    {
      return Serialization::get_vint64_length(block_ids_);
    }

    RemoveBlockMessageV2::RemoveBlockMessageV2():
      block_id_(INVALID_BLOCK_ID)
    {
      _packetHeader._pcode = REMOVE_BLOCK_MESSAGE_V2;
    }

    RemoveBlockMessageV2::~RemoveBlockMessageV2()
    {
    }

    int RemoveBlockMessageV2::serialize(Stream& output) const
    {
      return output.set_int64(block_id_);
    }

    int RemoveBlockMessageV2::deserialize(Stream& input)
    {
      return input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
    }

    int64_t RemoveBlockMessageV2::length() const
    {
      return INT64_SIZE;
    }

    UpdateBlockInfoMessageV2::UpdateBlockInfoMessageV2():
      server_id_(INVALID_SERVER_ID), unlink_flag_(UNLINK_FLAG_NO)
    {
      _packetHeader._pcode = UPDATE_BLOCK_INFO_MESSAGE_V2;
    }

    UpdateBlockInfoMessageV2::~UpdateBlockInfoMessageV2()
    {
    }

    int UpdateBlockInfoMessageV2::serialize(Stream& output) const
    {
      int64_t pos = 0;
      int ret = block_info_.serialize(output.get_free(), output.get_free_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        output.pour(block_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_id_);
      }

      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(unlink_flag_);
      }

      return ret;
    }

    int UpdateBlockInfoMessageV2::deserialize(Stream& input)
    {
      int64_t pos = 0;
      int ret = block_info_.deserialize(input.get_data(), input.get_data_length(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drain(block_info_.length());
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&server_id_));
      }

      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(reinterpret_cast<int32_t*> (&unlink_flag_));
      }

      return ret;
    }

    int64_t UpdateBlockInfoMessageV2::length() const
    {
      return block_info_.length() + INT64_SIZE + INT_SIZE;
    }

    RepairBlockMessageV2::RepairBlockMessageV2():
      block_id_(INVALID_BLOCK_ID), server_id_(INVALID_SERVER_ID),
      family_id_(INVALID_FAMILY_ID)
    {
      _packetHeader._pcode = REPAIR_BLOCK_MESSAGE_V2;
    }

    RepairBlockMessageV2::~RepairBlockMessageV2()
    {
    }

    int RepairBlockMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int32(type_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(server_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(family_id_);
      }

      return ret;
    }

    int RepairBlockMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int32(reinterpret_cast<int32_t *>(&type_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&server_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&family_id_));
      }

      return ret;
    }

    int64_t RepairBlockMessageV2::length() const
    {
      return INT_SIZE + 3 * INT64_SIZE;
    }

  }
}
