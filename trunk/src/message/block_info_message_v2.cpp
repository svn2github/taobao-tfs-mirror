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
      block_id_(INVALID_BLOCK_ID), mode_(0), flag_(0)
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
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }
      return ret;
    }

    int GetBlockInfoMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t* >(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      if (TFS_SUCCESS == ret && input.get_data_length() > 0)
      {
        ret = input.get_int32(&flag_);
      }
      return ret;
    }

    int64_t GetBlockInfoMessageV2::length() const
    {
      return INT_SIZE * 2 + INT64_SIZE;
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

    BatchGetBlockInfoMessageV2::BatchGetBlockInfoMessageV2():
      mode_(0), size_(0), flag_(0)
    {
      _packetHeader._pcode = BATCH_GET_BLOCK_INFO_MESSAGE_V2;
    }

    BatchGetBlockInfoMessageV2::~BatchGetBlockInfoMessageV2()
    {
    }

    int BatchGetBlockInfoMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int32(size_);
      for (int32_t i = 0; i < size_; i++)
      {
        output.set_int64(block_ids_[i]);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(mode_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(flag_);
      }
      return ret;
    }

    int BatchGetBlockInfoMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int32(&size_);
      for (int32_t i = 0; i < size_; i++)
      {
        input.get_int64(reinterpret_cast<int64_t* >(&block_ids_[i]));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&mode_);
      }
      if (TFS_SUCCESS == ret && input.get_data_length() > 0)
      {
        ret = input.get_int32(&flag_);
      }
      return ret;
    }

    int64_t BatchGetBlockInfoMessageV2::length() const
    {
      return INT_SIZE * 3 + size_ * INT64_SIZE;
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
      int ret = ((size_ >= 0) && (size_ <= MAX_BATCH_SIZE)) ? TFS_SUCCESS: TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size_; i++)
        {
          int64_t pos = 0;
          ret = block_metas_[i].serialize(output.get_free(), output.get_free_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            output.pour(block_metas_[i].length());
          }
        }

      }
      return ret;
    }

    int BatchGetBlockInfoRespMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int32(&size_);
      if (TFS_SUCCESS == ret)
      {
        ret = ((size_ >= 0) && (size_ <= MAX_BATCH_SIZE)) ? TFS_SUCCESS: TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; i < size_ && TFS_SUCCESS == ret; i++)
        {
          int64_t pos = 0;
          ret = block_metas_[i].deserialize(input.get_data(), input.get_data_length(), pos);
          if (TFS_SUCCESS == ret)
          {
            input.drain(block_metas_[i].length());
          }
        }
      }
      return ret;
    }

    int64_t BatchGetBlockInfoRespMessageV2::length() const
    {
      int64_t len = INT_SIZE;
      for (int32_t i = 0; i < size_; i++)
      {
        len +=  block_metas_[i].length();
      }
      return len;
    }

    NewBlockMessageV2::NewBlockMessageV2():
      family_id_(INVALID_FAMILY_ID), index_num_(0), expire_time_(0), tmp_(0)
    {
      _packetHeader._pcode = NEW_BLOCK_MESSAGE_V2;
    }

    NewBlockMessageV2::~NewBlockMessageV2()
    {
    }

    int NewBlockMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int64(family_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(index_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int32(expire_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(tmp_);
      }

      return ret;
    }

    int NewBlockMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int64(reinterpret_cast<int64_t *>(&family_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&index_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&expire_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&tmp_);
      }

      return ret;
    }

    int64_t NewBlockMessageV2::length() const
    {
      return 2 * INT64_SIZE + 2 * INT_SIZE + INT8_SIZE;
    }

    RemoveBlockMessageV2::RemoveBlockMessageV2():
      block_id_(INVALID_BLOCK_ID), tmp_(0)
    {
      _packetHeader._pcode = REMOVE_BLOCK_MESSAGE_V2;
    }

    RemoveBlockMessageV2::~RemoveBlockMessageV2()
    {
    }

    int RemoveBlockMessageV2::serialize(Stream& output) const
    {
      int ret = output.set_int64(block_id_);
      if (TFS_SUCCESS == ret)
      {
        ret = output.set_int8(tmp_);
      }
      return ret;
    }

    int RemoveBlockMessageV2::deserialize(Stream& input)
    {
      int ret = input.get_int64(reinterpret_cast<int64_t *>(&block_id_));
      if (TFS_SUCCESS == ret)
      {
        ret = input.get_int8(&tmp_);
      }
      return ret;
    }

    int64_t RemoveBlockMessageV2::length() const
    {
      return INT64_SIZE + INT8_SIZE;
    }

    UpdateBlockInfoMessageV2::UpdateBlockInfoMessageV2():
      server_id_(INVALID_SERVER_ID), type_(UPDATE_BLOCK_INFO_WRITE)
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
        ret = output.set_int32(type_);
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
        ret = input.get_int32(reinterpret_cast<int32_t*> (&type_));
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

    BlockFileInfoMessageV2::BlockFileInfoMessageV2() :
      block_id_(0)
    {
      _packetHeader._pcode = common::BLOCK_FILE_INFO_MESSAGE_V2;
      fileinfo_list_.clear();
    }

    BlockFileInfoMessageV2::~BlockFileInfoMessageV2()
    {
    }

    int BlockFileInfoMessageV2::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int32_t ret = input.get_int64(reinterpret_cast<int64_t*> (&block_id_));
      if (common::TFS_SUCCESS == ret)
      {
        ret = input.get_int32(&size);
      }
      if (common::TFS_SUCCESS == ret)
      {
        common::FileInfoV2 info;
        for (int32_t i = 0; i < size; ++i)
        {
          int64_t pos = 0;
          ret = info.deserialize(input.get_data(), input.get_data_length(), pos);
          if (common::TFS_SUCCESS == ret)
          {
            input.drain(info.length());
            fileinfo_list_.push_back(info);
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    int64_t BlockFileInfoMessageV2::length() const
    {
      common::BlockInfoV2 info;
      return common::INT_SIZE + common::INT64_SIZE + fileinfo_list_.size() * info.length();
    }

    int BlockFileInfoMessageV2::serialize(common::Stream& output)  const
    {
      int32_t ret = output.set_int64(block_id_);
      if (common::TFS_SUCCESS == ret)
      {
        ret = output.set_int32(fileinfo_list_.size());
      }
      if (common::TFS_SUCCESS == ret)
      {
        common::FILE_INFO_LIST_V2::const_iterator iter = fileinfo_list_.begin();
        for (; iter != fileinfo_list_.end(); ++iter)
        {
          int64_t pos = 0;
          ret = (*iter).serialize(output.get_free(), output.get_free_length(), pos);
          if (common::TFS_SUCCESS == ret)
          {
            output.pour((*iter).length());
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    BlockStatisticVisitInfoMessage::BlockStatisticVisitInfoMessage()
    {
      _packetHeader._pcode = common::GET_BLOCK_STATISTIC_VISIT_INFO_MESSAGE;
    }

    BlockStatisticVisitInfoMessage::~BlockStatisticVisitInfoMessage()
    {

    }

    int BlockStatisticVisitInfoMessage::serialize(common::Stream& output) const
    {
      int32_t ret = output.set_int32(block_statistic_visit_maps_.size());
      if (common::TFS_SUCCESS == ret)
      {
        std::map<uint64_t, common::ThroughputV2>::const_iterator iter = block_statistic_visit_maps_.begin();
        for (; iter != block_statistic_visit_maps_.end() && common::TFS_SUCCESS == ret; ++iter)
        {
          ret = output.set_int64(iter->first);
          if (common::TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = iter->second.serialize(output.get_free(), output.get_free_length(), pos);
            if (common::TFS_SUCCESS == ret)
            {
              output.pour(iter->second.length());
            }
          }
        }
      }
      return ret;
    }

    int BlockStatisticVisitInfoMessage::deserialize(common::Stream& input)
    {
      int32_t size = 0;
      int32_t ret = input.get_int32(&size);
      if (common::TFS_SUCCESS == ret)
      {
        common::ThroughputV2 tp;
        uint64_t block = common::INVALID_BLOCK_ID;
        for (int32_t index = 0; index < size && common::TFS_SUCCESS == ret; ++index)
        {
          ret = input.get_int64(reinterpret_cast<int64_t*>(&block));
          if (common::TFS_SUCCESS == ret)
          {
            int64_t pos = 0;
            ret = tp.deserialize(input.get_data(), input.get_data_length(), pos);
            if (common::TFS_SUCCESS == ret)
            {
              input.drain(tp.length());
              block_statistic_visit_maps_.insert(std::map<uint64_t, common::ThroughputV2>::value_type(block, tp));
            }
          }
        }
      }
      return ret;
    }

    int64_t BlockStatisticVisitInfoMessage::length() const
    {
      common::ThroughputV2 tp;
      return common::INT_SIZE + block_statistic_visit_maps_.size() * (tp.length() + common::INT64_SIZE);
    }
  }/** end namespace message **/
}/** end namespace tfs **/
