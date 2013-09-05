/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: internal.h 311 2011-05-18 05:38:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */

#include "internal.h"
#include "serialization.h"
#include "tfs_vector.h"

namespace tfs
{
  namespace common
  {
    int FileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, usize_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, create_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, flag_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, crc_);
      }
      return ret;
    }

    int FileInfo::deserialize(const char*data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &usize_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &modify_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &create_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &flag_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&crc_));
      }
      return ret;
    }

    int64_t FileInfo::length() const
    {
      return INT64_SIZE  + INT_SIZE  * 7;
    }

    int SSMScanParameter::serialize(char* data, const int64_t data_len, int64_t& pos ) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, addition_param1_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, addition_param2_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, start_next_position_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, should_actual_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int16(data, data_len, pos, child_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, end_flag_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, data_.getDataLen());
      }
      if (TFS_SUCCESS == ret)
      {
        if (data_.getDataLen() > 0)
        {
          ret = Serialization::set_bytes(data, data_len, pos, data_.getData(), data_.getDataLen());
        }
      }
      return ret;
    }

    int SSMScanParameter::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &addition_param1_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &addition_param2_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&start_next_position_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&should_actual_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int16(data, data_len, pos, &child_type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, &type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, &end_flag_);
      }
      int32_t len = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &len);
      }
      if (TFS_SUCCESS == ret)
      {
        if (len > 0)
        {
          data_.ensureFree(len);
          data_.pourData(len);
          ret = Serialization::get_bytes(data, data_len, pos, data_.getData(), len);
        }
      }
      return ret;
    }

    int64_t SSMScanParameter::length() const
    {
      return INT_SIZE * 4  + INT64_SIZE * 2 + data_.getDataLen();
    }

    int BlockInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, del_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, del_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, seq_no_);
      }
      return ret;
    }

    int BlockInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &del_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &del_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&seq_no_));
      }
      return ret;
    }

    int64_t BlockInfo::length() const
    {
      return INT_SIZE * 7;
    }

    int ReplBlock::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, source_num_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < source_num_ && TFS_SUCCESS == ret; ++index)
          ret = Serialization::set_int64(data, data_len, pos, source_id_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, destination_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, start_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, is_move_);
      }
      return ret;
    }

    int ReplBlock::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &source_num_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < source_num_ && TFS_SUCCESS == ret; ++index)
          ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&source_id_[index]));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&destination_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &start_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &is_move_);
      }
      return ret;
    }

    int64_t ReplBlock::length() const
    {
      return  INT_SIZE * 3 + INT64_SIZE * (2 + source_num_);
    }

    int CheckBlockInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = (NULL != data && data_len - pos >= length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, total_size_);
      }
      return ret;
    }

    int CheckBlockInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = (NULL != data && data_len - pos >= length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&total_size_));
      }
      return ret;
    }

    int64_t CheckBlockInfo::length() const
    {
      return  INT_SIZE * 4;
    }

    int Throughput::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&write_byte_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&write_file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&read_byte_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&read_file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&unlink_file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&fail_write_byte_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&fail_write_file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&fail_read_byte_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&fail_read_file_count_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&fail_unlink_file_count_));
      }
      return ret;
    }
    int Throughput::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, write_byte_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, write_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, read_byte_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, read_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, unlink_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, fail_write_byte_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, fail_write_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, fail_read_byte_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, fail_read_file_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, fail_unlink_file_count_);
      }



      return ret;
    }

    int64_t Throughput::length() const
    {
      return INT64_SIZE * 4;
    }

    int DataServerStatInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &use_capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &total_capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &write_bytes_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &write_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &read_bytes_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &read_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &stat_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::get_int64(data, data_len, pos, &unlink_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &current_load_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &block_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &last_update_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &startup_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &current_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &total_network_bandwith_);
      }
      return ret;
    }

    int DataServerStatInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, use_capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, total_capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, write_bytes_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, write_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, read_bytes_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, read_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, stat_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int8_t index = 0; index < 2 && TFS_SUCCESS == ret; ++index)
          ret =  Serialization::set_int64(data, data_len, pos, unlink_file_count_[index]);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, current_load_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, last_update_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, startup_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, current_time_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, type_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, total_network_bandwith_);
      }
      return ret;
    }

    int64_t DataServerStatInfo::length() const
    {
      return  INT64_SIZE * 15  + INT_SIZE * 7;
    }

    int WriteDataInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &length_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&is_server_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_number_));
      }
      return ret;
    }
    int WriteDataInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, length_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, is_server_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_number_);
      }
      return ret;
    }
    int64_t WriteDataInfo::length() const
    {
      return INT_SIZE * 4 + INT64_SIZE * 2;
    }

    int CloseFileInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&mode_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&crc_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_number_));
      }
      return ret;
    }
    int CloseFileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, mode_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, crc_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_number_);
      }
      return ret;
    }
    int64_t CloseFileInfo::length() const
    {
      return INT_SIZE * 3 + INT64_SIZE * 2;
    }

    int RenameFileInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&new_file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&is_server_));
      }
      return ret;
    }

    int RenameFileInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, new_file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, is_server_);
      }
      return ret;
    }
    int64_t RenameFileInfo::length() const
    {
      return INT_SIZE * 2 + INT64_SIZE * 2;
    }
    int ServerMetaInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &available_);
      }
      return ret;
    }
    int ServerMetaInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, capacity_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, available_);
      }
      return ret;
    }

    int64_t ServerMetaInfo::length() const
    {
      return INT_SIZE * 2;
    }
    int SegmentHead::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_bytes(data, data_len, pos, reserve_,SEGMENT_HEAD_RESERVE_SIZE);
      }
      return ret;

    }
    int SegmentHead::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, count_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_bytes(data, data_len, pos, reserve_, SEGMENT_HEAD_RESERVE_SIZE);
      }
      return ret;
    }
    int64_t SegmentHead::length() const
    {
      return INT_SIZE + INT64_SIZE;
    }
    int SegmentInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &crc_);
      }
      return ret;
    }
    int SegmentInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, offset_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, crc_);
      }
      return ret;
    }
    int64_t SegmentInfo::length() const
    {
      return INT_SIZE * 3 + INT64_SIZE * 2;
    }

    int ClientCmdInformation::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, cmd_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value1_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value3_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value4_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value2_);
      }
      return ret;
    }

    int ClientCmdInformation::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      // change value3&value4 to 64bits, must compatible with old client
      // int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      int32_t ret = TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&cmd_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value1_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &value3_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &value4_);
      }
      if (common::TFS_SUCCESS == ret && data_len > pos)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value2_));
      }
      return ret;
    }

    int64_t ClientCmdInformation::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE * 4;
    }

    int ToolsClientCmdInformation::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, cmd_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value1_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value3_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value4_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, value2_);
      }
      return ret;
    }

    int ToolsClientCmdInformation::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&cmd_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value1_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &value3_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &value4_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&value2_));
      }
      return ret;
    }

    int64_t ToolsClientCmdInformation::length() const
    {
      return common::INT_SIZE + common::INT64_SIZE * 4;
    }

    bool MMapOption::check() const
    {
      return (max_mmap_size_ > 0 && first_mmap_size_ >= 0 && per_mmap_size_ > 0
          && first_mmap_size_ <= max_mmap_size_ && per_mmap_size_ <= max_mmap_size_);
    }

    int MMapOption::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_mmap_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, first_mmap_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, per_mmap_size_);
      }
      return ret;
    }

    int MMapOption::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &max_mmap_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &first_mmap_size_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &per_mmap_size_);
      }
      return ret;
    }

    int64_t MMapOption::length() const
    {
      return INT_SIZE * 3;
    }

    int SuperBlock::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, MAX_DEV_TAG_LEN);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_bytes(data, data_len, pos, mount_tag_, MAX_DEV_TAG_LEN);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, common::MAX_DEV_NAME_LEN);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_bytes(data, data_len, pos, mount_point_, MAX_DEV_NAME_LEN);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int64(data, data_len, pos, mount_point_use_space_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, base_fs_type_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, superblock_reserve_offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, bitmap_start_offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, avg_segment_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, static_cast<int32_t>(block_type_ratio_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, main_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, main_block_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, extend_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, extend_block_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, used_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, used_extend_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, static_cast<int32_t>(hash_slot_ratio_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, hash_slot_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = mmap_option_.serialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::set_int32(data, data_len, pos, version_);
      }
      return ret;
    }

    int SuperBlock::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? common::TFS_SUCCESS : common::TFS_ERROR;
      int32_t str_length = 0;
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &str_length);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = str_length > 0 && str_length <= common::MAX_DEV_NAME_LEN ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          ret = common::Serialization::get_bytes(data, data_len, pos, mount_tag_, str_length);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &time_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        str_length = 0;
        ret = common::Serialization::get_int32(data, data_len, pos, &str_length);
      }

      if (common::TFS_SUCCESS == ret)
      {
        ret = str_length > 0 && str_length <= common::MAX_DEV_NAME_LEN ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          ret = common::Serialization::get_bytes(data, data_len, pos, mount_point_, str_length);
        }
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int64(data, data_len, pos, &mount_point_use_space_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&base_fs_type_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &superblock_reserve_offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &bitmap_start_offset_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &avg_segment_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&block_type_ratio_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &main_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &main_block_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &extend_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &extend_block_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &used_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &used_extend_block_count_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&hash_slot_ratio_));
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &hash_slot_size_);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = mmap_option_.deserialize(data, data_len, pos);
      }
      if (common::TFS_SUCCESS == ret)
      {
        ret = common::Serialization::get_int32(data, data_len, pos, &version_);
      }
      return ret;
    }
    int64_t SuperBlock::length() const
    {
      return 17 * common::INT_SIZE + common::INT64_SIZE + MAX_DEV_TAG_LEN + MAX_DEV_NAME_LEN + mmap_option_.length();
    }
    void SuperBlock::display() const
    {
      std::cout << "tag " << mount_tag_ << std::endl;
      std::cout << "mount time " << time_ << std::endl;
      std::cout << "mount desc " << mount_point_ << std::endl;
      std::cout << "max use space " << mount_point_use_space_ << std::endl;
      std::cout << "base filesystem " << base_fs_type_ << std::endl;
      std::cout << "superblock reserve " << superblock_reserve_offset_ << std::endl;
      std::cout << "bitmap start offset " << bitmap_start_offset_ << std::endl;
      std::cout << "avg inner file size " << avg_segment_size_ << std::endl;
      std::cout << "block type ratio " << block_type_ratio_ << std::endl;
      std::cout << "main block count " << main_block_count_ << std::endl;
      std::cout << "main block size " << main_block_size_ << std::endl;
      std::cout << "extend block count " << extend_block_count_ << std::endl;
      std::cout << "extend block size " << extend_block_size_ << std::endl;
      std::cout << "used main block count " << used_block_count_ << std::endl;
      std::cout << "used extend block count " << used_extend_block_count_ << std::endl;
      std::cout << "hash slot ratio " << hash_slot_ratio_ << std::endl;
      std::cout << "hash slot size " << hash_slot_size_ << std::endl;
      std::cout << "first mmap size " << mmap_option_.first_mmap_size_ << std::endl;
      std::cout << "mmap size step " << mmap_option_.per_mmap_size_ << std::endl;
      std::cout << "max mmap size " << mmap_option_.max_mmap_size_ << std::endl;
      std::cout << "version " << version_ << std::endl;
    }

    const char* dynamic_parameter_str[49] = {
        "log_level",
        "plan_run_flag",
        "task_expired_time",
        "safe_mode_time",
        "max_write_timeout",
        "max_write_file_count",
        "add_primary_block_count",
        "cleanup_write_timeout_threshold",
        "max_use_capacity_ratio",
        "heart_interval",
        "replicate_ratio",
        "replicate_wait_time",
        "compact_delete_ratio",
        "compact_max_load",
        "compact_time_lower",
        "compact_time_upper",
        "max_task_in_machine_nums",
        "discard_newblk_safe_mode_time",
        "discard_max_count",
        "cluster_index",
        "object_dead_max_time",
        "group_count",
        "group_seq",
        "object_clear_max_time",
        "report_block_queue_size",
        "report_block_time_lower",
        "report_block_time_upper",
        "report_block_time_interval",
        "report_block_expired_time",
        "choose_target_server_random_max_nums",
        "max_keepalive_queue_size",
        "marshalling_delete_ratio",
        "marshalling_time_lower",
        "marshalling_time_upper",
        "marshalling_type",
        "max_data_member_num",
        "max_check_member_num",
        "max_marshalling_queue_timeout",
        "move_task_expired_time",
        "compact_task_expired_time",
        "marshalling_task_expired_time",
        "reinstate_task_expired_time",
        "dissolve_task_expired_time",
        "compact_update_ratio",
        "max_mr_network_bandwith_ratio",
        "max_rw_network_bandwith_ratio",
        "compact_family_member_ratio",
        "max_single_machine_network_bandwith",
        "write_file_check_copies_complete"
    };

    const char* planstr[PLAN_TYPE_EC_MARSHALLING+1] =
    {
      "replicate",
      "reinstate",
      "move",
      "compact",
      "dissolve",
      "marshalling"
    };

    int FamilyInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &family_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &family_aid_info_);
      }
      int32_t size = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size);
      }
      if (TFS_SUCCESS == ret)
      {
        uint64_t block = 0;
        int32_t version = 0;
        for (int32_t i = 0; i < size && TFS_SUCCESS == ret; ++i)
        {
          ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&block));
          if (TFS_SUCCESS == ret)
            ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t*>(&version));
          if (TFS_SUCCESS == ret)
            family_member_.push_back(std::make_pair(block, version));
        }
      }
      return ret;
    }

    int FamilyInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, family_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, family_aid_info_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, family_member_.size());
      }
      if (TFS_SUCCESS == ret)
      {
        std::vector<std::pair<uint64_t, int32_t> >::const_iterator iter = family_member_.begin();
        for (; iter != family_member_.end() && TFS_SUCCESS == ret; ++iter)
        {
          ret = Serialization::set_int64(data, data_len, pos, (*iter).first);
          if (TFS_SUCCESS == ret)
            ret = Serialization::set_int32(data, data_len, pos, (*iter).second);
        }
      }
      return ret;
    }

    int64_t FamilyInfo::length() const
    {
      return INT64_SIZE + INT_SIZE + INT_SIZE + family_member_.size() * (INT_SIZE + INT64_SIZE);
    }

    bool FamilyMemberInfo::operator ==(const FamilyMemberInfo& rhs) const
    {
      return block_ == rhs.block_;
    }

    int FamilyMemberInfo::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&server_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&block_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &status_);
      }
      return ret;
    }

    int FamilyMemberInfo::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, server_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, status_);
      }
      return ret;
    }

    int64_t FamilyMemberInfo::length() const
    {
      return INT64_SIZE * 2 + INT_SIZE * 2;
    }


    uint64_t FamilyInfoExt::get_block(uint64_t server_id) const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      uint64_t block_id = INVALID_BLOCK_ID;
      for (int i = member_num - 1; (INVALID_BLOCK_ID == block_id) && (i >= 0); i--)
      {
        if (members_[i].second == server_id)
        {
          block_id = members_[i].first;
        }
      }
      return block_id;
    }

    uint64_t FamilyInfoExt::get_server(uint64_t block_id) const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      uint64_t server_id = INVALID_SERVER_ID;
      for (int i = member_num - 1; (INVALID_SERVER_ID == server_id) && (i >= 0); i--)
      {
        if (members_[i].first == block_id)
        {
          server_id = members_[i].second;
        }
      }
      return server_id;
    }

    int32_t FamilyInfoExt::get_index_by_server(uint64_t server_id) const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t index = 0;
      for (; index < member_num; index++)
      {
        if (members_[index].second == server_id)
        {
          break;
        }
      }

      return (index < member_num) ? index : -1;
    }

    int32_t FamilyInfoExt::get_index_by_block(uint64_t block_id) const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t index = 0;
      for (; index < member_num; index++)
      {
        if (members_[index].first == block_id)
        {
          break;
        }
      }

      return (index < member_num) ? index : -1;
    }

    void FamilyInfoExt::get_check_servers(std::vector<uint64_t>& servers) const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      servers.clear();
      for (int i = data_num; i < member_num; i++)
      {
        servers.push_back(members_[i].second);
      }
    }

    int32_t FamilyInfoExt::get_alive_data_num() const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);

      int32_t count = 0;
      for (int32_t i = 0; i < data_num; i++)
      {
        if (INVALID_SERVER_ID != members_[i].second)
        {
          count++;
        }
      }
      return count;
    }

    int32_t FamilyInfoExt::get_alive_check_num() const
    {
      const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info_);
      const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info_);
      const int32_t member_num = data_num + check_num;

      int32_t count = 0;
      for (int32_t i = data_num; i < member_num; i++)
      {
        if (INVALID_SERVER_ID != members_[i].second)
        {
          count++;
        }
      }
      return count;
    }

    int FamilyInfoExt::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &family_id_);
      }

      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_id_))
      {
        ret = Serialization::get_int32(data, data_len, pos, &family_aid_info_);
        if (TFS_SUCCESS == ret)
        {
          const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
          for (uint32_t i = 0; (i < MEMBER_NUM) && (TFS_SUCCESS == ret); i++)
          {
            ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&members_[i].first));
            if (TFS_SUCCESS == ret)
            {
              ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&members_[i].second));
            }
          }
        }
      }

      return ret;

    }

    int FamilyInfoExt::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, family_id_);
      }

      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_id_))
      {
        ret = Serialization::set_int32(data, data_len, pos, family_aid_info_);
        if (TFS_SUCCESS == ret)
        {
          const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
          for (uint32_t i = 0; (i < MEMBER_NUM) && (TFS_SUCCESS == ret); i++)
          {
            ret = Serialization::set_int64(data, data_len, pos, members_[i].first);
            if (TFS_SUCCESS == ret)
            {
              ret = Serialization::set_int64(data, data_len, pos, members_[i].second);
            }
          }
        }
      }

      return ret;
    }

    int64_t FamilyInfoExt::length() const
    {
      int64_t len = INT64_SIZE;
      if (INVALID_FAMILY_ID != family_id_)
      {
        const uint32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info_) + GET_CHECK_MEMBER_NUM(family_aid_info_);
        len += (INT_SIZE + MEMBER_NUM * (INT64_SIZE + INT64_SIZE));
      }
      return len;
    }

    int BlockMeta::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size_; i++)
        {
          ret = Serialization::set_int64(data, data_len, pos, ds_[i]);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = family_info_.serialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, result_);
      }

      return ret;
    }

    int BlockMeta::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t* >(&block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
        if (TFS_SUCCESS == ret)
        {
          ret = ((size_ >= 0) && (size_ <= MAX_REPLICATION_NUM)) ? TFS_SUCCESS: TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size_; i++)
        {
          ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t* >(&ds_[i]));
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = family_info_.deserialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &result_);
      }
      return ret;
    }

    int64_t BlockMeta::length() const
    {
      return INT64_SIZE + INT_SIZE * 3 + size_ * INT64_SIZE + family_info_.length();
    }

    int FileSegment::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, length_);
      }

      return ret;
    }

    int FileSegment::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t *>(&block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t *>(&file_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &length_);
      }

      return ret;
    }

    int64_t FileSegment::length() const
    {
      return (2 * INT64_SIZE + 2 * INT_SIZE);
    }

    int ThroughputV2::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int64_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      ret = Serialization::set_int64(data, data_len, pos, write_bytes_);
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, read_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, update_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, unlink_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, write_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, read_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, update_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, unlink_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, last_statistics_time_);
      }

      return ret;
    }

    int ThroughputV2::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int64_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      ret = Serialization::get_int64(data, data_len, pos, &write_bytes_);
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &read_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &update_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &unlink_bytes_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &write_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &read_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &update_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &unlink_visit_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &last_statistics_time_);
      }

      return ret;

    }
    int64_t ThroughputV2::length() const
    {
      return INT64_SIZE * 9;
    }

    bool IndexHeaderV2::check_need_mremap(const double threshold) const
    {
      bool ret = (file_info_bucket_size_ > 0 && used_file_info_bucket_size_ > 0);
      if (ret)
      {
        double ratio = static_cast<double>(used_file_info_bucket_size_) / static_cast<double>(file_info_bucket_size_);
        ret = ratio >= threshold;
        //TBSYS_LOG(INFO, "need remap %d, %e, %e", ret, ratio, threshold);
      }
      return ret;
    }

    int IndexHeaderV2::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = info_.serialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = throughput_.serialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, used_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, avail_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, marshalling_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, seq_no_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int16(data, data_len, pos, file_info_bucket_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int16(data, data_len, pos, used_file_info_bucket_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int8(data, data_len, pos, max_index_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; (TFS_SUCCESS == ret) && (i < 27); i++)
        {
          ret = Serialization::set_int8(data, data_len, pos, reserve_[i]);
        }
      }

      return ret;
    }

    int IndexHeaderV2::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = info_.deserialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = throughput_.deserialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &used_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &avail_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &marshalling_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos,
            reinterpret_cast<int32_t* >(&seq_no_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int16(data, data_len, pos,
            reinterpret_cast<int16_t* >(&file_info_bucket_size_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int16(data, data_len, pos,
            reinterpret_cast<int16_t* >(&used_file_info_bucket_size_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int8(data, data_len, pos, &max_index_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int i = 0; (TFS_SUCCESS == ret) && (i < 27); i++)
        {
          ret = Serialization::get_int8(data, data_len, pos, &reserve_[i]);
        }
      }

      return ret;
    }

    int64_t IndexHeaderV2::length() const
    {
      return info_.length() + throughput_.length() + INT_SIZE * 12;
    }

    int IndexDataV2::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = header_.deserialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t finfo_size = 0;
        FileInfoV2 finfo;
        ret = Serialization::get_int32(data, data_len, pos, &finfo_size);
        for (int32_t i = 0; (TFS_SUCCESS == ret) && (i < finfo_size); i++)
        {
          ret =  finfo.deserialize(data, data_len, pos);
          if (TFS_SUCCESS == ret)
          {
            finfos_.push_back(finfo);
          }
        }
        if (finfo_size != static_cast<int32_t>(finfos_.size()))
        {
          ret = TFS_ERROR;
        }
      }

      return ret;
    }

    int IndexDataV2::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = header_.serialize(data, data_len, pos);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, finfos_.size());
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < finfos_.size()); i++)
        {
          ret =  finfos_[i].serialize(data, data_len, pos);
        }
      }

      return ret;
    }

    int64_t IndexDataV2::length() const
    {
      int64_t len = header_.length() + INT_SIZE;
      for (uint32_t i = 0; i < finfos_.size(); i++)
      {
        len += finfos_[i].length();
      }
      return len;
    }

    int ECMeta::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t* >(&family_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &used_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &mars_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_step_);
      }

      return ret;
    }

    int ECMeta::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, family_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, used_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, mars_offset_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_step_);
      }

      return ret;
    }

    int64_t ECMeta::length() const
    {
      return INT64_SIZE + INT_SIZE * 3;
    }

    int FileInfoV2::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, id_);
      }

      if ((TFS_SUCCESS == ret) && (INVALID_FILE_ID != id_))
      {
        ret = Serialization::set_int32(data, data_len, pos, offset_);

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int32(data, data_len, pos, size_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int8(data, data_len, pos, status_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int32(data, data_len, pos, crc_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int32(data, data_len, pos, modify_time_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int32(data, data_len, pos, create_time_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::set_int16(data, data_len, pos, next_);
        }
      }

      return ret;
    }

    int FileInfoV2::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t *>(&id_));
      }

      if ((TFS_SUCCESS == ret) && (INVALID_FILE_ID != id_))
      {
        ret = Serialization::get_int32(data, data_len, pos, &offset_);
        if (TFS_SUCCESS == ret)
        {
          int32_t size = 0;
          ret = Serialization::get_int32(data, data_len, pos, &size);
          size_ = size;
        }

        if (TFS_SUCCESS == ret)
        {
          int8_t status = 0;
          ret = Serialization::get_int8(data, data_len, pos, &status);
          status_ = status;
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_int32(data, data_len, pos, reinterpret_cast<int32_t *>(&crc_));
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_int32(data, data_len, pos, &modify_time_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_int32(data, data_len, pos, &create_time_);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = Serialization::get_int16(data, data_len, pos, reinterpret_cast<int16_t *>(&next_));
        }
      }

      return ret;
    }

    int64_t FileInfoV2::length() const
    {
      int64_t len = INT64_SIZE;
      if (INVALID_FILE_ID !=  id_)
      {
        len += 5 * INT_SIZE + INT8_SIZE + INT16_SIZE;
      }
      return len;
    }

    int BlockInfoV2::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, family_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, version_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, del_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, del_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, update_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, update_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, last_update_time_);
      }

      for (int i = 0; (TFS_SUCCESS == ret) && i < 2; i++)
      {
        ret = Serialization::set_int32(data, data_len, pos, reserve_[i]);
      }

      return ret;
    }

    int BlockInfoV2::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t *>(&block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t *>(&family_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &version_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &del_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &del_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &update_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &update_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &last_update_time_);
      }

      for (int i = 0; (TFS_SUCCESS == ret) && i < 2; i++)
      {
        ret = Serialization::get_int32(data, data_len, pos, &reserve_[i]);
      }

      return ret;
    }

    int64_t BlockInfoV2::length() const
    {
      return 10 * INT_SIZE + 2 * INT64_SIZE;
    }

    int TimeRange::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &start_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &end_);
      }
      return ret;
    }

    int TimeRange::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, start_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, end_);
      }
      return ret;
    }

    int64_t TimeRange::length() const
    {
      return INT64_SIZE * 2;
    }

    int BlockLease::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&block_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&servers_[index]));
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &result_);
      }

      return ret;
    }

    int BlockLease::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, size_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < size_ && TFS_SUCCESS == ret; index++)
        {
          ret = Serialization::set_int64(data, data_len, pos, servers_[index]);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, result_);
      }

      return ret;
    }

    int64_t BlockLease::length() const
    {
      return INT64_SIZE + 2 * INT_SIZE + size_ * INT64_SIZE;
    }

    int LeaseMeta::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&lease_id_));
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &lease_expire_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &lease_renew_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &renew_retry_times_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &renew_retry_timeout_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &max_mr_network_bandwith_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &max_rw_network_bandwith_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &ns_role_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &max_block_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int32(data, data_len, pos, &max_write_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < 4 && TFS_SUCCESS == ret; index++)
        {
          ret = Serialization::get_int32(data, data_len, pos, &reserve_[index]);
        }
      }

      return ret;
    }

    int LeaseMeta::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, lease_id_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, lease_expire_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, lease_renew_time_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, renew_retry_times_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, renew_retry_timeout_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_mr_network_bandwith_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_rw_network_bandwith_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, ns_role_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_block_size_);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int32(data, data_len, pos, max_write_file_count_);
      }

      if (TFS_SUCCESS == ret)
      {
        for (int index = 0; index < 4 && TFS_SUCCESS == ret; index++)
        {
          ret = Serialization::set_int32(data, data_len, pos, reserve_[index]);
        }
      }

      return ret;
    }

    int64_t LeaseMeta::length() const
    {
      return INT64_SIZE + 13 * INT_SIZE;
    }

    int SyncFileEntry::deserialize(const char* data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, &app_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&block_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&file_id_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&source_ds_addr_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&source_ns_addr_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int64(data, data_len, pos, reinterpret_cast<int64_t*>(&dest_ns_addr_));
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::get_int16(data, data_len, pos, &type_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < 2; ++index)
        {
          ret = Serialization::get_int64(data, data_len, pos, &reserve_[index]);
        }
      }
      return ret;
    }

    int SyncFileEntry::serialize(char* data, const int64_t data_len, int64_t& pos) const
    {
      int32_t ret = NULL != data && data_len - pos >= length() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, app_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, block_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, file_id_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, source_ds_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, source_ns_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int64(data, data_len, pos, dest_ns_addr_);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = Serialization::set_int16(data, data_len, pos, type_);
      }
      if (TFS_SUCCESS == ret)
      {
        for (int32_t index = 0; index < 2; ++index)
        {
          ret = Serialization::set_int64(data, data_len, pos, reserve_[index]);
        }
      }
      return ret;
    }

    int64_t SyncFileEntry::length() const
    {
      return INT64_SIZE * 8 + INT16_SIZE;
    }

    bool SyncFileEntry::check_need_sync(const time_t now) const
    {
      bool ret = 0 == sync_fail_count_;
      if (!ret)
      {
        //这里可以设计一些同步失败后的策略
        ret = now - last_sync_time_ >= 30;//30s
      }
      return ret;
    }

    void SyncFileEntry::dump(const int32_t level, const char* file, const int32_t line,
                const char* function, pthread_t thid, const char* format, ...)
    {
      if (level <= TBSYS_LOGGER._level)
      {
        char msgstr[256] = {'\0'};/** include '\0'*/
        va_list ap;
        va_start(ap, format);
        vsnprintf(msgstr, 256, NULL == format ? "" : format, ap);
        va_end(ap);
        std::stringstream str;
        TBSYS_LOGGER.logMessage(level, file, line, function, thid,
          "%s, app_id: %"PRI64_PREFIX"d, block_id: %"PRI64_PREFIX"u, file_id: %"PRI64_PREFIX"u, dest_ns_addr: %s, source_ds addr: %s, source ns addr: %s type: %d\
          last_sync_time: %d, sync_fail_count: %u",
            msgstr, app_id_, block_id_, file_id_, tbsys::CNetUtil::addrToString(dest_ns_addr_).c_str(),
            tbsys::CNetUtil::addrToString(source_ds_addr_).c_str(),
            tbsys::CNetUtil::addrToString(source_ns_addr_).c_str(), type_, last_sync_time_, sync_fail_count_);
      }
    }
  } /** nameserver **/
}/** tfs **/
