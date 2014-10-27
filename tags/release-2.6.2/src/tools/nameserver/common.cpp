/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: common.cpp 447 2011-06-08 09:32:19Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "common.h"
#include "requester/ds_requester.h"
using namespace tfs::common;

namespace tfs
{
  namespace tools
  {
    ServerBase::ServerBase()
    {
    }
    ServerBase::~ServerBase()
    {
    }

    int ServerBase::serialize(tbnet::DataBuffer& output, int32_t& length)
    {
      return server_stat_.serialize(output, length);
    }

    int ServerBase::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t type)
    {
      int ret = server_stat_.deserialize(input, length, offset);
      int len = 0;
      if (TFS_SUCCESS == ret)
      {
        if (type & SERVER_TYPE_BLOCK_LIST)
        {
          len = input.getDataLen();
          int32_t hold_size = input.readInt32();
          while (hold_size > 0)
          {
            hold_.insert(input.readInt64());
            hold_size--;
          }
          offset += (len - input.getDataLen());
        }

        if (type & SERVER_TYPE_BLOCK_WRITABLE)
        {
          len = input.getDataLen();
          int32_t writable_size = input.readInt32();
          while (writable_size > 0)
          {
            writable_.insert(input.readInt64());
            writable_size--;
          }
          offset += (len - input.getDataLen());
        }

        if (type & SERVER_TYPE_BLOCK_MASTER)
        {
          len = input.getDataLen();
          int32_t master_size = input.readInt32();
          while (master_size > 0)
          {
            master_.insert(input.readInt64());
            master_size--;
          }
          offset += (len - input.getDataLen());
        }
      }
      return ret;
    }

    int ServerBase::fetch_family_set()
    {
      const uint64_t ds_id = server_stat_.id_;
      std::vector<BlockInfoV2> block_infos;
      int ret = requester::DsRequester::list_block(ds_id, block_infos);
      for (uint32_t i = 0; i < block_infos.size(); ++i)
      {
        if (INVALID_FAMILY_ID != block_infos[i].family_id_)
        {
          family_set_.insert(block_infos[i].family_id_);
        }
      }
      return ret;
    }

    void ServerBase::dump() const
    {
      TBSYS_LOG(INFO, "server_id: %"PRI64_PREFIX"d, use_capacity: %"PRI64_PREFIX"d, total_capacity: %"PRI64_PREFIX"d, current_load: %d, block_count: %d, last_update_time: %s, startup_time: %s, write_byte: %"PRI64_PREFIX"d, write_file_count: %"PRI64_PREFIX"d, read_byte: %"PRI64_PREFIX"d, read_file_count: %"PRI64_PREFIX"d current_time: %s, status: %d, rb_expired_time_: %"PRI64_PREFIX"d, next_report_block_time: %"PRI64_PREFIX"d, disk_type: %u, rb_status_: %u , hold_size: %zd, writable size: %zd, master size: %zd",
          server_stat_.id_,
          server_stat_.use_capacity_,
          server_stat_.total_capacity_,
          server_stat_.current_load_,
          server_stat_.block_count_,
          Func::time_to_str(server_stat_.last_update_time_).c_str(),
          Func::time_to_str(server_stat_.startup_time_).c_str(),
          server_stat_.total_tp_.write_byte_,
          server_stat_.total_tp_.write_file_count_,
          server_stat_.total_tp_.read_byte_,
          server_stat_.total_tp_.read_file_count_,
          Func::time_to_str(server_stat_.current_time_).c_str(),
          server_stat_.status_,
          server_stat_.rb_expired_time_,
          server_stat_.next_report_block_time_,
          server_stat_.disk_type_,
          server_stat_.rb_status_,
          hold_.size(),
          writable_.size(),
          master_.size()
          );
    }

    //************************BLOCK**************************
    BlockBase::BlockBase()
    {
      memset(&info_, 0, sizeof(info_));
      server_list_.clear();
    }
    BlockBase::~BlockBase()
    {
    }
    int32_t BlockBase::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset, const int8_t type)
    {
      // type must contain SSM_CHILD_BLOCK_TYPE_INFO at lease
      if (input.getDataLen() <= 0 || offset >= length || 0 == (type & SSM_CHILD_BLOCK_TYPE_INFO))
      {
        return EXIT_PARAMETER_ERROR;
      }

      int64_t pos = 0;
      int32_t len = input.getDataLen();
      int32_t ret = info_.deserialize(input.getData(), input.getDataLen(), pos);
      if (TFS_SUCCESS == ret)
      {
        input.drainData(info_.length());
        if (type & SSM_CHILD_BLOCK_TYPE_SERVER)
        {
          int8_t server_size = input.readInt8();
          while (server_size > 0)
          {
            ServerInfo server_info;
            server_info.server_id_ = input.readInt64();
            server_info.family_id_ = input.readInt64();
            server_info.version_   = input.readInt32();

            server_list_.push_back(server_info);
            server_size--;
          }
        }
        if (type & SSM_CHILD_BLOCK_TYPE_STATUS)
        {
          expire_time_ = input.readInt64();
          create_flag_ = input.readInt8();
          in_replicate_queue_ = input.readInt8();
          has_lease_ = input.readInt8();
          choose_master_ = input.readInt8();
          last_leave_time_ = input.readInt64();
        }
        offset += (len - input.getDataLen());
      }
      return ret;
    }

    void BlockBase::dump() const
    {
      TBSYS_LOG(INFO, "family_id: %"PRI64_PREFIX"d,block_id: %"PRI64_PREFIX"u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d,"
          "copys: %Zd, expire_time: %"PRI64_PREFIX"d, create_flag: %d, in_replicate_queue: %d, has_lease: %d, choose_master: %d, last_leave_time: %"PRI64_PREFIX"d",
          info_.family_id_, info_.block_id_, info_.version_, info_.file_count_, info_.size_, info_.del_file_count_, info_.del_size_, server_list_.size(),
          expire_time_, create_flag_, in_replicate_queue_, has_lease_, choose_master_, last_leave_time_);
    }
  }
}
