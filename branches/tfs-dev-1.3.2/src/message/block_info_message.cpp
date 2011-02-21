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
#include "block_info_message.h"

using namespace tfs::common;
using namespace std;

namespace tfs
{
  namespace message
  {

    GetBlockInfoMessage::GetBlockInfoMessage(int32_t mode) :
      block_id_(0), mode_(mode)
    {
      _packetHeader._pcode = GET_BLOCK_INFO_MESSAGE;
    }

    GetBlockInfoMessage::~GetBlockInfoMessage()
    {
    }

    int GetBlockInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &mode_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      get_vint64(&data, &len, fail_server_);

      return TFS_SUCCESS;
    }

    int32_t GetBlockInfoMessage::message_length()
    {
      int32_t len = INT_SIZE * 2 + get_vint64_len(fail_server_);
      return len;
    }

    int GetBlockInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, mode_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, fail_server_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* GetBlockInfoMessage::get_name()
    {
      return "getblockinfomessage";
    }

    Message* GetBlockInfoMessage::create(const int32_t type)
    {
      GetBlockInfoMessage* req_gbi_msg = new GetBlockInfoMessage();
      req_gbi_msg->set_message_type(type);
      return req_gbi_msg;
    }

    SetBlockInfoMessage::SetBlockInfoMessage() :
      block_id_(0), version_(0), lease_(0), has_lease_(false)
    {
      _packetHeader._pcode = SET_BLOCK_INFO_MESSAGE;
      ds_.clear();
    }

    SetBlockInfoMessage::~SetBlockInfoMessage()
    {
    }

    //block_id, server_count, server_id1, server_id2, ..., filename_len, filename
    int SetBlockInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      has_lease_ = parse_special_ds(ds_, version_, lease_);
      return TFS_SUCCESS;
    }

    int32_t SetBlockInfoMessage::message_length()
    {
      int32_t len = INT_SIZE + get_vint64_len(ds_);
      if ((has_lease_ == true) && (ds_.size() > 0))
      {
        len += INT64_SIZE * 3;
      }
      return len;
    }

    int SetBlockInfoMessage::build(char* data, int32_t len)
    {
      if ((has_lease_ == true) && (ds_.size() > 0))
      {
        ds_.push_back(ULONG_LONG_MAX);
        ds_.push_back(static_cast<uint64_t> (version_));
        ds_.push_back(static_cast<uint64_t> (lease_));
      }

      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint64(&data, &len, ds_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      // reparse, avoid push verion&lease again when clone twice;
      has_lease_ = parse_special_ds(ds_, version_, lease_);
      return TFS_SUCCESS;
    }

    char* SetBlockInfoMessage::get_name()
    {
      return "setblockinfomessage";
    }

    Message* SetBlockInfoMessage::create(const int32_t type)
    {
      SetBlockInfoMessage* req_sbi_msg = new SetBlockInfoMessage();
      req_sbi_msg->set_message_type(type);
      return req_sbi_msg;
    }

    void SetBlockInfoMessage::set_read_block_ds(const uint32_t block_id, VUINT64* ds)
    {
      block_id_ = block_id;
      if (ds != NULL)
      {
        ds_ = (*ds);
      }
    }

    void SetBlockInfoMessage::set_write_block_ds(const uint32_t block_id, VUINT64* ds, const int32_t version,
                                                 const int32_t lease)
    {
      block_id_ = block_id;
      if (ds != NULL)
      {
        ds_ = (*ds);
      }
      version_ = version;
      lease_ = lease;
      has_lease_ = true;
    }

    BatchGetBlockInfoMessage::BatchGetBlockInfoMessage(int32_t mode) :
      mode_(mode), block_count_(0)
    {
      _packetHeader._pcode = BATCH_GET_BLOCK_INFO_MESSAGE;
      block_ids_.clear();
    }

    BatchGetBlockInfoMessage::~BatchGetBlockInfoMessage()
    {
    }

    int BatchGetBlockInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &mode_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (mode_ & common::T_READ)
      {
        if (get_vint32(&data, &len, block_ids_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      else if (get_int32(&data, &len, &block_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t BatchGetBlockInfoMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return (mode_ & common::T_READ) ? len + get_vint_len(block_ids_) : len + INT_SIZE;
    }

    int BatchGetBlockInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, mode_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (mode_ & common::T_READ)
      {
        if (set_vint32(&data, &len, block_ids_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      else if (set_int32(&data, &len, block_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* BatchGetBlockInfoMessage::get_name()
    {
      return "batchgetblockinfomessage";
    }

    Message* BatchGetBlockInfoMessage::create(const int32_t type)
    {
      BatchGetBlockInfoMessage* req_gbi_msg = new BatchGetBlockInfoMessage();
      req_gbi_msg->set_message_type(type);
      return req_gbi_msg;
    }

    BatchSetBlockInfoMessage::BatchSetBlockInfoMessage()
    {
      _packetHeader._pcode = BATCH_SET_BLOCK_INFO_MESSAGE;
    }

    BatchSetBlockInfoMessage::~BatchSetBlockInfoMessage()
    {
    }

    // count, blockid, server_count, server_id1, server_id2, ..., blockid, server_count, server_id1 ...
    int BatchSetBlockInfoMessage::parse(char* data, int32_t len)
    {
      int32_t count = 0;
      if (get_int32(&data, &len, &count) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      uint32_t block_id;
      for (int32_t i = 0; i < count; i++)
      {
        BlockInfoSeg block_info;
        if (get_int32(&data, &len, reinterpret_cast<int32_t*>(&block_id)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (get_vint64(&data, &len, block_info.ds_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        block_info.has_lease_ = parse_special_ds(block_info.ds_, block_info.version_, block_info.lease_);
        block_infos_[block_id] = block_info;
      }
      return TFS_SUCCESS;
    }

    int32_t BatchSetBlockInfoMessage::message_length()
    {
      int32_t count = block_infos_.size();
      // count + blockids
      int32_t len = INT_SIZE + count * INT_SIZE;

      // just test first has lease, then all has lease, maybe add mode test
      if (count > 0)
      {
        // ds
        std::map<uint32_t, BlockInfoSeg>::iterator it = block_infos_.begin();
        for (; it != block_infos_.end(); it++)
        {
          len += get_vint64_len(it->second.ds_);
        }

        if (block_infos_.begin()->second.has_lease_)
        {
          // has_lease + lease + version
          len += INT64_SIZE * 3 * count;
        }
      }

      return len;
    }

    int BatchSetBlockInfoMessage::build(char* data, int32_t len)
    {
      // count
      if (set_int32(&data, &len, block_infos_.size()) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      std::map<uint32_t, BlockInfoSeg>::iterator it = block_infos_.begin();
      BlockInfoSeg* block_info = NULL;
      for (; it != block_infos_.end(); it++)
      {
        block_info = &it->second;
        if ((block_info->has_lease_ == true) && (block_info->ds_.size() > 0))
        {
          block_info->ds_.push_back(ULONG_LONG_MAX);
          block_info->ds_.push_back(static_cast<uint64_t> (block_info->version_));
          block_info->ds_.push_back(static_cast<uint64_t> (block_info->lease_));
        }

        // blockid
        if (set_int32(&data, &len, it->first) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        // ds
        if (set_vint64(&data, &len, block_info->ds_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        // reparse, avoid push verion&lease again when clone twice;
        block_info->has_lease_ = parse_special_ds(block_info->ds_, block_info->version_, block_info->lease_);
      }
      return TFS_SUCCESS;
    }

    char* BatchSetBlockInfoMessage::get_name()
    {
      return "batchsetblockinfomessage";
    }

    Message* BatchSetBlockInfoMessage::create(const int32_t type)
    {
      BatchSetBlockInfoMessage* req_sbi_msg = new BatchSetBlockInfoMessage();
      req_sbi_msg->set_message_type(type);
      return req_sbi_msg;
    }

    void BatchSetBlockInfoMessage::set_read_block_ds(const uint32_t block_id, VUINT64& ds)
    {
        block_infos_[block_id] = BlockInfoSeg(ds);
    }

    void BatchSetBlockInfoMessage::set_write_block_ds(const uint32_t block_id, VUINT64& ds,
                                                      const int32_t version, const int32_t lease)
    {
        block_infos_[block_id] = BlockInfoSeg(ds, true, lease, version);
    }

    CarryBlockMessage::CarryBlockMessage()
    {
      _packetHeader._pcode = CARRY_BLOCK_MESSAGE;
      expire_blocks_.clear();
      remove_blocks_.clear();
      new_blocks_.clear();
    }

    CarryBlockMessage::~CarryBlockMessage()
    {
    }

    int CarryBlockMessage::parse(char* data, int32_t len)
    {
      if (get_vint32(&data, &len, expire_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint32(&data, &len, remove_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t CarryBlockMessage::message_length()
    {
      int32_t len = get_vint_len(expire_blocks_) + get_vint_len(remove_blocks_) + get_vint_len(new_blocks_);
      return len;
    }

    int CarryBlockMessage::build(char* data, int32_t len)
    {
      if (set_vint32(&data, &len, expire_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint32(&data, &len, remove_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* CarryBlockMessage::get_name()
    {
      return "CarryBlockMessage";
    }

    Message* CarryBlockMessage::create(const int32_t type)
    {
      CarryBlockMessage* req_cb_msg = new CarryBlockMessage();
      req_cb_msg->set_message_type(type);
      return req_cb_msg;
    }

    void CarryBlockMessage::add_expire_id(const uint32_t block_id)
    {
      expire_blocks_.push_back(block_id);
    }
    void CarryBlockMessage::add_new_id(const uint32_t block_id)
    {
      new_blocks_.push_back(block_id);
    }
    void CarryBlockMessage::add_remove_id(const uint32_t block_id)
    {
      remove_blocks_.push_back(block_id);
    }

    NewBlockMessage::NewBlockMessage()
    {
      _packetHeader._pcode = NEW_BLOCK_MESSAGE;
      new_blocks_.clear();
    }

    NewBlockMessage::~NewBlockMessage()
    {
    }

    int NewBlockMessage::parse(char* data, int32_t len)
    {
      if (get_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t NewBlockMessage::message_length()
    {
      int32_t len = get_vint_len(new_blocks_);
      return len;
    }

    int NewBlockMessage::build(char* data, int32_t len)
    {
      if (set_vint32(&data, &len, new_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* NewBlockMessage::get_name()
    {
      return "NewBlockMessage";
    }

    Message* NewBlockMessage::create(const int32_t type)
    {
      NewBlockMessage* req_nb_msg = new NewBlockMessage();
      req_nb_msg->set_message_type(type);
      return req_nb_msg;
    }

    void NewBlockMessage::add_new_id(const uint32_t block_id)
    {
      new_blocks_.push_back(block_id);
    }

    RemoveBlockMessage::RemoveBlockMessage()
    {
      _packetHeader._pcode = REMOVE_BLOCK_MESSAGE;
      remove_blocks_.clear();
    }

    RemoveBlockMessage::~RemoveBlockMessage()
    {
    }

    int RemoveBlockMessage::parse(char* data, int32_t len)
    {
      if (get_vint32(&data, &len, remove_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t RemoveBlockMessage::message_length()
    {
      int32_t len = get_vint_len(remove_blocks_);
      return len;
    }

    int RemoveBlockMessage::build(char* data, int32_t len)
    {
      if (set_vint32(&data, &len, remove_blocks_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* RemoveBlockMessage::get_name()
    {
      return "RemoveBlockMessage";
    }

    Message* RemoveBlockMessage::create(const int32_t type)
    {
      RemoveBlockMessage* req_rb_msg = new RemoveBlockMessage();
      req_rb_msg->set_message_type(type);
      return req_rb_msg;
    }

    void RemoveBlockMessage::add_remove_id(const uint32_t block_id)
    {
      remove_blocks_.push_back(block_id);
    }

    ListBlockMessage::ListBlockMessage() :
      type_(0)
    {
      _packetHeader._pcode = LIST_BLOCK_MESSAGE;
    }

    ListBlockMessage::~ListBlockMessage()
    {
    }

    int ListBlockMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ListBlockMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return len;
    }

    int ListBlockMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* ListBlockMessage::get_name()
    {
      return "ListBlockMessage";
    }

    Message* ListBlockMessage::create(const int32_t type)
    {
      ListBlockMessage* req_lb_msg = new ListBlockMessage();
      req_lb_msg->set_message_type(type);
      return req_lb_msg;
    }

    RespListBlockMessage::RespListBlockMessage() :
      status_type_(0), need_free_(0)
    {
      _packetHeader._pcode = RESP_LIST_BLOCK_MESSAGE;
      blocks_.clear();
    }

    RespListBlockMessage::~RespListBlockMessage()
    {
    }

    int RespListBlockMessage::parse(char* data, int32_t len)
    {
      int32_t i, size;
      if (get_int32(&data, &len, &status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      need_free_ = 1;
      // m_Blocks
      if (status_type_ & LB_BLOCK)
      {
        if (get_vint32(&data, &len, blocks_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      // m_BlockPairs
      if (status_type_ & LB_PAIRS)
      {
        if (get_int32(&data, &len, &size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        uint32_t block_id;
        for (i = 0; i < size; i++)
        {
          if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id)) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          //stack or heap?
          vector < uint32_t > tmpVector;
          get_vint32(&data, &len, tmpVector);
          block_pairs_.insert(map<uint32_t, vector<uint32_t> >::value_type(block_id, tmpVector));
        }
      }
      // wblock_list_
      if (status_type_ & LB_INFOS)
      {
        if (get_int32(&data, &len, &size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        for (i = 0; i < size; i++)
        {
          BlockInfo* block_info = NULL;
          if (get_object(&data, &len, reinterpret_cast<void**> (&block_info), BLOCKINFO_SIZE) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          block_infos_.insert(map<uint32_t, BlockInfo*>::value_type(block_info->block_id_, block_info));
        }
      }

      return TFS_SUCCESS;

    }

    int32_t RespListBlockMessage::message_length()
    {
      int32_t len = INT_SIZE;

      // m_Blocks
      if (status_type_ & LB_BLOCK)
      {
        len += get_vint_len(blocks_);
      }

      // m_BlockPairs
      if (status_type_ & LB_PAIRS)
      {
        len += INT_SIZE;
        map<uint32_t, vector<uint32_t> >::iterator mit = block_pairs_.begin();
        for (; mit != block_pairs_.end(); mit++)
        {
          len += INT_SIZE;
          len += get_vint_len(mit->second);
        }
      }

      // m_BlockInfos
      if (status_type_ & LB_INFOS)
      {
        len += INT_SIZE;
        len += block_infos_.size() * BLOCKINFO_SIZE;
      }

      return len;
    }

    int RespListBlockMessage::build(char* data, int32_t len)
    {
      int32_t size;
      if (set_int32(&data, &len, status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (status_type_ & LB_BLOCK)
      {
        if (set_vint32(&data, &len, blocks_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      if (status_type_ & LB_PAIRS)
      {
        map<uint32_t, vector<uint32_t> >::iterator mit;
        size = block_pairs_.size();
        if (set_int32(&data, &len, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        for (mit = block_pairs_.begin(); mit != block_pairs_.end(); mit++)
        {
          if (set_int32(&data, &len, mit->first) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          if (set_vint32(&data, &len, mit->second) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
        }
      }

      if (status_type_ & LB_INFOS)
      {
        map<uint32_t, BlockInfo*>::iterator mit;
        size = block_infos_.size();
        if (set_int32(&data, &len, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        for (mit = block_infos_.begin(); mit != block_infos_.end(); mit++)
        {
          if (set_object(&data, &len, mit->second, BLOCKINFO_SIZE) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
        }
      }

      return TFS_SUCCESS;
    }

    char* RespListBlockMessage::get_name()
    {
      return "RespListBlockMessage";
    }

    Message* RespListBlockMessage::create(const int32_t type)
    {
      RespListBlockMessage* resp_lb_msg = new RespListBlockMessage();
      resp_lb_msg->set_message_type(type);
      return resp_lb_msg;
    }

    void RespListBlockMessage::add_block_id(const uint32_t block_id)
    {
      blocks_.push_back(block_id);
    }

    UpdateBlockInfoMessage::UpdateBlockInfoMessage() :
      block_id_(0), block_info_(NULL), server_id_(0), repair_(0), db_stat_(NULL)
    {
      _packetHeader._pcode = UPDATE_BLOCK_INFO_MESSAGE;
    }

    UpdateBlockInfoMessage::~UpdateBlockInfoMessage()
    {
    }

    int UpdateBlockInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&server_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t have_block = 0;
      if (get_int32(&data, &len, &have_block) == TFS_ERROR)
      {
        have_block = 0;
      }
      if (have_block)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (get_int32(&data, &len, &repair_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t have_sdbm = 0;
      if (get_int32(&data, &len, &have_sdbm) == TFS_ERROR)
      {
        have_sdbm = 0;
      }
      if (have_sdbm > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&db_stat_), sizeof(SdbmStat)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      return TFS_SUCCESS;
    }

    int32_t UpdateBlockInfoMessage::message_length()
    {
      int32_t len = INT64_SIZE + INT_SIZE * 4;
      if (block_info_ != NULL)
      {
        len += sizeof(BlockInfo);
      }
      if (db_stat_ != NULL)
      {
        len += sizeof(SdbmStat);
      }
      return len;
    }

    int UpdateBlockInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (set_int64(&data, &len, server_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      int32_t have_block = 0;
      if (block_info_ != NULL)
      {
        have_block = 1;
      }
      if (set_int32(&data, &len, have_block) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (block_info_ != NULL)
      {
        if (set_object(&data, &len, block_info_, BLOCKINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      if (set_int32(&data, &len, repair_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      // SdbmStat
      int32_t have_sdbm = 0;
      if (db_stat_ != NULL)
      {
        have_sdbm = 1;
      }
      if (set_int32(&data, &len, have_sdbm) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (db_stat_ != NULL)
      {
        if (set_object(&data, &len, db_stat_, sizeof(SdbmStat)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      return TFS_SUCCESS;
    }

    char* UpdateBlockInfoMessage::get_name()
    {
      return "updateblockinfomessage";
    }

    Message* UpdateBlockInfoMessage::create(const int32_t type)
    {
      UpdateBlockInfoMessage* req_ubi_msg = new UpdateBlockInfoMessage();
      req_ubi_msg->set_message_type(type);
      return req_ubi_msg;
    }

    ResetBlockVersionMessage::ResetBlockVersionMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = RESET_BLOCK_VERSION_MESSAGE;
    }

    ResetBlockVersionMessage::~ResetBlockVersionMessage()
    {
    }

    int ResetBlockVersionMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t ResetBlockVersionMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return len;
    }

    int ResetBlockVersionMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* ResetBlockVersionMessage::get_name()
    {
      return "resetblockversionmessage";
    }

    Message* ResetBlockVersionMessage::create(const int32_t type)
    {
      ResetBlockVersionMessage* req_rbv_msg = new ResetBlockVersionMessage();
      req_rbv_msg->set_message_type(type);
      return req_rbv_msg;
    }

    BlockFileInfoMessage::BlockFileInfoMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = BLOCK_FILE_INFO_MESSAGE;
      fileinfo_list_.clear();
    }

    BlockFileInfoMessage::~BlockFileInfoMessage()
    {
    }

    int BlockFileInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t size = 0;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      FileInfo* file_info;
      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&file_info), FILEINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        fileinfo_list_.push_back(file_info);
      }

      return TFS_SUCCESS;
    }

    int32_t BlockFileInfoMessage::message_length()
    {
      int32_t len = INT_SIZE * 2 + fileinfo_list_.size() * FILEINFO_SIZE;
      return len;
    }

    int BlockFileInfoMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t size = fileinfo_list_.size();
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        FileInfo* file_info = fileinfo_list_[i];
        if (set_object(&data, &len, reinterpret_cast<void*> (file_info), FILEINFO_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* BlockFileInfoMessage::get_name()
    {
      return "blockfileinfomessage";
    }

    Message* BlockFileInfoMessage::create(const int32_t type)
    {
      BlockFileInfoMessage* req_bfi_msg = new BlockFileInfoMessage();
      req_bfi_msg->set_message_type(type);
      return req_bfi_msg;
    }

    BlockRawMetaMessage::BlockRawMetaMessage() :
      block_id_(0)
    {
      _packetHeader._pcode = BLOCK_RAW_META_MESSAGE;
      raw_meta_list_.clear();
    }

    BlockRawMetaMessage::~BlockRawMetaMessage()
    {
    }

    int BlockRawMetaMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t size = 0;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      RawMeta* raw_meta;
      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&raw_meta), RAW_META_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        raw_meta_list_.push_back(*raw_meta);
      }

      return TFS_SUCCESS;
    }

    int32_t BlockRawMetaMessage::message_length()
    {
      int32_t len = INT_SIZE * 2 + raw_meta_list_.size() * RAW_META_SIZE;
      return len;
    }

    int BlockRawMetaMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t size = raw_meta_list_.size();
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      int32_t i = 0;
      for (i = 0; i < size; i++)
      {
        RawMeta raw_meta = raw_meta_list_[i];
        if (set_object(&data, &len, reinterpret_cast<void*> (&raw_meta), RAW_META_SIZE) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      return TFS_SUCCESS;
    }

    char* BlockRawMetaMessage::get_name()
    {
      return "blockrawmetamessage";
    }

    Message* BlockRawMetaMessage::create(const int32_t type)
    {
      BlockRawMetaMessage* req_brm_msg = new BlockRawMetaMessage();
      req_brm_msg->set_message_type(type);
      return req_brm_msg;
    }

    BlockWriteCompleteMessage::BlockWriteCompleteMessage() :
      block_info_(NULL), server_id_(0), write_complete_status_(WRITE_COMPLETE_STATUS_NO), unlink_flag_(UNLINK_FLAG_NO)
    {
      _packetHeader._pcode = BLOCK_WRITE_COMPLETE_MESSAGE;
    }

    BlockWriteCompleteMessage::~BlockWriteCompleteMessage()
    {
    }

    int BlockWriteCompleteMessage::parse(char* data, int32_t len)
    {
      if (get_int64(&data, &len, reinterpret_cast<int64_t*> (&server_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_object(&data, &len, reinterpret_cast<void**> (&block_info_), BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&lease_id_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      } if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&write_complete_status_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&unlink_flag_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t BlockWriteCompleteMessage::message_length()
    {
      int32_t len = INT64_SIZE + BLOCKINFO_SIZE + INT_SIZE * 3;
      return len;
    }

    int BlockWriteCompleteMessage::build(char* data, int32_t len)
    {
      if (block_info_ == NULL)
      {
        return TFS_ERROR;
      }

      if (set_int64(&data, &len, server_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_object(&data, &len, block_info_, BLOCKINFO_SIZE) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, lease_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, write_complete_status_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, unlink_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    char* BlockWriteCompleteMessage::get_name()
    {
      return "blockwritecompletemessage";
    }

    Message* BlockWriteCompleteMessage::create(const int32_t type)
    {
      BlockWriteCompleteMessage* req_bwc_msg = new BlockWriteCompleteMessage();
      req_bwc_msg->set_message_type(type);
      return req_bwc_msg;
    }

    ListBitMapMessage::ListBitMapMessage() :
      type_(0)
    {
      _packetHeader._pcode = LIST_BITMAP_MESSAGE;

    }

    ListBitMapMessage::~ListBitMapMessage()
    {
    }

    int ListBitMapMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t ListBitMapMessage::message_length()
    {
      int32_t len = INT_SIZE;
      return len;
    }

    int ListBitMapMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* ListBitMapMessage::get_name()
    {
      return "ListBitMapMessage";
    }

    Message* ListBitMapMessage::create(const int32_t type)
    {
      ListBitMapMessage* req_lbm_msg = new ListBitMapMessage();
      req_lbm_msg->set_message_type(type);
      return req_lbm_msg;
    }

    RespListBitMapMessage::RespListBitMapMessage() :
      ubitmap_len_(0), uuse_len_(0), data_(NULL), alloc_(false)
    {
      _packetHeader._pcode = RESP_LIST_BITMAP_MESSAGE;
    }

    RespListBitMapMessage::~RespListBitMapMessage()
    {
      if ((data_ != NULL) && (alloc_ == true))
      {
        ::free(data_);
        data_ = NULL;
      }
    }

    char* RespListBitMapMessage::alloc_data(int32_t len)
    {
      if (len <= 0)
      {
        return NULL;
      }
      if (data_ != NULL)
      {
        ::free(data_);
        data_ = NULL;
      }
      ubitmap_len_ = len;
      data_ = (char*) malloc(len);
      alloc_ = true;
      return data_;
    }

    int RespListBitMapMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&uuse_len_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&ubitmap_len_)) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (ubitmap_len_ > 0)
      {
        if (get_object(&data, &len, reinterpret_cast<void**> (&data_), ubitmap_len_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int32_t RespListBitMapMessage::message_length()
    {
      int32_t len = 2 * INT_SIZE;
      if (ubitmap_len_ > 0)
      {
        len += ubitmap_len_;
      }
      return len;
    }

    int RespListBitMapMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, uuse_len_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (set_int32(&data, &len, ubitmap_len_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (ubitmap_len_ > 0)
      {
        if (set_object(&data, &len, data_, ubitmap_len_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    char* RespListBitMapMessage::get_name()
    {
      return "resplistbitmapmessage";
    }

    Message* RespListBitMapMessage::create(const int32_t type)
    {
      RespListBitMapMessage* resp_lbm_msg = new RespListBitMapMessage();
      resp_lbm_msg->set_message_type(type);
      return resp_lbm_msg;
    }
  }
}
