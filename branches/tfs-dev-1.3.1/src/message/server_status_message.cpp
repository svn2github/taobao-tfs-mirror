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
#include "common/interval.h"
#include "server_status_message.h"

using namespace tfs::common;
using namespace tfs::nameserver;

namespace tfs
{
  namespace message
  {
    GetServerStatusMessage::GetServerStatusMessage() :
      from_row_(0), return_row_(1000)
    {
      _packetHeader._pcode = GET_SERVER_STATUS_MESSAGE;
    }

    GetServerStatusMessage::~GetServerStatusMessage()
    {
    }

    int GetServerStatusMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &from_row_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &return_row_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int32_t GetServerStatusMessage::message_length()
    {
      int32_t len = INT_SIZE * 3;
      return len;
    }

    int GetServerStatusMessage::build(char* data, int32_t len)
    {
      if (set_int32(&data, &len, status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, from_row_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, return_row_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    char* GetServerStatusMessage::get_name()
    {
      return "getserverstatusmessage";
    }

    Message* GetServerStatusMessage::create(const int32_t type)
    {
      GetServerStatusMessage* req_gss_msg = new GetServerStatusMessage();
      req_gss_msg->set_message_type(type);
      return req_gss_msg;
    }

    SetServerStatusMessage::SetServerStatusMessage() :
			Message(),
      status_type_(0), from_row_(0), return_row_(1000), has_next_(0), need_free_(0), block_server_map_ptr_(NULL)
    {
      _packetHeader._pcode = SET_SERVER_STATUS_MESSAGE;
    }

    SetServerStatusMessage::~SetServerStatusMessage()
    {
      if (need_free_ == 1)
      {
        for (BLOCK_MAP_ITER it = block_map_.begin(); it != block_map_.end(); it++)
        {
          delete it->second;
          it->second = NULL;
        }
        for (SERVER_MAP_ITER itx = server_map_.begin(); itx != server_map_.end(); itx++)
        {
          delete itx->second;
          itx->second = NULL;
        }
      }
    }

    int SetServerStatusMessage::get_int_map(char** data, int32_t* len, std::set<uint32_t>* map)
    {
      int32_t i = 0;
      int32_t size = 0;
      uint32_t id;

      if (get_int32(data, len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      for (i = 0; i < size; i++)
      {
        if (get_int32(data, len, reinterpret_cast<int*> (&id)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        map->insert(id);
      }
      return TFS_SUCCESS;
    }

    int SetServerStatusMessage::set_int_map(char** data, int32_t* len, std::set<uint32_t>* map, int32_t size)
    {
      if (set_int32(data, len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      std::set<uint32_t>::iterator it = map->begin();
      for ( ;it != map->end(); it++)
      {
        if (set_int32(data, len, *it) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        size--;
        if (size <= 0)
        {
          break;
        }
      }
      return TFS_SUCCESS;
    }

    int SetServerStatusMessage::parse(char* data, int32_t len)
    {
      int32_t i = 0;
      int32_t size = 0;
      if (get_int32(&data, &len, &status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      need_free_ = 1;

      if ((status_type_ > 0) && (status_type_& SSM_BLOCK))
      {
        if (get_int32(&data, &len, &has_next_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        BlockInfo* block_info;
        if (get_int32(&data, &len, &size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        for (i = 0; i < size; i++)
        {
          BlockCollect* block_collect = new BlockCollect();
          block_info = const_cast<BlockInfo*>(block_collect->get_block_info());
          if (get_object_copy(&data, &len, reinterpret_cast<void*> (block_info), BLOCKINFO_SIZE) == TFS_ERROR)
          {
            delete block_collect;
            return TFS_ERROR;
          }
          if (get_vint64(&data, &len, const_cast<VUINT64&>(*block_collect->get_ds())) == TFS_ERROR)
          {
            delete block_collect;
            return TFS_ERROR;
          }
          block_map_.insert(BLOCK_MAP::value_type(block_info->block_id_, block_collect));
        }
      }

      if ((status_type_ > 0) && (status_type_ & SSM_SERVER))
      {
        DataServerStatInfo* ds;
        if (get_int32(&data, &len, &size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        for (i = 0; i < size; i++)
        {
          ServerCollect* server_collect = new ServerCollect();
          ds = server_collect->get_ds();
          if (get_object_copy(&data, &len, reinterpret_cast<void*>(ds), sizeof(DataServerStatInfo)) == TFS_ERROR)
          {
            delete server_collect;
            return TFS_ERROR;
          }
          server_map_.insert(SERVER_MAP::value_type(ds->id_, server_collect));
          get_int_map(&data, &len, const_cast<std::set<uint32_t>*> (server_collect->get_writable_block_list()));
          get_int_map(&data, &len, const_cast<std::set<uint32_t>*>(server_collect->get_primary_writable_block_list()));
        }
      }
      if ((status_type_ > 0) && (status_type_ & SSM_WBLIST))
      {
        if (get_vint32(&data, &len, wblock_list_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      return TFS_SUCCESS;
    }

    int32_t SetServerStatusMessage::message_length()
    {
      int32_t len = INT_SIZE;

      if ((status_type_ > 0) && (status_type_ & SSM_BLOCK))
      {
        len += INT_SIZE; // hasNext
        len += INT_SIZE;
        int32_t count = 0;
        int32_t i = 0;
        int32_t chunk_num = block_server_map_ptr_->get_block_chunk_num();
        const BlockChunkPtr* array = block_server_map_ptr_->get_block_chunk_array();
        for (i = 0; i < chunk_num; ++i)
        {
          const BLOCK_MAP & current = array[i]->get_block_map();
          BLOCK_MAP::const_iterator it = current.begin();
          for (; it != current.end(); ++it)
          {
            count++;
            if (count <= from_row_)
              continue;
            if (count > from_row_ + return_row_)
              break;
            len += BLOCKINFO_SIZE;
            len += get_vint64_len(const_cast<VUINT64&>(*(it->second->get_ds())));
          }
        }
      }
      if ((status_type_ > 0) && (status_type_ & SSM_SERVER))
      {
        len += INT_SIZE;
        SERVER_MAP_ITER it = server_map_.begin();
        for (; it != server_map_.end(); it++)
        {
          ServerCollect* server_collect = it->second;
          if (!server_collect->is_alive())
          {
            continue;
          }
          len += sizeof(DataServerStatInfo);
          int32_t wsize = server_collect->get_writable_block_list()->size();
          if (wsize > MAX_WBLOCK)
          {
            wsize = MAX_WBLOCK;
          }
          len += INT_SIZE + INT_SIZE * wsize;
          len += INT_SIZE + INT_SIZE * server_collect->get_primary_writable_block_list()->size();
        }
      }
      // wblock_list_
      if ((status_type_ > 0) && (status_type_ & SSM_WBLIST))
      {
        len += get_vint_len(wblock_list_);
      }

      return len;
    }

    int SetServerStatusMessage::build(char* data, int32_t len)
    {
      int32_t size = 0;
      if (set_int32(&data, &len, status_type_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
			if ((status_type_ > 0)
				&& (status_type_ & SSM_BLOCK)
				&& (block_server_map_ptr_ != NULL))
      {
        size = block_server_map_ptr_->cacl_all_block_count();
        size -= from_row_;
        if (size < 0)
        {
          size = 0;
        }
        if (size > return_row_)
        {
          has_next_ = 1;
          size = return_row_;
        }
        if (set_int32(&data, &len, has_next_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (set_int32(&data, &len, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }

        int32_t count = 0;
        int32_t i = 0;
        int32_t chunk_num = block_server_map_ptr_->get_block_chunk_num();
        const BlockChunkPtr* array = block_server_map_ptr_->get_block_chunk_array();

        for (i = 0; i < chunk_num; ++i)
        {
          const BLOCK_MAP & current = array[i]->get_block_map();
          BLOCK_MAP::const_iterator it = current.begin();
          for (; it != current.end(); ++it)
          {
            count++;
            if (count <= from_row_)
              continue;
            if (count > from_row_ + return_row_)
              break;
            if (set_object(&data, &len, const_cast<BlockInfo*>(it->second->get_block_info()), BLOCKINFO_SIZE) == TFS_ERROR)
            {
              return TFS_ERROR;
            }
            if (set_vint64(&data, &len, const_cast<VUINT64&>(*it->second->get_ds())) == TFS_ERROR)
            {
              return TFS_ERROR;
            }
          }
        }
      }

      if ((status_type_ > 0)
				&& (status_type_ & SSM_SERVER))
      {
        size = std::count_if(server_map_.begin(), server_map_.end(), AliveDataServer());
        if (set_int32(&data, &len, size) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        for (SERVER_MAP_ITER it = server_map_.begin(); it != server_map_.end(); it++)
        {
          ServerCollect* servcol = it->second;
          if (!servcol->is_alive())
            continue;
          if (set_object(&data, &len, servcol->get_ds(), sizeof(DataServerStatInfo)) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          int32_t wsize = servcol->get_writable_block_list()->size();
          if (wsize > MAX_WBLOCK)
          {
            wsize = MAX_WBLOCK;
          }
          if (set_int_map(&data, &len, const_cast<std::set<uint32_t>*>(servcol->get_writable_block_list()), wsize) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          if (set_int_map(&data, &len, const_cast<std::set<uint32_t>*>(servcol->get_primary_writable_block_list()), servcol->get_primary_writable_block_list()->size()) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
        }
      }
      if ((status_type_ > 0)
				&& (status_type_ & SSM_WBLIST))
      {
        if (set_vint32(&data, &len, wblock_list_) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }

      return TFS_SUCCESS;
    }

    char* SetServerStatusMessage::get_name()
    {
      return "setserverstatusmessage";
    }

    Message* SetServerStatusMessage::create(const int32_t type)
    {
      SetServerStatusMessage* req_sss_msg = new SetServerStatusMessage();
      req_sss_msg->set_message_type(type);
      return req_sss_msg;
    }

    ReplicateInfoMessage::ReplicateInfoMessage()
    {
      _packetHeader._pcode = REPLICATE_INFO_MESSAGE;
    }

    ReplicateInfoMessage::~ReplicateInfoMessage()
    {
    }

    char* ReplicateInfoMessage::get_name()
    {
      return "replicateinfomessage";
    }

    Message* ReplicateInfoMessage::create(const int32_t type)
    {
      ReplicateInfoMessage* req_ri_msg = new ReplicateInfoMessage();
      req_ri_msg->set_message_type(type);
      return req_ri_msg;
    }

    int ReplicateInfoMessage::build(char* data, int32_t len)
    {
      int32_t size = replicating_map_.size();
      if (set_int32(&data, &len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      for (REPL_BLOCK_MAP::const_iterator it = replicating_map_.begin(); it != replicating_map_.end(); ++it)
      {
        if (set_int32(&data, &len, static_cast<int32_t> (it->first)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (set_object(&data, &len, reinterpret_cast<void*> (const_cast<ReplBlock*>(&it->second)), sizeof(ReplBlock)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      if (set_counter_map(&data, &len, source_ds_counter_, source_ds_counter_.size()))
      {
        return TFS_ERROR;
      }
      if (set_counter_map(&data, &len, dest_ds_counter_, dest_ds_counter_.size()))
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int ReplicateInfoMessage::parse(char* data, int32_t len)
    {
      int32_t size = 0;
      if (get_int32(&data, &len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      ReplBlock item;
      uint32_t block_id = 0;
      int32_t i = 0;
      for (i = 0; i < size; ++i)
      {
        if (get_int32(&data, &len, reinterpret_cast<int32_t*> (&block_id)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (get_object_copy(&data, &len, reinterpret_cast<void*> (&item), sizeof(ReplBlock)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        replicating_map_.insert(REPL_BLOCK_MAP::value_type(block_id, item));
      }
      if (get_counter_map(&data, &len, source_ds_counter_))
      {
        return TFS_ERROR;
      }
      if (get_counter_map(&data, &len, dest_ds_counter_))
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int ReplicateInfoMessage::set_counter_map(char** data, int32_t* len, const COUNTER_TYPE & map, int32_t size)
    {

      if (set_int32(data, len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      for (COUNTER_TYPE::const_iterator it = map.begin(); it != map.end(); ++it)
      {
        if (set_int64(data, len, static_cast<int64_t> (it->first)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (set_int32(data, len, static_cast<int32_t> (it->second)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        size--;
        if (size <= 0)
        {
          break;
        }
      }
      return TFS_SUCCESS;
    }

    int ReplicateInfoMessage::get_counter_map(char** data, int32_t* len, COUNTER_TYPE & m)
    {
      int32_t i = 0;
      int32_t size = 0;
      uint64_t server_id = 0;
      uint32_t count = 0;

      if (get_int32(data, len, &size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      for (i = 0; i < size; ++i)
      {
        if (get_int64(data, len, reinterpret_cast<int64_t*> (&server_id)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (get_int32(data, len, reinterpret_cast<int32_t*> (&count)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        m.insert(COUNTER_TYPE::value_type(server_id, count));
      }
      return TFS_SUCCESS;
    }

    int32_t ReplicateInfoMessage::message_length()
    {
      int32_t length = INT_SIZE; // replicatingMap.size()
      length += replicating_map_.size() * (INT_SIZE + sizeof(ReplBlock));
      length += INT_SIZE;
      length += source_ds_counter_.size() * (INT_SIZE + INT64_SIZE);
      length += INT_SIZE;
      length += dest_ds_counter_.size() * (INT_SIZE + INT64_SIZE);
      return length;
    }

    void ReplicateInfoMessage::set_replicating_map(const __gnu_cxx::hash_map<uint32_t, ReplBlock*>& src)
    {
      __gnu_cxx::hash_map<uint32_t, ReplBlock*>::const_iterator it = src.begin();
      for (; it != src.end(); ++it)
      {
        replicating_map_.insert(REPL_BLOCK_MAP::value_type(it->first, *(it->second)));
      }
    }

    AccessStatInfoMessage::AccessStatInfoMessage() :
      from_row_(0), return_row_(0), has_next_(0)
    {
      _packetHeader._pcode = ACCESS_STAT_INFO_MESSAGE;
    }

    AccessStatInfoMessage::~AccessStatInfoMessage()
    {
    }

    char* AccessStatInfoMessage::get_name()
    {
      return "accessstatinfomessage";
    }

    Message* AccessStatInfoMessage::create(int32_t type)
    {
      AccessStatInfoMessage* req_asi_msg = new AccessStatInfoMessage();
      req_asi_msg->set_message_type(type);
      return req_asi_msg;
    }

    int AccessStatInfoMessage::build(char* data, int32_t len)
    {
      int32_t size = stats_.size();
      size -= from_row_;
      if (size < 0)
      {
        size = 0;
      }
      if (size > return_row_)
      {
        has_next_ = 1;
        size = return_row_;
      }
      if (set_int32(&data, &len, has_next_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_counter_map(&data, &len, stats_, from_row_, return_row_, size))
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int AccessStatInfoMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &has_next_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_counter_map(&data, &len, stats_))
      {
        return TFS_ERROR;
      }
      return TFS_SUCCESS;
    }

    int AccessStatInfoMessage::set_counter_map(char** data, int32_t* len, const COUNTER_TYPE & map, int32_t from_row,
        int32_t return_row, int32_t size)
    {
      if (set_int32(data, len, size) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (size <= 0)
      {
        return TFS_SUCCESS;
      }
      int count = 0;
      COUNTER_TYPE::const_iterator it = map.begin();
      for (; it != map.end(); ++it)
      {
        count++;
        if (count <= from_row)
        {
          continue;
        }
        if (count > from_row + return_row)
        {
          break;
        }
        if (set_int32(data, len, static_cast<int32_t> (it->first)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        if (set_object(data, len, reinterpret_cast<void*> (const_cast<Throughput*>(&it->second)), sizeof(Throughput)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    int AccessStatInfoMessage::get_counter_map(char** data, int32_t* len, COUNTER_TYPE & map)
    {
      int32_t i = 0;
      int32_t size = 0;
      uint32_t server_id = 0;
      Throughput count;

      if (get_int32(data, len, &size) == TFS_ERROR)
        return TFS_ERROR;
      for (i = 0; i < size; ++i)
      {
        if (get_int32(data, len, (int32_t*) &server_id) == TFS_ERROR)
          return TFS_ERROR;
        if (get_object_copy(data, len, reinterpret_cast<void*> (&count), sizeof(Throughput)) == TFS_ERROR)
        {
          return TFS_ERROR;
        }
        map.insert(COUNTER_TYPE::value_type(server_id, count));
      }
      return TFS_SUCCESS;
    }

    int32_t AccessStatInfoMessage::message_length()
    {
      int32_t length = 0;
      length += INT_SIZE; // has_next_
      length += INT_SIZE; // size of return row
      int32_t size = stats_.size();
      size -= from_row_;
      if (size < 0)
      {
        size = 0;
      }
      if (size > return_row_)
      {
        size = return_row_;
      }
      length += size * (INT_SIZE + sizeof(Throughput));
      return length;
    }

    GetBlockListMessage::GetBlockListMessage() :
      request_flag_(1), write_flag_(0), start_block_id_(0), start_inclusive_(0), end_block_id_(0), read_count_(0),
      return_count_(0), start_block_chunk_(0), next_block_chunk_(0)
    {
      _packetHeader._pcode = GET_BLOCK_LIST_MESSAGE;
    }

    GetBlockListMessage::~GetBlockListMessage()
    {
      BLOCK_MAP_ITER it = block_map_.begin();
      for (; it != block_map_.end(); it++)
      {
        delete it->second;
        it->second = NULL;
      }
    }

    int GetBlockListMessage::parse(char* data, int32_t len)
    {
      if (get_int32(&data, &len, &request_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &write_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_uint32(&data, &len, &start_block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &start_inclusive_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_uint32(&data, &len, &end_block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &read_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &return_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &start_block_chunk_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (get_int32(&data, &len, &next_block_chunk_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (request_flag_ > 0)
      {
        return TFS_SUCCESS;
      }

      int i = 0;
      for (i = 0; i < return_count_; i++)
      {
        BlockCollect* blcok_collect = new BlockCollect();
        BlockInfo* block_info = const_cast<BlockInfo*> (blcok_collect->get_block_info());
        if (get_object_copy(&data, &len, reinterpret_cast<void*> (block_info), BLOCKINFO_SIZE) == TFS_ERROR)
        {
          delete blcok_collect;
          return TFS_ERROR;
        }
        if (get_vint64(&data, &len, const_cast<VUINT64&>(*blcok_collect->get_ds())) == TFS_ERROR)
        {
          delete blcok_collect;
          return TFS_ERROR;
        }
        block_map_.insert(BLOCK_MAP::value_type(block_info->block_id_, blcok_collect));
      }
      return TFS_SUCCESS;
    }

    int GetBlockListMessage::build(char* data, int32_t len)
    {
      char* ptr = data;
      int32_t olen = len;

      if (set_int32(&data, &len, request_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, write_flag_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, start_block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, start_inclusive_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, end_block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, read_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, return_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, start_block_chunk_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&data, &len, next_block_chunk_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      if (request_flag_ > 0)
      {
        return TFS_SUCCESS;
      }
      if (!block_server_map_ptr_)
      {
        return TFS_ERROR;
      }

      int32_t chunk_num = block_server_map_ptr_->get_block_chunk_num();
      const BlockChunkPtr* array = block_server_map_ptr_->get_block_chunk_array();
      if (start_block_chunk_ >= chunk_num)
      {
        return TFS_ERROR;
      }
      int32_t index = start_block_chunk_;
      return_count_ = 0;
      bool chunk_done = false;
      while (index < chunk_num)
      {
        const BLOCK_MAP & current = array[index]->get_block_map();
        BLOCK_MAP::const_iterator it = current.end();
        if (start_block_id_ == 0)
        {
          it = current.begin();
        }
        else
        {
          it = current.find(start_block_id_);
        }
        if ((start_block_id_ != 0) && (!start_inclusive_) && (it != current.end()))
        {
          ++it;
        }

        while (it != current.end())
        {
          if (write_flag_ && (it->second->is_full()))
          {
            ++it;
            continue;
          }
          chunk_done = false;
          if (set_object(&data, &len, reinterpret_cast<void*>(const_cast<BlockInfo*>(it->second->get_block_info())), BLOCKINFO_SIZE) == TFS_ERROR)
          {
            return TFS_ERROR;
          }
          if (set_vint64(&data, &len, const_cast<VUINT64&>(*it->second->get_ds())) == TFS_ERROR)
          {
            return TFS_ERROR;
          }

          end_block_id_ = it->second->get_block_info()->block_id_;

          ++it;
          ++return_count_;
          if (return_count_ >= read_count_)
          {
            break;
          }
        }

        if (it == current.end())
        {
          chunk_done = true;
        }
        if (return_count_ >= read_count_)
        {
          break;
        }

        ++index;
        // reset start_block_id_ for next round.
        start_block_id_ = 0;
      }

      if ((index >= chunk_num - 1) && (chunk_done == true))
      {
        next_block_chunk_ = -1;
      }
      else
      {
        next_block_chunk_ = index;
      }

      // reset return_count_ and next_block_chunk_
      char* ebptr = ptr + 4 * INT_SIZE;
      char* rcptr = ptr + 6 * INT_SIZE;
      char* nbptr = ptr + 8 * INT_SIZE;
      if (set_int32(&ebptr, &olen, end_block_id_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&rcptr, &olen, return_count_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }
      if (set_int32(&nbptr, &olen, next_block_chunk_) == TFS_ERROR)
      {
        return TFS_ERROR;
      }

      return TFS_SUCCESS;
    }

    int32_t GetBlockListMessage::message_length()
    {
      int32_t len = 9 * sizeof(int32_t);
      if (!request_flag_)
      {
        len += read_count_ * (BLOCKINFO_SIZE + 2 * INT64_SIZE + INT_SIZE);
      }
      return len;
    }

    char* GetBlockListMessage::get_name()
    {
      return "blocklistmessage";
    }

    Message* GetBlockListMessage::create(const int32_t type)
    {
      Message* msg = new GetBlockListMessage();
      msg->set_message_type(type);
      return msg;
    }
  }
}
