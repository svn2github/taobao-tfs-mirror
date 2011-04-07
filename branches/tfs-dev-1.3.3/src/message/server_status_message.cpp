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
#include "server_status_message.h"

using namespace tfs::common;
using namespace tfs::nameserver;
using namespace std;
using namespace __gnu_cxx;

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

    void ReplicateInfoMessage::set_replicating_map(const hash_map<uint32_t, ReplBlock*>& src)
    {
      hash_map<uint32_t, ReplBlock*>::const_iterator it = src.begin();
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

    ShowServerInformationMessage::ShowServerInformationMessage()
    {
      _packetHeader._pcode = SHOW_SERVER_INFORMATION_MESSAGE;
      memset(&param, 0, sizeof(param));
    }

    ShowServerInformationMessage::~ShowServerInformationMessage()
    {

    }
    int ShowServerInformationMessage::parse(char* data, int32_t len)
    {
      return param.deserialize(data, len); 
    }

    int ShowServerInformationMessage::build(char* data, int32_t len)
    {
      return param.serialize(data, len);
    }
    int32_t ShowServerInformationMessage::message_length()
    {
      return param.get_serialize_size();
    }

    char* ShowServerInformationMessage::get_name()
    {
      return "showserverinformationmessage";
    }

    Message* ShowServerInformationMessage::create(const int32_t type)
    {
      Message* msg = new ShowServerInformationMessage();
      msg->set_message_type(type);
      return msg;
    }

  }
}
