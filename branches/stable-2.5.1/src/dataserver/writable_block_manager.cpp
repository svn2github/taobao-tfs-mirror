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

#include "dataservice.h"
#include "writable_block_manager.h"

using namespace std;
using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    WritableBlockManager::WritableBlockManager(DataService& service):
      service_(service),
      writable_(MAX_WRITABLE_BLOCK_COUNT, 1024, 0.1),
      write_index_(0)
    {
    }

    WritableBlockManager::~WritableBlockManager()
    {
      BLOCK_TABLE::iterator iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        tbsys::gDelete(*iter);
      }
      writable_.clear();
    }

    BlockManager& WritableBlockManager::get_block_manager()
    {
      return service_.get_block_manager();
    }

    void WritableBlockManager::expire_one_block(const uint64_t block_id)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      WritableBlock* block = get_(block_id);
      if (NULL != block)
      {
        expire_one_block_(block);
      }
    }

    void WritableBlockManager::expire_update_blocks()
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      BLOCK_TABLE::iterator iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        if ((*iter)->get_type() == BLOCK_UPDATE)
        {
          expire_one_block_(*iter);
          TBSYS_LOG(INFO, "expire block %"PRI64_PREFIX"u because update",
              (*iter)->get_block_id());
        }
      }
    }

    void WritableBlockManager::expire_all_blocks()
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      BLOCK_TABLE::iterator iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        expire_one_block_(*iter);
      }
    }

    bool WritableBlockManager::is_full(const uint64_t block_id)
    {
      int32_t block_size = 0;
      get_block_manager().get_used_offset(block_size, block_id);
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      return BLOCK_RESERVER_LENGTH + block_size >= ds_info.max_block_size_;
    }

    void  WritableBlockManager::size(int32_t& writable, int32_t& update, int32_t& expired)
    {
      writable = 0;
      update = 0;
      expired = 0;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      BLOCK_TABLE_ITER iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        if (BLOCK_WRITABLE == (*iter)->get_type())
        {
          writable++;
        }
        else if (BLOCK_UPDATE == (*iter)->get_type())
        {
          update++;
        }
        else if (BLOCK_EXPIRED == (*iter)->get_type())
        {
          expired++;
        }
      }
    }

    WritableBlock* WritableBlockManager::insert(const uint64_t block_id,
        const ArrayHelper<uint64_t>& servers, const BlockType type)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return insert_(block_id, servers, type);
    }

    void WritableBlockManager::remove(const uint64_t block_id, const BlockType type)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(block_id, type);
    }

    WritableBlock* WritableBlockManager::get(const uint64_t block_id)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return get_(block_id);
    }

    void WritableBlockManager::expire_one_block_(WritableBlock* block)
    {
      if (NULL != block)
      {
        block->set_type(BLOCK_EXPIRED);
      }
    }

    WritableBlock* WritableBlockManager::insert_(const uint64_t block_id,
        const ArrayHelper<uint64_t>& servers, const BlockType type)
    {
      WritableBlock* result = NULL;
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WritableBlock* block = new (std::nothrow) WritableBlock(block_id);
        assert(NULL != block);
        ret = writable_.insert_unique(result, block);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(block);
          if (EXIT_ELEMENT_EXIST == ret)
          {
            assert(NULL != result);
            result->update_last_time(Func::get_monotonic_time());
          }
        }

        if (NULL != result)
        {
          result->set_servers(servers);
          result->set_type(type);
        }
      }
      return result;
    }

    void WritableBlockManager::remove_(const uint64_t block_id, const BlockType type)
    {
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WritableBlock query(block_id);
        BLOCK_TABLE_ITER iter = writable_.find(&query);
        if (iter != writable_.end())
        {
          if ((*iter)->get_type() == type) // remove when type matched
          {
            WritableBlock* result = writable_.erase(&query);
            if (NULL != result)
            {
#ifndef TFS_GTEST
              get_block_manager().get_gc_manager().add(result);
#endif

            }
          }
        }
      }
    }

    WritableBlock* WritableBlockManager::get_(const uint64_t block_id)
    {
      WritableBlock* result = NULL;
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WritableBlock query(block_id);
        BLOCK_TABLE_ITER iter = writable_.find(&query);
        if (iter != writable_.end())
        {
          result = *iter;
          assert(NULL != result);
        }
      }
      return result;
    }

    int WritableBlockManager::alloc_writable_block(WritableBlock*& block)
    {
      rwmutex_.rdlock();
      int retry = 0;
      int count = writable_.size();
      VUINT64 expires;
      block = NULL;
      while ((NULL == block) && (retry++ < count))
      {
        int index = write_index_++;
        if (index >= count)
        {
          index = 0;
        }
        if (write_index_ >= count)
        {
          write_index_ = 0;
        }
        WritableBlock* target = writable_.at(index);
        assert(NULL != target);
        if (BLOCK_WRITABLE == target->get_type())
        {
          if (is_full(target->get_block_id()))
          {
            expires.push_back(target->get_block_id());
          }
          else if (!target->get_use_flag())
          {
            target->set_use_flag(true);
            block = target;
          }
        }
      }
      rwmutex_.unlock();

      // expire full blocks
      if (expires.size() > 0)
      {
        VUINT64::iterator iter = expires.begin();
        for ( ;iter != expires.end(); iter++)
        {
          expire_one_block(*iter);
          TBSYS_LOG(INFO, "expire block %"PRI64_PREFIX"u because block full", *iter);
        }
      }

      if (NULL == block)
      {
        TBSYS_LOG(WARN, "alloc writable block failed.");
      }
      else
      {
        TBSYS_LOG(DEBUG, "alloc writable block %"PRI64_PREFIX"u",
            block->get_block_id());
      }

      return (NULL != block) ? TFS_SUCCESS : EXIT_NO_WRITABLE_BLOCK;
    }

    int WritableBlockManager::alloc_update_block(const uint64_t block_id, WritableBlock*& block)
    {
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      block = NULL;
      WritableBlock* target = NULL;

      // check if block exists
      if (TFS_SUCCESS == ret)
      {
        BlockInfoV2 info;
        ret = get_block_manager().get_block_info(info, block_id);
      }

      if (TFS_SUCCESS == ret)
      {
        rwmutex_.rdlock();
        target = get_(block_id);
        ret = NULL == target ? EXIT_NO_WRITABLE_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          ret = target->get_use_flag() ? EXIT_BLOCK_HAS_WRITE : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ret = target->get_type() == BLOCK_EXPIRED ? EXIT_NO_WRITABLE_BLOCK: TFS_SUCCESS;
          }
        }
        rwmutex_.unlock();
      }

      // apply from ns
      if (EXIT_NO_WRITABLE_BLOCK == ret)
      {
        ret = apply_update_block(block_id);
        if (TFS_SUCCESS == ret)
        {
          target = get(block_id);
          assert(NULL != target);
          ret = target->get_use_flag() ? EXIT_BLOCK_HAS_WRITE: TFS_SUCCESS;
        }
        else if (EXIT_LEASE_EXISTED == ret)
        {
          // another thread may already applyed this block
          target = get(block_id);
          if (NULL != target)
          {
            ret = target->get_use_flag() ? EXIT_BLOCK_HAS_WRITE: TFS_SUCCESS;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        block = target;
        target->set_use_flag(true);
      }

      TBSYS_LOG_DW(ret, "alloc update block, ret: %d", ret);

      return ret;
    }

    void WritableBlockManager::free_writable_block(const uint64_t block_id)
    {
      if (INVALID_BLOCK_ID != block_id)
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        WritableBlock* block = get_(block_id);
        if (NULL != block)
        {
          block->set_use_flag(false);
        }
        TBSYS_LOG(DEBUG, "free writable block %"PRI64_PREFIX"u", block_id);
      }
    }

    // asynchronized request to nameserver
    int WritableBlockManager::apply_writable_block(const int32_t count)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsApplyBlockMessage req_msg;
      req_msg.set_count(count);
      req_msg.set_server_id(ds_info.information_.id_);
      ret = post_msg_to_server(ds_info.ns_vip_port_,&req_msg, ds_async_callback);
      return ret;
    }

    // synchronized request to nameserver
    int WritableBlockManager::apply_update_block(const int64_t block_id)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsApplyBlockForUpdateMessage req_msg;
      req_msg.set_block_id(block_id);
      req_msg.set_server_id(ds_info.information_.id_);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (DS_APPLY_BLOCK_FOR_UPDATE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            DsApplyBlockForUpdateResponseMessage* resp_msg =
              dynamic_cast<DsApplyBlockForUpdateResponseMessage* >(ret_msg);
            ret = process_apply_update_block(resp_msg);
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = resp_msg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }

      TBSYS_LOG_DW(ret, "apply update block %"PRI64_PREFIX"u, ret: %d",
          block_id, ret);

      return ret;
    }

    // asynchronized request to nameserver
    int WritableBlockManager::giveup_writable_block()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsGiveupBlockMessage req_msg;
      req_msg.set_server_id(ds_info.information_.id_);
      ArrayHelper<BlockInfoV2> blocks(MAX_WRITABLE_BLOCK_COUNT, req_msg.get_block_infos());
      get_blocks(blocks, BLOCK_EXPIRED);
      req_msg.set_size(blocks.get_array_index());

      ret = post_msg_to_server(ds_info.ns_vip_port_, &req_msg, ds_async_callback);
      return ret;
    }

    void WritableBlockManager::get_blocks(common::ArrayHelper<BlockInfoV2>& blocks,
        const BlockType type)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int ret = TFS_SUCCESS;
      BLOCK_TABLE_ITER iter = writable_.begin();
      for ( ; iter != writable_.end() &&
          blocks.get_array_index() < blocks.get_array_size(); iter++)
      {
        if (type == (*iter)->get_type())
        {
          uint64_t block_id = (*iter)->get_block_id();
          BlockInfoV2 block_info;
          ret = get_block_manager().get_block_info(block_info, block_id);
          if (TFS_SUCCESS == ret)
          {
            blocks.push_back(block_info);
          }
        }
      }
    }

    int WritableBlockManager::callback(NewClient* client)
    {
      int ret = TFS_SUCCESS;
      NewClient::RESPONSE_MSG_MAP* sresponse = NULL;
      NewClient::RESPONSE_MSG_MAP* fresponse = NULL;
      tbnet::Packet* source = NULL;
      int32_t pcode = 0;
      if (TFS_SUCCESS == ret)
      {
        sresponse = client->get_success_response();
        fresponse = client->get_fail_response();
        if ((NULL == sresponse) || (NULL == fresponse))
        {
          ret = EXIT_POINTER_NULL;
        }
        else
        {
          source = client->get_source_msg();
          ret = (NULL == source) ? EXIT_POINTER_NULL: TFS_SUCCESS;
          if( TFS_SUCCESS == ret)
          {
            pcode = source->getPCode();
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (DS_APPLY_BLOCK_MESSAGE == pcode && sresponse->size() > 0)
        {
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          tbnet::Packet* packet = iter->second.second;
          if (DS_APPLY_BLOCK_RESPONSE_MESSAGE == packet->getPCode())
          {
            apply_block_callback(dynamic_cast<DsApplyBlockResponseMessage* >(packet));
          }
        }
        else if (DS_GIVEUP_BLOCK_MESSAGE == pcode && sresponse->size() > 0)
        {
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          tbnet::Packet* packet = iter->second.second;
          if (DS_GIVEUP_BLOCK_RESPONSE_MESSAGE == packet->getPCode())
          {
            giveup_block_callback(dynamic_cast<DsGiveupBlockResponseMessage* >(packet));
          }
        }
      }

      return TFS_SUCCESS;
    }

    int WritableBlockManager::process_apply_update_block(message::DsApplyBlockForUpdateResponseMessage* response)
    {
      BlockLease& block_lease = response->get_block_lease();
      if (TFS_SUCCESS == block_lease.result_)
      {
        assert(block_lease.size_ > 0);
        ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM,
            block_lease.servers_, block_lease.size_);
        insert(block_lease.block_id_, servers, BLOCK_UPDATE);
      }
      return block_lease.result_;
    }

    void WritableBlockManager::apply_block_callback(DsApplyBlockResponseMessage* response)
    {
      assert(NULL != response);
      BlockLease* block_lease = response->get_block_lease();
      int32_t size = response->get_size();
      for (int index = 0; index < size; index++)
      {
        if (TFS_SUCCESS == block_lease[index].result_)
        {
          assert(block_lease[index].size_ > 0);
          ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM,
              block_lease[index].servers_, block_lease[index].size_);
          insert(block_lease[index].block_id_,
              servers, BLOCK_WRITABLE);
        }
        TBSYS_LOG(DEBUG, "apply writable block %"PRI64_PREFIX"u replica: %d, ret %d",
            block_lease[index].block_id_, block_lease[index].size_, block_lease[index].result_);
      }
    }

    void WritableBlockManager::giveup_block_callback(DsGiveupBlockResponseMessage* response)
    {
      assert(NULL != response);
      BlockLease* block_lease = response->get_block_lease();
      int32_t size = response->get_size();
      for (int index = 0; index < size; index++)
      {
        uint64_t block_id = block_lease[index].block_id_;
        remove(block_id, BLOCK_EXPIRED);
        TBSYS_LOG(DEBUG, "giveup writable block %"PRI64_PREFIX"u ret %d",
            block_lease[index].block_id_, block_lease[index].result_);
      }
    }
  }
}

