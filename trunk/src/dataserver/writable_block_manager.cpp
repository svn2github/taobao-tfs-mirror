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
      writable_size_(0),
      update_size_(0),
      expired_size_(0),
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
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return service_.get_block_manager();
    }

    void WritableBlockManager::expire_one_block(const uint64_t block_id)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      WritableBlock* block = get_(block_id);
      if (NULL != block)
      {
        (*select_size(block->get_type()))--;
        block->set_type(BLOCK_EXPIRED);
        (*select_size(BLOCK_EXPIRED))++;
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
          (*iter)->set_type(BLOCK_EXPIRED);
        }
      }
      expired_size_ += update_size_;
      update_size_ = 0;
    }

    void WritableBlockManager::expire_all_blocks()
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      BLOCK_TABLE::iterator iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        (*iter)->set_type(BLOCK_EXPIRED);
      }
      expired_size_ += writable_size_ + update_size_;
      writable_size_ = 0;
      update_size_ = 0;
    }

    void WritableBlockManager::run_apply_and_giveup()
    {
      int ret = TFS_SUCCESS;
      const int32_t SLEEP_TIME_US = 1 * 1000 * 1000;
      int64_t last_giveup_time = 0;
      int32_t async_timeout = DEFAULT_NETWORK_CALL_TIMEOUT * 1000 + 100000;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        int64_t now = Func::get_monotonic_time();

        // giveup expired block first
        int32_t expired = size(BLOCK_EXPIRED);
        if (expired > 0 && last_giveup_time + async_timeout < now) // wait giveup callback end
        {
          ret = giveup_writable_block();
          last_giveup_time = now;
          TBSYS_LOG(DEBUG, "giveup writable block, ret: %d", ret);
        }

        // apply writable block
        int32_t need = MAX_WRITABLE_BLOCK_COUNT - size(BLOCK_WRITABLE);
        if (need > 0)
        {
          ret = apply_writable_block(need);
          TBSYS_LOG(DEBUG, "apply writabl block, count: %d, ret: %d", need, ret);
        }

        usleep(SLEEP_TIME_US);
      }
    }

    int32_t WritableBlockManager::size(const BlockType type)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return *select_size(type);
    }

    int32_t* WritableBlockManager::select_size(const BlockType type)
    {
      int32_t* size = NULL;
      if (type == BLOCK_WRITABLE)
      {
        size = &writable_size_;
      }
      else if (type == BLOCK_UPDATE)
      {
        size = &update_size_;
      }
      else
      {
        size = &expired_size_;
      }
      return size;
    }

    WritableBlock* WritableBlockManager::insert(const uint64_t block_id,
        const ArrayHelper<uint64_t>& servers, const BlockType type)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return insert_(block_id, servers, type);
    }

    WritableBlock* WritableBlockManager::remove(const uint64_t block_id)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(block_id);
    }

    WritableBlock* WritableBlockManager::get(const uint64_t block_id)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return get_(block_id);
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
          (*select_size(type))++;
        }
      }
      return result;
    }

    WritableBlock* WritableBlockManager::remove_(const uint64_t block_id)
    {
      WritableBlock* result = NULL;
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        WritableBlock query(block_id);
        result = writable_.erase(&query);
        if (NULL != result)
        {
          (*select_size(result->get_type()))--;
        }
      }
      return result;
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
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int retry = 0;
      int count = writable_.size();
      uint64_t block_id = INVALID_BLOCK_ID;
      block = NULL;
      while ((INVALID_BLOCK_ID == block_id) && (retry++ < count))
      {
        if (write_index_ >= count)
        {
          write_index_ = 0;
        }
        int32_t index = write_index_++;
        WritableBlock* target = writable_.at(index);
        assert(NULL != target);
        if (target->get_type() != BLOCK_EXPIRED && !target->get_use_flag())
        {
          target->set_use_flag(true);
          block_id = target->get_block_id();
          block = target;
        }
      }

      TBSYS_LOG(DEBUG, "alloc block %"PRI64_PREFIX"u", block_id);

      return (NULL != block) ? TFS_SUCCESS : EXIT_NO_WRITABLE_BLOCK;
    }

    int WritableBlockManager::alloc_update_block(const uint64_t block_id, WritableBlock*& block)
    {
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      block = NULL;
      WritableBlock* target = NULL;

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
        }
      }

      if (TFS_SUCCESS == ret)
      {
        block = target;
        target->set_use_flag(true);
      }

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

      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = post_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ds_async_callback);
        if (TFS_SUCCESS != ret)
        {
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }
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
            process_apply_update_block(resp_msg);
          }
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
        NewClientManager::get_instance().destroy_client(new_client);
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      TBSYS_LOG(DEBUG, "apply update block ret: %d", ret);

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

      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = post_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ds_async_callback);
        if (TFS_SUCCESS != ret)
        {
          NewClientManager::get_instance().destroy_client(new_client);
        }
      }
      return ret;
    }

    void WritableBlockManager::get_blocks(common::ArrayHelper<BlockInfoV2> blocks,
        const BlockType type)
    {
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
        if (DS_APPLY_BLOCK_MESSAGE == pcode)
        {
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          tbnet::Packet* packet = iter->second.second;
          if (DS_APPLY_BLOCK_RESPONSE_MESSAGE == packet->getPCode())
          {
            apply_block_callback(dynamic_cast<DsApplyBlockResponseMessage* >(packet));
          }
        }
        else if (DS_GIVEUP_BLOCK_MESSAGE == pcode)
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

    void WritableBlockManager::process_apply_update_block(message::DsApplyBlockForUpdateResponseMessage* response)
    {
      BlockLease& block_lease = response->get_block_lease();
      if (TFS_SUCCESS == block_lease.result_)
      {
        ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM,
            block_lease.servers_, block_lease.size_);
        insert(block_lease.block_id_, servers, BLOCK_UPDATE);
      }
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
          ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM,
              block_lease[index].servers_, block_lease[index].size_);
          insert(block_lease[index].block_id_,
              servers, BLOCK_WRITABLE);
        }
        TBSYS_LOG(DEBUG, "apply writable block %"PRI64_PREFIX"u ret %d",
            block_lease[index].block_id_, block_lease[index].result_);
      }
    }

    void WritableBlockManager::giveup_block_callback(DsGiveupBlockResponseMessage* response)
    {
      assert(NULL != response);
      BlockLease* block_lease = response->get_block_lease();
      int32_t size = response->get_size();
      for (int index = 0; index < size; index++)
      {
        if (TFS_SUCCESS == block_lease[index].result_)
        {
          uint64_t block_id = block_lease[index].block_id_;
          WritableBlock* block = remove(block_id);
          if (NULL != block)
          {
#ifndef TFS_GTEST
            get_block_manager().get_gc_manager().add(block);
#endif
          }
        }
        TBSYS_LOG(DEBUG, "giveup writable block %"PRI64_PREFIX"u ret %d",
            block_lease[index].block_id_, block_lease[index].result_);
      }
    }
  }
}

