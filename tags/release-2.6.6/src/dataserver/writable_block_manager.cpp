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
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      WritableBlock* block = get_(block_id);
      if (NULL != block)
      {
        expire_one_block_(block);
      }
    }

    void WritableBlockManager::expire_update_blocks()
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
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
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      BLOCK_TABLE::iterator iter = writable_.begin();
      for ( ; iter != writable_.end(); iter++)
      {
        expire_one_block_(*iter);
      }
    }

    bool WritableBlockManager::is_full(const uint64_t block_id)
    {
      BlockInfoV2 info;
      get_block_manager().get_block_info(info, block_id);
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      return info.size_ + BLOCK_RESERVER_LENGTH >= ds_info.max_block_size_ ||
        info.file_count_ >= common::MAX_SINGLE_BLOCK_FILE_COUNT;
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
      rwmutex_.wrlock();
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
        target = get(block_id);
        ret = NULL == target ? EXIT_NO_WRITABLE_BLOCK : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          ret = target->get_use_flag() ? EXIT_BLOCK_HAS_WRITE : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ret = target->get_type() == BLOCK_EXPIRED ? EXIT_NO_WRITABLE_BLOCK: TFS_SUCCESS;
          }
        }
      }

      // apply from ns
      if (EXIT_NO_WRITABLE_BLOCK == ret)
      {
        ret = apply_update_block(block_id);
        if (TFS_SUCCESS == ret)
        {
          target = get(block_id);
          ret = NULL == target ? EXIT_NO_WRITABLE_BLOCK : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
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

      TBSYS_LOG_DW(ret, "alloc update block %"PRI64_PREFIX"u, ret: %d", block_id, ret);

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
      create_msg_ref(DsApplyBlockMessage, req_msg);
      req_msg.set_size(count);
      req_msg.set_server_id(ds_info.information_.id_);
      ret = post_msg_to_server(ds_info.ns_vip_port_,&req_msg, ds_async_callback);
      TBSYS_LOG_IW(ret, "apply block, count: %d, ret: %d", count, ret);
      return ret;
    }

    // synchronized request to nameserver
    int WritableBlockManager::apply_update_block(const int64_t block_id)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsApplyBlockForUpdateMessage, req_msg);
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

      TBSYS_LOG_DW(ret, "apply update block %"PRI64_PREFIX"u, ret: %d", block_id, ret);

      return ret;
    }

    // asynchronized request to nameserver
    int WritableBlockManager::renew_writable_block()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsRenewBlockMessage, req_msg);
      req_msg.set_server_id(ds_info.information_.id_);
      ArrayHelper<BlockInfoV2> blocks(MAX_WRITABLE_BLOCK_COUNT, req_msg.get_block_infos());
      get_blocks(blocks, BLOCK_WRITABLE);
      req_msg.set_size(blocks.get_array_index());

      if (blocks.get_array_index() > 0)
      {
        ret = post_msg_to_server(ds_info.ns_vip_port_, &req_msg, ds_async_callback);
      }
      TBSYS_LOG_IW(ret, "renew block, count: %"PRI64_PREFIX"d, ret: %d", blocks.get_array_index(), ret);
      return ret;
    }


    // asynchronized request to nameserver
    int WritableBlockManager::giveup_writable_block()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsGiveupBlockMessage, req_msg);
      req_msg.set_server_id(ds_info.information_.id_);
      ArrayHelper<BlockInfoV2> blocks(MAX_WRITABLE_BLOCK_COUNT, req_msg.get_block_infos());
      get_blocks(blocks, BLOCK_EXPIRED);
      req_msg.set_size(blocks.get_array_index());

      if (blocks.get_array_index() > 0)
      {
        ret = post_msg_to_server(ds_info.ns_vip_port_, &req_msg, ds_async_callback);
      }
      TBSYS_LOG_IW(ret, "giveup block, count: %"PRI64_PREFIX"d, ret: %d", blocks.get_array_index(), ret);
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
        if (BLOCK_ANY == type || (*iter)->get_type() == type)
        {
          uint64_t block_id = (*iter)->get_block_id();
          BlockInfoV2 block_info;
          ret = get_block_manager().get_block_info(block_info, block_id);
          if (TFS_SUCCESS == ret)
          {
            blocks.push_back(block_info);
          }
          else if (EXIT_NO_LOGICBLOCK_ERROR == ret)
          {
            expire_one_block_(*iter);
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
          else if (STATUS_MESSAGE == packet->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(packet);
            ret = smsg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          TBSYS_LOG_IW(ret, "apply block callback, ret: %d", ret);
        }
        else if (DS_GIVEUP_BLOCK_MESSAGE == pcode && sresponse->size() > 0)
        {
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          tbnet::Packet* packet = iter->second.second;
          if (DS_GIVEUP_BLOCK_RESPONSE_MESSAGE == packet->getPCode())
          {
            giveup_block_callback(dynamic_cast<DsGiveupBlockResponseMessage* >(packet));
          }
          else if (STATUS_MESSAGE == packet->getPCode())
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(packet);
            ret = smsg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
          TBSYS_LOG_IW(ret, "giveup block callback, ret: %d", ret);
        }
        else if (DS_RENEW_BLOCK_MESSAGE == pcode)
        {
          if (sresponse->size() > 0)
          {
            NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
            tbnet::Packet* packet = iter->second.second;
            if (DS_RENEW_BLOCK_RESPONSE_MESSAGE == packet->getPCode())
            {
              renew_block_callback(dynamic_cast<DsRenewBlockResponseMessage* >(packet));
            }
            else if (STATUS_MESSAGE == packet->getPCode())
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(packet);
              ret = smsg->get_status();
            }
            else
            {
              ret = EXIT_UNKNOWN_MSGTYPE;
            }
          }
          else
          {
            ret = EXIT_TIMEOUT_ERROR;
          }

          if (TFS_SUCCESS != ret)
          {
            // timeout, expire writable blocks in renew list
            DsRenewBlockMessage* msg = dynamic_cast<DsRenewBlockMessage*>(source);
            BlockInfoV2* infos = msg->get_block_infos();
            int32_t size = msg->get_size();
            for (int i = 0; i < size; i++)
            {
              expire_one_block(infos[i].block_id_);
            }
          }
          TBSYS_LOG_IW(ret, "renew block callback, ret: %d", ret);
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
        TBSYS_LOG(INFO, "apply block %"PRI64_PREFIX"u, replica: %d, ret %d",
            block_lease[index].block_id_, block_lease[index].size_, block_lease[index].result_);
      }
    }

    void WritableBlockManager::renew_block_callback(DsRenewBlockResponseMessage* response)
    {
      assert(NULL != response);
      ArrayHelper<BlockLease> leases(response->get_size(),
          response->get_block_lease(), response->get_size());
      for (int index = 0; index < response->get_size(); index++)
      {
        BlockLease& lease = *leases.at(index);
        if (TFS_SUCCESS == lease.result_)
        {
          WritableBlock* block = get(lease.block_id_);
          if (NULL != block)
          {
            // update replica information
            RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
            ArrayHelper<uint64_t> helper(lease.size_, lease.servers_, lease.size_);
            block->set_servers(helper);
          }
        }
        else  // move to expired list
        {
          expire_one_block(lease.block_id_);
          TBSYS_LOG(INFO, "expire block %"PRI64_PREFIX"u because renew fail, ret: %d",
              lease.block_id_, lease.result_);
        }
        TBSYS_LOG_DW(lease.result_, "renew block %"PRI64_PREFIX"u, replica: %d, ret: %d",
            lease.block_id_, lease.size_, lease.result_);
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
        TBSYS_LOG(INFO, "giveup block %"PRI64_PREFIX"u, ret %d",
            block_lease[index].block_id_, block_lease[index].result_);
      }
    }
  }
}

