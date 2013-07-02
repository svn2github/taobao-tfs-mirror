/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include "common/lock.h"
#include "block_manager.h"
#include "server_collect.h"
#include "server_manager.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    static const int32_t MAX_BLOCK_CHUNK_SLOT_DEFALUT = 1024;
    static const int32_t MAX_BLOCK_CHUNK_SLOT_EXPAND_DEFAULT = 1024;
    static const float   MAX_BLOCK_CHUNK_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    BlockManager::BlockManager(LayoutManager& manager):
      manager_(manager),
      last_wirte_block_nums_(0)
    {
      for (int32_t i = 0; i < MAX_BLOCK_CHUNK_NUMS; i++)
      {
        blocks_[i] = new (std::nothrow)common::TfsSortedVector<BlockCollect*, BlockIdCompare>(MAX_BLOCK_CHUNK_SLOT_DEFALUT,
            MAX_BLOCK_CHUNK_SLOT_EXPAND_DEFAULT, MAX_BLOCK_CHUNK_SLOT_EXPAND_RATION_DEFAULT);
        assert(blocks_[i]);
      }
    }

    BlockManager::~BlockManager()
    {
      BlockCollect* block = NULL;
      for (int32_t i = 0; i < MAX_BLOCK_CHUNK_NUMS; i++)
      {
        for (int32_t j = 0; j < blocks_[i]->size(); j++)
        {
          block = blocks_[i]->at(j);
          tbsys::gDelete(block);
        }
        tbsys::gDelete(blocks_[i]);
      }
    }

    #ifdef TFS_GTEST
    void BlockManager::clear_()
    {
      for (int32_t i = 0; i < MAX_BLOCK_CHUNK_NUMS; i++)
      {
        blocks_[i]->clear();
      }
    }
    #endif

    BlockCollect* BlockManager::insert(const uint64_t block, const time_t now, const bool set)
    {
      RWLock::Lock lock(get_mutex_(block), WRITE_LOCKER);
      return insert_(block, now, set);
    }

    bool BlockManager::remove(GCObject*& gc_object, const uint64_t block)
    {
      RWLock::Lock lock(get_mutex_(block), WRITE_LOCKER);
      gc_object = remove_(block);
      return true;
    }

    BlockCollect* BlockManager::remove_(const uint64_t block)
    {
      BlockCollect query(block);
      return blocks_[get_chunk_(block)]->erase(&query);
    }

    BlockCollect* BlockManager::insert_(const uint64_t block_id, const time_t now, const bool set)
    {
      BlockCollect* block = new (std::nothrow)BlockCollect(block_id, now);
      assert(NULL != block);
      BlockCollect* result = NULL;
      int ret = blocks_[get_chunk_(block_id)]->insert_unique(result, block);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(block);
        if (EXIT_ELEMENT_EXIST == ret)
        {
          assert(NULL != result);
          if (set)
            result->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
        }
      }
      else
      {
        assert(NULL != result);
        if (set)
          result->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
      }
      return result;
    }

    BlockCollect* BlockManager::get(const uint64_t block) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      return get_(block);
    }

    bool BlockManager::push_to_delete_queue(const uint64_t block, const uint64_t server)
    {
      tbutil::Mutex::Lock lock(delete_queue_mutex_);
      delete_queue_.push_back(std::make_pair(server, block));
      return true;
    }

    bool BlockManager::pop_from_delete_queue_(std::pair<uint64_t,uint64_t>& output)
    {
      tbutil::Mutex::Lock lock(delete_queue_mutex_);
      bool ret = !delete_queue_.empty();
      if (ret)
      {
        output = delete_queue_.front();
        delete_queue_.pop_front();
      }
      return ret;
    }

    bool BlockManager::pop_from_delete_queue(std::pair<uint64_t,uint64_t>& output)
    {
      //这里有可能会出现遗漏，这种状况可以通过下一次汇报来处理
      bool ret = false;
      BlockCollect* block = NULL;
      ServerCollect* server = NULL;
      int32_t loop = 0;
      const int32_t MAX_LOOP_NUM = delete_queue_.size();
      const int8_t MIN_REPLICATE = SYSPARAM_NAMESERVER.max_replication_ > 1 ? 2 : SYSPARAM_NAMESERVER.max_replication_;
      while (loop++ < MAX_LOOP_NUM && !ret && pop_from_delete_queue_(output))
      {
        block = get(output.second);
        server = manager_.get_server_manager().get(output.first);
        ret = (NULL != block) && (NULL != server);
        if (ret)
        {
          get_mutex_(output.second).rdlock();
          int8_t size = block->get_servers_size();
          bool in_family = block->is_in_family();
          ret = in_family ? !block->exist(server->id()) : size >= MIN_REPLICATE && !block->exist(server->id());
          get_mutex_(output.second).unlock();
          if (!ret && size < MIN_REPLICATE && size > 0 && !in_family)
            push_to_delete_queue(output.second, output.first);
        }
      }
      return ret;
    }

    bool BlockManager::delete_queue_empty() const
    {
      return delete_queue_.empty();
    }

    void BlockManager::clear_delete_queue()
    {
      tbutil::Mutex::Lock lock(delete_queue_mutex_);
      delete_queue_.clear();
    }

    bool BlockManager::push_to_emergency_replicate_queue(BlockCollect* block)
    {
      bool ret = ((NULL != block) && (!block->in_replicate_queue()));
      if (ret)
      {
        TBSYS_LOG(INFO, "block %"PRI64_PREFIX"u mybe lack of backup, we'll replicate", block->id());
        block->set_in_replicate_queue(BLOCK_IN_REPLICATE_QUEUE_YES);
        emergency_replicate_queue_.push_back(block->id());
      }
      return ret;
    }

    BlockCollect* BlockManager::pop_from_emergency_replicate_queue()
    {
      BlockCollect* block = NULL;
      if (!emergency_replicate_queue_.empty())
      {
        uint64_t id = emergency_replicate_queue_.front();
        emergency_replicate_queue_.pop_front();
        block = get(id);
        if (NULL == block)
          TBSYS_LOG(INFO, "block: %"PRI64_PREFIX"u maybe lost,don't replicate", id);
        else
          block->set_in_replicate_queue(BLOCK_IN_REPLICATE_QUEUE_NO);
      }
      return block;
    }

    bool BlockManager::has_emergency_replicate_in_queue() const
    {
      return !emergency_replicate_queue_.empty();
    }

    int64_t BlockManager::get_emergency_replicate_queue_size() const
    {
      return emergency_replicate_queue_.size();
    }

    void BlockManager::dump(const int32_t level) const
    {
      UNUSED(level);
      TBSYS_LOG(DEBUG, "===========================DUMP BEGIN=====================");
      for (int32_t index = 0; index < MAX_BLOCK_CHUNK_NUMS; ++index)
      {
        BLOCK_MAP_ITER iter = blocks_[index]->begin();
        for (; iter != blocks_[index]->end(); ++iter)
        {
          TBSYS_LOG(DEBUG, "index: %d, block: %"PRI64_PREFIX"u", index, (*iter)->id());
        }
      }
      TBSYS_LOG(DEBUG, "===========================DUMP END=====================");
    }

    void BlockManager::dump_write_block(const int32_t level) const
    {
      UNUSED(level);
      TBSYS_LOG(DEBUG, "===========================DUMP BEGIN=====================");
      for (int32_t index = 0; index < MAX_BLOCK_CHUNK_NUMS; ++index)
      {
        LAST_WRITE_BLOCK_MAP_CONST_ITER iter = last_write_blocks_[index].begin();
        for (; iter != last_write_blocks_[index].end(); ++iter)
        {
          TBSYS_LOG(DEBUG, "has write index: %d, block: %"PRI64_PREFIX"u", index, iter->first);
        }
      }
      TBSYS_LOG(DEBUG, "===========================DUMP END=====================");
    }

    void BlockManager::clear_write_block()
    {
      for (int32_t index = 0; index < MAX_BLOCK_CHUNK_NUMS; ++index)
      {
        RWLock::Lock lock(rwmutex_[index], WRITE_LOCKER);
        last_write_blocks_[index].clear();
      }
    }

    bool BlockManager::scan(common::ArrayHelper<BlockCollect*>& result, uint64_t& begin, const int32_t count) const
    {
      bool end  = false;
      int32_t actual = 0;
      int32_t next = get_chunk_(begin);
      BlockCollect query(begin);
      bool all_over = next >= MAX_BLOCK_CHUNK_NUMS;
      for (; next < MAX_BLOCK_CHUNK_NUMS && actual < count;)
      {
        rwmutex_[next].rdlock();
        BLOCK_MAP_ITER iter = ((0 == begin) || end) ? blocks_[next]->begin() : blocks_[next]->lower_bound(&query);
        for (; iter != blocks_[next]->end(); ++iter)
        {
          result.push_back(*iter);
          if (++actual >= count)
          {
            ++iter;
            break;
          }
        }
        end = (blocks_[next]->end() == iter);
        all_over = ((next == MAX_BLOCK_CHUNK_NUMS - 1) && end);
        if (!end)
          begin = (*iter)->id();
        rwmutex_[next].unlock();
        if (end)
        {
          ++next;
          begin = 0;
          while ((0 == begin) && (next < MAX_BLOCK_CHUNK_NUMS))
          {
            rwmutex_[next].rdlock();
            if (!blocks_[next]->empty())
            {
              begin = (*blocks_[next]->begin())->id();
              rwmutex_[next].unlock();
            }
            else
            {
              rwmutex_[next].unlock();
              ++next;
            }
          }
        }
      }
      return all_over;
    }

    int BlockManager::scan(common::SSMScanParameter& param, int32_t& next, bool& all_over,
        bool& cutover, const int32_t should) const
    {
      int32_t actual = 0;
      BLOCK_MAP_ITER iter;
      all_over = next >= MAX_BLOCK_CHUNK_NUMS;
      while (next < MAX_BLOCK_CHUNK_NUMS && actual < should)
      {
        RWLock::Lock lock(rwmutex_[next], READ_LOCKER);
        BlockCollect query(param.addition_param1_);
        iter = cutover ?  blocks_[next]->begin() : blocks_[next]->lower_bound(&query);
        if (cutover && iter != blocks_[next]->end())
          param.addition_param1_ = (*iter)->id();
        for(;blocks_[next]->end() != iter; ++iter)
        {
          if (TFS_SUCCESS == (*iter)->scan(param))
          {
            if (++actual >= should)
            {
              ++iter;
              break;
            }
          }
        }
        cutover = blocks_[next]->end() == iter;
        if (!cutover)
          param.addition_param2_ = (*iter)->id();
        else
          ++next;
      }
      all_over = (next >= MAX_BLOCK_CHUNK_NUMS) ? true : (MAX_BLOCK_CHUNK_NUMS - 1 == next) ? cutover : false;
      return actual;
    }

    bool BlockManager::exist(const uint64_t block) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      return (NULL != pblock && pblock->id() == block);
    }

    int BlockManager::get_servers(ArrayHelper<uint64_t>& servers, const BlockCollect* block) const
    {
      int32_t ret = (NULL == block) ? EXIT_NO_BLOCK : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
        ret = get_servers_(servers, block);
      }
      return ret;
    }

    int BlockManager::get_servers(ArrayHelper<uint64_t>& servers, const uint64_t block) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      return get_servers_(servers, pblock);
    }

    int BlockManager::get_servers_size(const uint64_t block) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      return (NULL != pblock) ? pblock->get_servers_size() : 0;
    }

    uint64_t BlockManager::get_server(const uint64_t block, const int8_t index) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      return (NULL != pblock) ? pblock->get_server(index) : INVALID_SERVER_ID;
    }

    bool BlockManager::exist(const BlockCollect* block, const ServerCollect* server) const
    {
      bool ret = (NULL != block && NULL != server);
      if (ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
        ret = block->exist(server->id());
      }
      return ret;
    }

    RWLock& BlockManager::get_mutex_(const uint64_t block) const
    {
      return rwmutex_[get_chunk_(block)];
    }

    int BlockManager::update_relation(std::vector<uint64_t>& expires, ServerCollect* server,
        const common::ArrayHelper<common::BlockInfoV2>& blocks, const time_t now)
    {
      int32_t ret = (NULL != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        server->clear(manager_, now);
        ret = update_relation_(expires, server, blocks, now);
      }
      return ret;
    }

    int BlockManager::update_relation_(std::vector<uint64_t>& cleanup_family_id_array, ServerCollect* server,
        const common::ArrayHelper<common::BlockInfoV2>& blocks, const time_t now)
    {
      int32_t ret = (NULL != server && server->is_alive()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        cleanup_family_id_array.clear();
        uint64_t servers[MAX_REPLICATION_NUM];
        ArrayHelper<uint64_t> helper(MAX_REPLICATION_NUM, servers);
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        for (int64_t i= 0; i < blocks.get_array_index(); ++i)
        {
          helper.clear();
          int8_t all_server_size = 0;
          int64_t family_id = INVALID_FAMILY_ID;
          bool writable = false, master = false;
          BlockInfoV2& info = (*blocks.at(i));

          // check block version, rebuilding relation.
          get_mutex_(info.block_id_).wrlock();
          BlockCollect* block = get_(info.block_id_);
          bool isnew = (NULL == block);
          if (isnew)
          {
            block = insert_(info.block_id_, now);
          }
          ret = NULL != block ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            family_id = block->get_family_id();
            if (INVALID_FAMILY_ID == family_id)
            {
              if (INVALID_FAMILY_ID != info.family_id_)
                cleanup_family_id_array.push_back(info.block_id_);
              info.family_id_ = INVALID_FAMILY_ID;
              ret = IS_VERFIFY_BLOCK(info.block_id_) ? EXIT_EXPIRE_SELF_ERROR : TFS_SUCCESS;
              if (TFS_SUCCESS == ret)
              {
                ret = block->check_version(manager_, helper, server->id(), isnew, info, now);
                if (TFS_SUCCESS == ret)//build relation
                  ret = build_relation_(block, writable, master, server->id(),now);
              }
              else
              {
                block = remove_(info.block_id_);
                tbsys::gDelete(block);
              }
            }
            else
            {
              ret =  (info.family_id_ == family_id) ? TFS_SUCCESS : EXIT_EXPIRE_SELF_ERROR ;
              if (TFS_SUCCESS == ret)
              {
                if (info.version_ == block->version())
                {
                  ret = (block->get_servers_size() > 0 && !block->exist(server->id())) ? EXIT_EXPIRE_SELF_ERROR : TFS_SUCCESS;
                  if (TFS_SUCCESS == ret)
                    ret = build_relation_(block, writable, master, server->id(),now);
                }

                if (info.version_ > block->version())
                {
                  block->update(info);
                  if (!block->exist(server->id()))
                  {
                    block->cleanup(helper);
                    ret = build_relation_(block, writable, master, server->id(),now);
                  }
                }

                if (info.version_ < block->version())
                {
                  ret = (block->get_servers_size() > 0 && !block->exist(server->id())) ? EXIT_EXPIRE_SELF_ERROR : TFS_SUCCESS;
                  if (TFS_SUCCESS == ret)
                  {
                    block->cleanup(helper);
                    ret = build_relation_(block, writable, master, server->id(),now);
                  }
                }
              }
            }
            if (NULL != block)
              all_server_size = block->get_servers_size();
          }
          get_mutex_(info.block_id_).unlock();

          if (TFS_SUCCESS == ret)
          {
            manager_.get_server_manager().build_relation(server, block->id(), writable, master);
          }

          for (int64_t index = 0; index < helper.get_array_index(); ++index)
          {
            uint64_t id = *helper.at(index);
            manager_.get_server_manager().relieve_relation(*helper.at(index), block->id());
            if (ngi.is_master() && all_server_size > 0)//i'm master, we're going to expire blocks
              push_to_delete_queue(info.block_id_, id);
          }

          if (EXIT_EXPIRE_SELF_ERROR == ret && ngi.is_master())
          {
            ret = TFS_SUCCESS;
            push_to_delete_queue(info.block_id_, server->id());
          }
        }
      }
      return ret;
    }

    int BlockManager::relieve_relation_(ServerCollect* server, const common::ArrayHelper<common::BlockInfoV2>& blocks, const time_t now)
    {
      int32_t ret = (NULL != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        GCObject* object = NULL;
        BlockCollect* block = NULL;
        for (int64_t index = 0; index < blocks.get_array_index(); ++index)
        {
          const BlockInfoV2& info = (*blocks.at(index));
          block = get(info.block_id_);
          ret = relieve_relation(block, server->id(), now);
          if ((NULL != block) && (block->get_servers_size() <= 0))
          {
            ret = remove(object , info.block_id_);
            if (NULL != object)
              manager_.get_gc_manager().add(object);
          }
          ret = manager_.get_server_manager().relieve_relation(server, info.block_id_);
        }
      }
      return TFS_SUCCESS;
    }

    int BlockManager::build_relation(BlockCollect* block, bool& writable, bool& master,
        const uint64_t server, const time_t now, const bool set)
    {
      int32_t ret = ((NULL != block) && (INVALID_SERVER_ID != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), WRITE_LOCKER);
        ret = build_relation_(block, writable, master, server, now, set);
      }
      return ret;
    }

    int BlockManager::build_relation_(BlockCollect* block, bool& writable, bool& master,
        const uint64_t server, const time_t now, const bool set)
    {
      int32_t ret = ((NULL != block) && (INVALID_SERVER_ID != server)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = block->add(writable, master, server, now);
        if (set && (TFS_SUCCESS == ret || EXIT_SERVER_EXISTED == ret))
          block->set_create_flag();
      }
      return ret;
    }

    BlockCollect* BlockManager::get_(const uint64_t block) const
    {
      int32_t index = get_chunk_(block);
      BlockCollect query(block);
      BLOCK_MAP_ITER iter = blocks_[index]->find(&query);
      return blocks_[index]->end() == iter ? NULL : (*iter);
    }

    int BlockManager::update_block_info(BlockCollect*& output, bool& isnew, bool& writable, bool& master,
        const common::BlockInfoV2& info, const ServerCollect* server, const time_t now, const bool addnew)
    {
      isnew = false;
      int32_t ret = (NULL != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* block = get(info.block_id_);
        if ((NULL == block && addnew))
        {
          isnew = true;
          RWLock::Lock lock(get_mutex_(info.block_id_), WRITE_LOCKER);
          block = insert_(info.block_id_, now);
          assert(NULL != block);
          ret = build_relation_(block, writable, master, server->id(), now);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = NULL == block ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            if (block->version() > info.version_)//check version
            {
              TBSYS_LOG(INFO, "it's error that update block: %"PRI64_PREFIX"u information because old version: %d >= new version: %d",
                  info.block_id_, block->version(), info.version_);
              ret = EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR;//version error
            }
            else
            {
              output = block;
              block->update(info);
            }
          }
        }
      }
      return ret;
    }


    int BlockManager::update_block_info(const common::BlockInfoV2& info, BlockCollect* block)
    {
      int32_t ret = (NULL != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), WRITE_LOCKER);
        block->update(info);
      }
      return ret;
    }

    int BlockManager::update_family_id(const uint64_t block, const uint64_t family_id)
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      int32_t ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        pblock->set_family_id(family_id);
      }
      return ret;
    }

    int BlockManager::relieve_relation(BlockCollect* block, const uint64_t server, const time_t now)
    {
      RWLock::Lock lock(get_mutex_(block->id()), WRITE_LOCKER);
      return ((NULL != block) && (INVALID_SERVER_ID != server)) ? block->remove(server, now) : EXIT_PARAMETER_ERROR;
    }

    int BlockManager::relieve_relation(const uint64_t block, const uint64_t server, const time_t now)
    {
      int32_t ret = (INVALID_BLOCK_ID != block && INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(block), WRITE_LOCKER);
        BlockCollect* pblock = get_(block);
        ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = pblock->remove(server, now);
        }
      }
      return ret;
    }

    bool BlockManager::need_replicate(const BlockCollect* block) const
    {
      RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
      return (NULL != block) ? (block->get_servers_size() < SYSPARAM_NAMESERVER.max_replication_ && !block->is_in_family()) : false;
    }

    bool BlockManager::need_replicate(const BlockCollect* block, const time_t now) const
    {
      RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
      return (NULL != block) ? (block->check_replicate(now) >= PLAN_PRIORITY_NORMAL) : false;
    }

    bool BlockManager::need_replicate(ArrayHelper<uint64_t>& servers, PlanPriority& priority,
        const BlockCollect* block, const time_t now) const
    {
      bool ret = NULL != block;
      if (ret)
      {
        get_mutex_(block->id()).rdlock();
        priority = block->check_replicate(now);
        ret = ((priority >= PLAN_PRIORITY_NORMAL) && (!has_write_(block->id(), now)));
        if (ret)
          block->get_servers(servers);
        get_mutex_(block->id()).unlock();
        ret = (ret && !manager_.get_task_manager().exist_block(block->id()));
      }
      return ret;
    }

    bool BlockManager::need_compact(const BlockCollect* block, const time_t now, const bool check_in_family) const
    {
      RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
      return (NULL != block) ? (block->check_compact(now, check_in_family) && (!has_write_(block->id(), now))) : false;
    }

    bool BlockManager::need_compact(ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now, const bool check_in_family) const
    {
      bool ret = (NULL != block);
      if (ret)
      {
        get_mutex_(block->id()).rdlock();
        ret = ((block->check_compact(now,check_in_family)) && (!has_write_(block->id(), now)));
        if (ret)
          block->get_servers(servers);
        get_mutex_(block->id()).unlock();
        ret = (ret && !manager_.get_task_manager().exist_block(block->id())
            && !manager_.get_task_manager().exist(servers));
      }
      return ret;
    }

    bool BlockManager::need_balance(common::ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now) const
    {
      bool ret = (NULL != block);
      if (ret)
      {
        get_mutex_(block->id()).rdlock();
        ret = ((block->check_balance()) && (!has_write_(block->id(), now)));
        if (ret)
          block->get_servers(servers);
        get_mutex_(block->id()).unlock();
        ret = ret && !manager_.get_task_manager().exist_block(block->id());
      }
      return ret;
    }

    bool BlockManager::need_marshalling(const uint64_t block, const time_t now)
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      BlockCollect* pblock = get_(block);
      return  (NULL != pblock) ? ((pblock->check_marshalling()) && (!has_write_(pblock->id(), now))) : false;
    }

    bool BlockManager::need_marshalling(const BlockCollect* block, const time_t now)
    {
      RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
      return  (NULL != block) ? ((block->check_marshalling()) && (!has_write_(block->id(), now))) : false;
    }

    bool BlockManager::need_marshalling(common::ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now) const
    {
      bool ret = (NULL != block);
      if (ret)
      {
        get_mutex_(block->id()).rdlock();
        ret = ((block->check_marshalling()) && (!has_write_(block->id(), now)));
        if (ret)
          block->get_servers(servers);
        get_mutex_(block->id()).unlock();
        ret = ret && !manager_.get_task_manager().exist_block(block->id());
      }
      return ret;
    }

    bool BlockManager::need_reinstate(const BlockCollect* block, const time_t now) const
    {
      bool ret = (NULL != block && block->is_in_family());
      if (ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
        ret = block->check_reinstate(now);
      }
      return ret;
    }

    bool BlockManager::need_adjust_copies_location(common::ArrayHelper<uint64_t>& adjust_copies, const BlockCollect* block, const time_t now) const
    {
      bool ret = (NULL != block && !block->is_in_family());
      if (ret)
      {
        RWLock::Lock lock(get_mutex_(block->id()), READ_LOCKER);
        ret = block->check_need_adjust_copies_location(adjust_copies, now);
      }
      return ret;
    }

    int BlockManager::expand_ratio(int32_t& index, const float expand_ratio)
    {
      if (++index >= MAX_BLOCK_CHUNK_NUMS)
        index = 0;
      RWLock::Lock lock(rwmutex_[index], WRITE_LOCKER);
      if (blocks_[index]->need_expand(expand_ratio))
        blocks_[index]->expand_ratio(expand_ratio);
      return TFS_SUCCESS;
    }

    bool BlockManager::has_write(const uint64_t block, const time_t now) const
    {
      RWLock::Lock lock(get_mutex_(block), READ_LOCKER);
      return has_write_(block, now);
    }

    bool BlockManager::has_write_(const uint64_t block, const time_t now) const
    {
      LAST_WRITE_BLOCK_MAP_CONST_ITER iter = last_write_blocks_[get_chunk_(block)].find(block);
      bool ret = last_write_blocks_[get_chunk_(block)].end() == iter ? false : now < iter->second;
      if (ret)
      {
        TBSYS_LOG(INFO, "block : %"PRI64_PREFIX"u, %d, %ld, %ld", block,
          last_write_blocks_[get_chunk_(block)].end() == iter, iter->second, now);
      }
      return ret;
      //return last_write_blocks_[get_chunk_(block)].end() == iter ? false : iter->second < now;
    }

    void BlockManager::timeout(const time_t now)
    {
      int32_t actual = 0;
      int32_t cleanup_nums = last_wirte_block_nums_ - SYSPARAM_NAMESERVER.cleanup_write_timeout_threshold_;
      int32_t need_cleanup_nums = cleanup_nums;
      if (cleanup_nums > 0)
      {
        LAST_WRITE_BLOCK_MAP_ITER iter;
        uint32_t percent = last_wirte_block_nums_ <= MAX_BLOCK_CHUNK_NUMS ? 1 :
          last_wirte_block_nums_/ MAX_BLOCK_CHUNK_NUMS;
        int32_t next = random() % MAX_BLOCK_CHUNK_NUMS;
        int32_t index = next;
        for (int32_t i = 0; i < MAX_BLOCK_CHUNK_NUMS && cleanup_nums > 0; ++i,++next)
        {
          index = next % MAX_BLOCK_CHUNK_NUMS;
          RWLock::Lock lock(rwmutex_[index], WRITE_LOCKER);
          TBSYS_LOG(DEBUG, "last_write_blocks_.size: %zd, percent: %u", last_write_blocks_[index].size(), percent);
          if (last_write_blocks_[index].size() >= percent)
          {
            iter = last_write_blocks_[index].begin();
            for (; iter != last_write_blocks_[index].end() && cleanup_nums > 0;)
            {
              if (iter->second < now)
              {
                ++actual;
                --cleanup_nums;
                last_write_blocks_[index].erase(iter++);
              }
              else
              {
                ++iter;
              }
            }
          }
        }
        TBSYS_LOG(DEBUG, "cleanup write block entry, total: %d, need cleanup nums: %d, actual cleanup nums: %d",
            last_wirte_block_nums_, need_cleanup_nums, actual);
        last_wirte_block_nums_ -= actual;
      }
    }

    int BlockManager::update_block_last_wirte_time(uint64_t & id, const uint64_t block, const time_t now)
    {
      static uint64_t id_factory = 0;
      id = atomic_inc(&id_factory);
      RWLock::Lock lock(get_mutex_(block), WRITE_LOCKER);
      std::pair<LAST_WRITE_BLOCK_MAP_ITER, bool> res =
        last_write_blocks_[get_chunk_(block)].insert(
            LAST_WRITE_BLOCK_MAP::value_type(block, now +SYSPARAM_NAMESERVER.max_write_timeout_));
      if (res.second)
        ++last_wirte_block_nums_;
      else
        res.first->second = now + SYSPARAM_NAMESERVER.max_write_timeout_;
      return TFS_SUCCESS;
    }

    int32_t BlockManager::get_chunk_(const uint64_t block) const
    {
      return  block % MAX_BLOCK_CHUNK_NUMS;
    }

    int BlockManager::get_servers_(ArrayHelper<uint64_t>& servers, const BlockCollect* block) const
    {
      int32_t ret = (NULL != block && servers.get_array_size() > 0)? TFS_SUCCESS : EXIT_NO_BLOCK;
      if (TFS_SUCCESS == ret)
      {
        servers.clear();
        block->get_servers(servers);
        ret = servers.empty() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
      }
      return ret;
    }
  }/** nameserver **/
}/** tfs **/
