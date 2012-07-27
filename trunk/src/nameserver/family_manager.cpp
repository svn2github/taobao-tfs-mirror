/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
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

#include "layout_manager.h"
#include "family_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    static const int32_t MAX_FAMILY_CHUNK_NUM_SLOT_DEFALUT = 256;
    static const int32_t MAX_FAMILY_CHUNK_NUM_SLOT_EXPAND_DEFAULT = 128;
    static const float   MAX_FAMILY_CHUNK_NUM_SLOT_EXPAND_RATION_DEFAULT = 0.1;
    int FamilyManager::MarshallingItem::insert(const uint64_t server, const uint32_t block)
    {
      int32_t ret = (INVALID_SERVER_ID != server && INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = -1;
        bool exist    = false;
        last_update_time_ = Func::get_monotonic_time();
        for (int32_t i = 0; i < MAX_SINGLE_RACK_SERVER_NUM && !exist; i++)
        {
          exist = slot_[i].first == server;
          if (!exist && index < 0)
          {
            if (INVALID_SERVER_ID == slot_[i].first)
              index = i;
          }
        }
        ret = !exist ? TFS_SUCCESS : EXIT_ELEMENT_EXIST;
        ret = (TFS_SUCCESS == ret && index >= 0 && index < MAX_SINGLE_RACK_SERVER_NUM) ? TFS_SUCCESS : EXIT_OUT_OF_RANGE;
        if (TFS_SUCCESS == ret)
        {
          slot_[index].first = server;
          slot_[index].second= block;
          ++slot_num_;
        }
      }
      return ret;
    }

    int FamilyManager::MarshallingItem::choose_item_random(std::pair<uint64_t, uint32_t>& out)
    {
      int32_t ret = slot_num_ > 0  ? TFS_SUCCESS : EXIT_MARSHALLINT_ITEM_QUEUE_EMPTY;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = 0;
        int32_t random_num = 0;
        const int32_t MAX_RANDOM_NUM = 4;
        last_update_time_ = Func::get_monotonic_time();
        do
        {
          index = random() % MAX_SINGLE_RACK_SERVER_NUM;
          ret = INVALID_SERVER_ID != slot_[index].first ? TFS_SUCCESS : EXIT_MARSHALLINT_ITEM_QUEUE_EMPTY;
        }
        while (random_num++ < MAX_RANDOM_NUM && TFS_SUCCESS != ret);
        if (TFS_SUCCESS != ret)
        {
          index = 0;
          do
          {
            ret = INVALID_SERVER_ID != slot_[index].first ? TFS_SUCCESS : EXIT_MARSHALLINT_ITEM_QUEUE_EMPTY;
          }
          while (TFS_SUCCESS != ret && ++index < MAX_SINGLE_RACK_SERVER_NUM);
        }
        if (TFS_SUCCESS == ret && index >= 0 && index < MAX_SINGLE_RACK_SERVER_NUM)
        {
          --slot_num_;
          out = slot_[index];
          slot_[index].first = INVALID_SERVER_ID;
          slot_[index].second= INVALID_BLOCK_ID;
        }
      }
      return ret;
    }

    #ifdef TFS_GTEST
    bool FamilyManager::MarshallingItem::get(std::pair<uint64_t, uint32_t>& pair, const uint64_t server, const uint32_t block) const
    {
      bool exist = false;
      for (int32_t index = 0; index < MAX_SINGLE_RACK_SERVER_NUM && !exist; index++)
      {
        exist = ((slot_[index].first == server) && (slot_[index].second == block));
        if (exist)
          pair = slot_[index];
      }
      return exist;
    }
    #endif

    FamilyManager::FamilyManager(LayoutManager& manager):
      manager_(manager),
      marshalling_queue_(MAX_RACK_NUM, 1, 0.1)
    {
      for (int32_t i = 0; i < MAX_FAMILY_CHUNK_NUM; ++i)
      {
        families_[i] = new (std::nothrow)TfsSortedVector<FamilyCollect*, FamilyIdCompare>(MAX_FAMILY_CHUNK_NUM_SLOT_DEFALUT,
            MAX_FAMILY_CHUNK_NUM_SLOT_EXPAND_DEFAULT, MAX_FAMILY_CHUNK_NUM_SLOT_EXPAND_RATION_DEFAULT);
        assert(families_[i]);
      }
    }

    FamilyManager::~FamilyManager()
    {
      for (int32_t i = 0; i < MAX_FAMILY_CHUNK_NUM; ++i)
      {
        FAMILY_MAP_ITER iter = families_[i]->begin();
        for (; iter != families_[i]->end(); ++iter)
        {
          tbsys::gDelete((*iter));
        }
        tbsys::gDelete(families_[i]);
      }
      MARSHALLING_MAP_ITER it = marshalling_queue_.begin();
      for (; it != marshalling_queue_.end(); ++it)
      {
        tbsys::gDelete((*it));
      }
    }

    int FamilyManager::insert(const int64_t family_id, const int32_t family_aid_info,
          const common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const time_t now)
    {
      RWLock::Lock lock(get_mutex_(family_id), WRITE_LOCKER);
      return insert_(family_id, family_aid_info, members, now);
    }

    bool FamilyManager::exist(int32_t& version, const int64_t family_id, const uint32_t block, const int32_t new_version)
    {
      version = 0;
      bool ret = (INVALID_FAMILY_ID != family_id && INVALID_BLOCK_ID != block && new_version > 0);
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(family_id), READ_LOCKER);
        FamilyCollect* family = get_(family_id);
        ret = family->exist(version, block, new_version);
      }
      return ret;
    }

    int FamilyManager::update(const int64_t family_id, const uint32_t block, const int32_t new_version)
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && INVALID_BLOCK_ID != block && new_version > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(family_id), WRITE_LOCKER);
        FamilyCollect* family = get_(family_id);
        ret = (NULL != family) ? TFS_SUCCESS : EXIT_NO_FAMILY;
        if (TFS_SUCCESS == ret)
        {
          ret = family->update(block, new_version);
        }
      }
      return ret;
    }

    int FamilyManager::remove(GCObject*& object, const int64_t family_id)
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(family_id), WRITE_LOCKER);
        FamilyCollect query(family_id);
        object = families_[get_chunk_(family_id)]->erase(&query);
        ret = NULL != object ? TFS_SUCCESS : EXIT_NO_FAMILY;
      }
      return ret;
    }

    FamilyCollect* FamilyManager::get(const int64_t family_id) const
    {
      RWLock::Lock lock(get_mutex_(family_id), READ_LOCKER);
      return get_(family_id);
    }

    bool FamilyManager::scan(common::ArrayHelper<FamilyCollect*>& result, int64_t& begin, const int32_t count) const
    {
      bool end  = false;
      int32_t actual = 0;
      int32_t next = get_chunk_(begin);
      FamilyCollect query(begin);
      bool all_over = next >= MAX_FAMILY_CHUNK_NUM;
      for (; next < MAX_FAMILY_CHUNK_NUM && actual < count;)
      {
        mutexs_[next].rdlock();
        FAMILY_MAP_ITER iter = ((0 == begin) || end) ? families_[next]->begin() : families_[next]->lower_bound(&query);
        for (; iter != families_[next]->end(); ++iter)
        {
          result.push_back(*iter);
          if (++actual >= count)
          {
            ++iter;
            break;
          }
        }
        end = (families_[next]->end() == iter);
        all_over = ((next == MAX_FAMILY_CHUNK_NUM - 1) && end);
        if (!end)
          begin = (*iter)->get_family_id();
        mutexs_[next].unlock();
        if (end)
        {
          ++next;
          begin = 0;
          while ((0 == begin) && (next < MAX_FAMILY_CHUNK_NUM))
          {
            mutexs_[next].rdlock();
            if (!families_[next]->empty())
            {
              begin = (*families_[next]->begin())->get_family_id();
              mutexs_[next].unlock();
            }
            else
            {
              mutexs_[next].unlock();
              ++next;
            }
          }
        }
      }
      return all_over;
    }

    int FamilyManager::scan(common::SSMScanParameter& param, int32_t& next, bool& all_over,
        bool& cutover, const int32_t should) const
    {
      int32_t actual = 0;
      FAMILY_MAP_ITER iter;
      all_over = next >= MAX_FAMILY_CHUNK_NUM;
      while (next < MAX_FAMILY_CHUNK_NUM && actual < should)
      {
        RWLock::Lock lock(mutexs_[next], READ_LOCKER);
        FamilyCollect query(param.addition_param1_);
        iter = cutover ?  families_[next]->begin() : families_[next]->lower_bound(&query);
        if (cutover && iter != families_[next]->end())
          param.addition_param1_ = (*iter)->get_family_id();
        for(;families_[next]->end() != iter; ++iter)
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
        cutover = families_[next]->end() == iter;
        if (!cutover)
          param.addition_param2_ = (*iter)->get_family_id();
        else
          ++next;
      }
      all_over = (next >= MAX_FAMILY_CHUNK_NUM) ? true : (MAX_FAMILY_CHUNK_NUM - 1 == next) ? cutover : false;
      return actual;
    }


    int FamilyManager::get_members(common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const int64_t family_id) const
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && members.get_array_size() > 0 ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(get_mutex_(family_id), READ_LOCKER);
        ret = get_members_(members, family_id);
      }
      return ret;
    }

    int FamilyManager::get_members(common::ArrayHelper<BlockCollect*>& members, const int64_t family_id) const
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && members.get_array_size() > 0 ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::pair<uint32_t, int32_t> result[MAX_MARSHLLING_NUM];
        common::ArrayHelper<std::pair<uint32_t, int32_t> > helper(MAX_MARSHLLING_NUM, result);
        get_mutex_(family_id).rdlock();
        ret = get_members_(helper, family_id);
        get_mutex_(family_id).unlock();
        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < helper.get_array_index(); i++)
          {
            std::pair<uint32_t, int32_t> item = *helper.at(i);
            BlockCollect* block = manager_.get_block_manager().get(item.first);
            assert(block);
            members.push_back(block);
          }
        }
      }
      return ret;
    }

    int FamilyManager::get_members(common::ArrayHelper<std::pair<BlockCollect*, ServerCollect*> >& members,
          const int64_t family_id) const
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && members.get_array_size() > 0 ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* result[MAX_MARSHLLING_NUM];
        common::ArrayHelper<BlockCollect*> helper(MAX_MARSHLLING_NUM, result);
        ret = get_members(helper, family_id);
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* servers[SYSPARAM_NAMESERVER.max_replication_];
          common::ArrayHelper<ServerCollect*> helper2(SYSPARAM_NAMESERVER.max_replication_, servers);
          for (int32_t i = 0; i < helper.get_array_index() && TFS_SUCCESS == ret; ++i)
          {
            BlockCollect* block = *helper.at(i);
            assert(block);
            helper2.clear();
            ret = manager_.get_block_manager().get_servers(helper2, block);
            if (TFS_SUCCESS == ret)
            {
              ServerCollect* server = *helper2.at(0);
              assert(server);
              members.push_back(std::make_pair(block, server));
            }
          }
        }
      }
      return ret;
    }

    bool FamilyManager::push_to_reinstate_or_dissolve_queue(FamilyCollect* family)
    {
      bool ret = ((NULL != family) && (!family->in_reinstate_or_dissolve_queue()));
      if (ret)
      {
        TBSYS_LOG(INFO, "family %"PRI64_PREFIX"d mybe lack of backup, we'll reinstate or dissolve", family->get_family_id());
        family->set_in_reinstate_or_dissolve_queue(FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_YES);
        reinstate_or_dissolve_queue_.push_back(family->get_family_id());
      }
      return ret;
    }

    FamilyCollect* FamilyManager::pop_from_reinstate_or_dissolve_queue()
    {
      FamilyCollect* family = NULL;
      if (!reinstate_or_dissolve_queue_.empty())
      {
        int64_t family_id = reinstate_or_dissolve_queue_.front();
        reinstate_or_dissolve_queue_.pop_front();
        family = get(family_id);
        if (NULL == family)
          TBSYS_LOG(INFO, "family %"PRI64_PREFIX"d mybe delete, don't reinstate or dissolve", family_id);
        else
          family->set_in_reinstate_or_dissolve_queue(FAMILY_IN_REINSTATE_OR_DISSOLVE_QUEUE_NO);
      }
      return family;
    }

    bool FamilyManager::reinstate_or_dissolve_queue_empty() const
    {
      return reinstate_or_dissolve_queue_.empty();
    }

    int64_t FamilyManager::get_reinstate_or_dissolve_queue_size() const
    {
      return reinstate_or_dissolve_queue_.size();
    }

    bool FamilyManager::has_marshalling(int32_t& current_version, const int64_t family_id, const uint32_t block, const int32_t version) const
    {
      bool ret = (INVALID_FAMILY_ID != family_id && INVALID_BLOCK_ID != block && version > 0);
      if (ret)
      {
        RWLock::Lock lock(get_mutex_(family_id), READ_LOCKER);
        ret = has_marshalling_(current_version, family_id, block, version);
      }
      return ret;
    }

    bool FamilyManager::push_block_to_marshalling_queues(const BlockCollect* block, const time_t now)
    {
      bool ret = (NULL != block);
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* servers[SYSPARAM_NAMESERVER.max_replication_];
        common::ArrayHelper<ServerCollect*> helper(SYSPARAM_NAMESERVER.max_replication_, servers);
        if (ret = (manager_.get_block_manager().need_marshalling(helper, block, now)
          && !manager_.get_task_manager().exist(block->id())
          && !manager_.get_task_manager().exist(helper)))
        {
          uint32_t rack = 0;
          int64_t  index = 0;
          ServerCollect* server = NULL;
          for (; index < helper.get_array_index() && ret; ++index)
          {
            server = *helper.at(index);
            assert(server);
            ret = manager_.get_task_manager().has_space_do_task_in_machine(server->id());
            if (ret)
            {
              rack = SYSPARAM_NAMESERVER.group_mask_ > 0 ? Func::get_lan(server->id(), SYSPARAM_NAMESERVER.group_mask_)
                     : random() % MAX_RACK_NUM;
              ret = push_block_to_marshalling_queues(rack, server->id(), block->id());
            }
          }
          ret = (ret && index == helper.get_array_index());
        }
      }
      return ret;
    }

    int FamilyManager::push_block_to_marshalling_queues(const uint32_t rack, const uint64_t server, const uint32_t block)
    {
      int32_t ret = (INVALID_RACK_ID != rack && INVALID_BLOCK_ID != block && INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(marshallin_queue_mutex_, WRITE_LOCKER);
        MarshallingItem query;
        query.rack_ = rack;
        MarshallingItem* item = NULL;
        MARSHALLING_MAP_ITER iter = marshalling_queue_.find(&query);
        if (iter == marshalling_queue_.end())
        {
          MarshallingItem* result = NULL;
          item = new (std::nothrow)MarshallingItem();
          assert(item);
          memset(item, 0, sizeof(MarshallingItem));
          item->rack_ = rack;
          item->last_update_time_ = Func::get_monotonic_time();
          ret = marshalling_queue_.insert_unique(result, item);
          assert(TFS_SUCCESS == ret);
        }
        else
        {
          item = (*iter);
        }
        if (NULL != item)
        {
          item->insert(server, block);
        }
      }
      return ret;
    }

    int FamilyManager::marshalling_queue_timeout(const time_t now)
    {
      const int32_t TIMEOUT = 86000;
      const int32_t MAX_CLEANUP_NUM = 8;
      MarshallingItem* items[MAX_CLEANUP_NUM];
      common::ArrayHelper<MarshallingItem*> helper(MAX_CLEANUP_NUM, items);
      RWLock::Lock lock(marshallin_queue_mutex_, WRITE_LOCKER);
      MARSHALLING_MAP_ITER iter = marshalling_queue_.begin();
      for (; iter != marshalling_queue_.end() && helper.get_array_index() < MAX_CLEANUP_NUM; ++iter)
      {
        if (((*iter)->last_update_time_ + TIMEOUT)  < now)
        {
          helper.push_back((*iter));
        }
      }

      for (int32_t i = 0; i < helper.get_array_index(); ++i)
      {
        MarshallingItem* item = *helper.at(i);
        marshalling_queue_.erase(item);
        tbsys::gDelete(item);
      }
      return helper.get_array_index();
    }

    int FamilyManager::create_family_choose_data_members(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& members,
        const int32_t data_member_num)
    {
      int32_t ret = (members.get_array_size() > 0 &&  data_member_num > 0 && data_member_num <= MAX_DATA_MEMBER_NUM)
          ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret && !marshalling_queue_.empty())
      {
        int32_t choose_num = 0;
        std::pair<uint64_t, uint32_t> result;
        const int32_t MAX_LOOP_NUM = data_member_num * 2;
        do
        {
          time_t now = Func::get_monotonic_time();
          marshallin_queue_mutex_.rdlock();
          int32_t random_index = random() % marshalling_queue_.size();
          MarshallingItem* item = marshalling_queue_.at(random_index);
          assert(item);
          result.first = INVALID_SERVER_ID;
          result.second = INVALID_BLOCK_ID;
          ret = item->choose_item_random(result);
          marshallin_queue_mutex_.unlock();
          if (TFS_SUCCESS == ret)
          {
            //这里没办法精确控制1个SERVER只做一个任务，为了保险需要要DS来做保证，NS只是尽量保证
            #ifdef TFS_GTEST
            bool valid = true;
            UNUSED(now);
            #else
            bool valid = (manager_.get_block_manager().need_marshalling(result.second, now)
                        && !manager_.get_task_manager().exist(result.second)
                        && !manager_.get_task_manager().exist(result.first)
                        && manager_.get_task_manager().has_space_do_task_in_machine(result.first));
            #endif
            if (valid)
            {
              bool exist = false;
              for (int32_t i = 0; i < members.get_array_index() && !exist; ++i)
              {
                std::pair<uint64_t, uint32_t> middle = *members.at(i);
                exist =  middle.first == result.first;
              }
              if (!exist)
              {
                members.push_back(result);
              }
            }
          }
        }
        while (members.get_array_index() < data_member_num && ++choose_num < MAX_LOOP_NUM);
      }
      return ret;
    }

    int FamilyManager::create_family_choose_check_members(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& members,
        common::ArrayHelper<ServerCollect*>& already_exist, const int32_t check_member_num)
    {
      int32_t ret = (members.get_array_size() > 0 &&  check_member_num > 0 && check_member_num <= MAX_CHECK_MEMBER_NUM)
          ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MAX_CHOOSE_NUM = check_member_num * 2;
        ServerCollect* result[MAX_CHOOSE_NUM];
        ArrayHelper<ServerCollect*> helper(MAX_CHOOSE_NUM, result);
        manager_.get_server_manager().choose_create_block_target_server(already_exist, helper, MAX_CHOOSE_NUM);
        ret = helper.get_array_index() < check_member_num ? TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < helper.get_array_index(); ++index)
          {
            ServerCollect* server = *helper.at(index);
            assert(server);
            if (manager_.get_task_manager().has_space_do_task_in_machine(server->id()))
            {
              uint32_t block = manager_.get_alive_block_id();
              members.push_back(std::make_pair(server->id(), block));
            }
          }
        }
      }
      return ret;
    }

    int FamilyManager::reinstate_family_choose_members(common::ArrayHelper<uint64_t>& results,
        const int64_t family_id, const int32_t member_num)
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && results.get_array_size() > 0
                    && member_num > 0 && member_num <= MAX_MARSHLLING_NUM) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::pair<BlockCollect*, ServerCollect*> members[MAX_MARSHLLING_NUM];
        common::ArrayHelper<std::pair<BlockCollect*, ServerCollect*> > helper(MAX_MARSHLLING_NUM, members);
        ret = get_members(helper, family_id);
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* servers[MAX_MARSHLLING_NUM];
          common::ArrayHelper<ServerCollect*> helper2(MAX_MARSHLLING_NUM, servers);
          for (int32_t i = 0; i < helper.get_array_index(); ++i)
          {
            std::pair<BlockCollect*, ServerCollect*> item = *helper.at(i);
            helper2.push_back(item.second);
          }
          ServerCollect* choose_results[member_num];
          ArrayHelper<ServerCollect*> choose_result_helper(member_num, choose_results);
          manager_.get_server_manager().choose_create_block_target_server(helper2, choose_result_helper, member_num);
          ret = choose_result_helper.get_array_index() == member_num ? TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            for (int32_t i = 0; i < choose_result_helper.get_array_index(); ++i)
            {
              ServerCollect* server = *choose_result_helper.at(i);
              assert(server);
              results.push_back(server->id());
            }
          }
        }
      }
      return ret;
    }

    int FamilyManager::dissolve_family_choose_member_targets_server(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& results,
        const int64_t family_id)
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && results.get_array_size() > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::pair<BlockCollect*, ServerCollect*> members[MAX_MARSHLLING_NUM];
        common::ArrayHelper<std::pair<BlockCollect*, ServerCollect*> > helper(MAX_MARSHLLING_NUM, members);
        ret = get_members(helper, family_id);
        if (TFS_SUCCESS == ret)
        {
          ServerCollect* target = NULL;
          ServerCollect* servers[SYSPARAM_NAMESERVER.max_replication_];
          common::ArrayHelper<ServerCollect*> helper2(SYSPARAM_NAMESERVER.max_replication_, servers);
          for (int32_t i = 0; i < helper.get_array_index(); ++i)
          {
            target = NULL;
            helper2.clear();
            std::pair<BlockCollect*, ServerCollect*> item = *helper.at(i);
            helper2.push_back(item.second);
            ret = manager_.get_server_manager().choose_replicate_target_server(target, helper2);
            if (TFS_SUCCESS == ret)
            {
              results.push_back(std::make_pair(target->id(), item.first->id()));
            }
          }
        }
      }
      return ret;
    }

    bool FamilyManager::check_need_reinstate(common::ArrayHelper<std::pair<uint32_t, int32_t> >& reinstate_members,
          const FamilyCollect* family, const time_t now) const
    {
      int32_t ret = (NULL != family && reinstate_members.get_array_size() > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockCollect* blocks[MAX_MARSHLLING_NUM];
        common::ArrayHelper<BlockCollect*> helper(MAX_MARSHLLING_NUM, blocks);
        ret = get_members(helper, family->get_family_id());
        if (TFS_SUCCESS == ret)
        {
          for (int32_t i = 0; i < helper.get_array_index(); ++i)
          {
            BlockCollect* block = *helper.at(i);
            assert(block);
            if (manager_.get_block_manager().need_reinstate(block, now))
            {
              reinstate_members.push_back(std::make_pair(block->id(), block->version()));
            }
          }
        }
      }
      return reinstate_members.get_array_index() > 0;
    }

    bool FamilyManager::check_need_dissolve(const FamilyCollect* family, const common::ArrayHelper<std::pair<uint32_t, int32_t> >& need_reinstate_members) const
    {
      return (NULL != family) ? need_reinstate_members.get_array_index() > family->get_check_member_num() : false;
    }

    bool FamilyManager::check_need_compact() const
    {
      //TODO
      return false;
    }

    FamilyCollect* FamilyManager::get_(const int64_t family_id) const
    {
      FamilyCollect* family = NULL;
      int32_t ret = (INVALID_FAMILY_ID != family_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = get_chunk_(family_id);
        FamilyCollect query(family_id);
        FAMILY_MAP_ITER iter = families_[index]->find(&query);
        family = families_[index]->end() == iter ? NULL : (*iter);
      }
      return family;
    }

    int FamilyManager::insert_(const int64_t family_id, const int32_t family_aid_info,
          const common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const time_t now)
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && members.get_array_index() > 0) ?  TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        FamilyCollect* result = NULL;
        FamilyCollect* family = new (std::nothrow)FamilyCollect(family_id, family_aid_info, now);
        assert(family);
        ret = family->add(members);
        if (TFS_SUCCESS == ret)
        {
          ret = families_[get_chunk_(family_id)]->insert_unique(result, family);
          if (EXIT_ELEMENT_EXIST == ret)
            assert(result);
        }
        if (TFS_SUCCESS != ret)
          tbsys::gDelete(family);
      }
      return ret;
    }

    int FamilyManager::get_members_(common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const int64_t family_id) const
    {
      int32_t ret = (INVALID_FAMILY_ID != family_id && members.get_array_size() > 0 ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        FamilyCollect* family = get_(family_id);
        ret = NULL != family ? TFS_SUCCESS : EXIT_NO_FAMILY;
        if (TFS_SUCCESS == ret)
        {
          family->get_members(members);
        }
      }
      return ret;
    }

    common::RWLock& FamilyManager::get_mutex_(const int64_t family_id) const
    {
      return mutexs_[get_chunk_(family_id)];
    }

    int32_t FamilyManager::get_chunk_(const int64_t family_id) const
    {
      return family_id % MAX_FAMILY_CHUNK_NUM;
    }

    bool FamilyManager::has_marshalling_(int32_t& current_version, const int64_t family_id, const uint32_t block, const int32_t version) const
    {
      bool ret = (INVALID_FAMILY_ID != family_id && INVALID_BLOCK_ID != block && version > 0);
      if (ret)
      {
        FamilyCollect* family = get_(family_id);
        ret = (NULL != family);
        if (ret)
        {
          ret = family->exist(current_version, block, version);
        }
      }
      return ret;
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/
