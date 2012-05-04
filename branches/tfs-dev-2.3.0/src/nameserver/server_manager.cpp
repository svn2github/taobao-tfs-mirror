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

#include "server_collect.h"
#include "server_manager.h"
#include "layout_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
    ServerManager::ServerManager(LayoutManager& manager):
      manager_(manager),
      servers_(MAX_PROCESS_NUMS, 1024, 0.1),
      dead_servers_(MAX_PROCESS_NUMS, 1024, 0.1),
      write_index_(0)
    {

    }

    ServerManager::~ServerManager()
    {
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        tbsys::gDelete((*iter));
      }
      iter = dead_servers_.begin();
      for (; iter != dead_servers_.end(); ++iter)
      {
        tbsys::gDelete((*iter));
      }
    }

    int ServerManager::add(const DataServerStatInfo& info, const time_t now, bool& isnew)
    {
      bool reset = false;
      ServerCollect query(info.id_);

      rwmutex_.wrlock();
      ServerCollect* server = NULL;
      SERVER_TABLE_ITER iter = servers_.find(&query);
      bool alive = iter != servers_.end();
      if (!alive)
      {
        isnew = true;
        iter  = dead_servers_.find(&query);
        if (iter != dead_servers_.end())
        reset = iter != dead_servers_.end();
        if (reset)
        {
          server = (*iter);
          dead_servers_.erase(&query);
        }
        else
        {
          server = new (std::nothrow)ServerCollect(info, now);
        }
      }
      else
      {
        server = (*iter);
      }
      assert(NULL != server);
      if (isnew)
      {
        ServerCollect* result = NULL;
        bool ret = servers_.insert_unique(result, server);
        assert(ret);
        assert(NULL != result);
      }
      rwmutex_.unlock();

      if (reset)
        server->reset(manager_, info, now);
      else
        server->update(info, now, isnew);

      if (isnew)
      {
        std::vector<stat_int_t> stat(1, server->block_count());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat);
      }

      //update global statistic information
      GFactory::get_global_info().update(info, isnew);
      GFactory::get_global_info().dump(TBSYS_LOG_LEVEL_DEBUG);
      return TFS_SUCCESS;
    }

    int ServerManager::remove(const uint64_t server, const time_t now)
    {
      TBSYS_LOG(INFO, "dataserver: %s exit", tbsys::CNetUtil::addrToString(server).c_str());
      ServerCollect query(server);
      ServerCollect* object = NULL;
      rwmutex_.wrlock();
      SERVER_TABLE_ITER iter = servers_.find(&query);
      if (servers_.end() != iter)
      {
        object = (*iter);
        servers_.erase(object);
        dead_servers_.insert(object);
      }
      rwmutex_.unlock();

      if (NULL != object)
      {
        object->update_status();
        object->set_in_dead_queue_timeout(now);
        std::vector<stat_int_t> stat(1, object->block_count());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);

        //release all relations of blocks belongs to it
        relieve_relation_(object, now);

        manager_.del_report_block_server(object);
      }
      return TFS_SUCCESS;
    }

    void ServerManager::update_last_time(const uint64_t server, const time_t now)
    {
      ServerCollect* pserver = get(server);
      if (NULL != pserver)
      {
        pserver->update_last_time(now);
      }
    }

    ServerCollect* ServerManager::get(const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return get_(server);
    }

    ServerCollect* ServerManager::pop_from_dead_queue(const time_t now)
    {
      ServerCollect* result  = NULL;
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      SERVER_TABLE_ITER iter = dead_servers_.begin();
      while (iter != dead_servers_.end() && NULL == result)
      {
        if ((*iter)->is_in_dead_queue_timeout(now))
        {
          result = (*iter);
          dead_servers_.erase(result);
        }
        ++iter;
      }
      return result;
    }

    int64_t ServerManager::size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return servers_.size();
    }

    ServerCollect* ServerManager::get_(const uint64_t server) const
    {
      ServerCollect query(server);
      SERVER_TABLE_ITER iter = servers_.find(&query);
      return (servers_.end() != iter && (*iter)->is_alive()) ? (*iter) : NULL;
    }

    int ServerManager::get_dead_servers(ArrayHelper<uint64_t>& servers, NsGlobalStatisticsInfo& info, const time_t now) const
    {
      servers.clear();
      ServerCollect* pserver = NULL;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        pserver = (*iter);
        assert(NULL != pserver);
        if (!pserver->is_alive(now))
          servers.push_back(pserver->id());
        else
          pserver->statistics(info, true);
      }
      return servers.get_array_index();
    }

    int ServerManager::get_alive_servers(std::vector<uint64_t>& servers) const
    {
      servers.clear();
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        assert(NULL != (*iter));
        if ((*iter)->is_alive())
          servers.push_back((*iter)->id());
      }
      return servers.size();
    }

    bool ServerManager::get_range_servers(ArrayHelper<ServerCollect*>& result, const uint64_t begin, const int32_t count) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return get_range_servers_(result, begin, count);
    }

    bool ServerManager::get_range_servers_(ArrayHelper<ServerCollect*>& result, const uint64_t begin, const int32_t count) const
    {
      bool ret = count > 0;
      if (ret)
      {
        int32_t actual = 0;
        result.clear();
        ServerCollect query(begin);
        SERVER_TABLE_ITER iter = 0 == begin ? servers_.begin() : servers_.upper_bound(&query);
        while(iter != servers_.end() && actual < count)
        {
          result.push_back((*iter));
          actual++;
          iter++;
        }
        ret = actual < count;
      }
      return ret;
    }

    //statistic all dataserver 's information(capactiy, user_capacity, alive server nums)
    int ServerManager::move_statistic_all_server_info(int64_t& total_capacity,
        int64_t& total_use_capacity, int64_t& alive_server_nums) const
    {
      ServerCollect* pserver = NULL;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      alive_server_nums = servers_.size();
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        pserver = (*iter);
        assert(NULL !=pserver);
        total_capacity += (pserver->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_) / 100;
        total_use_capacity += pserver->use_capacity();
      }
      return TFS_SUCCESS;
    }

    int ServerManager::move_split_servers(std::multimap<int64_t, ServerCollect*>& source,
        SERVER_TABLE& targets, const double percent) const
    {
      bool complete = false;
      bool has_move = false;
      int32_t  size  = 0;
      uint64_t server = 0;
      const int32_t MAX_SLOT_NUMS = 32;
      ServerCollect* pserver = NULL;
      ServerCollect* servers[MAX_SLOT_NUMS];
      ArrayHelper<ServerCollect*> helper(MAX_SLOT_NUMS, servers);

      do
      {
        helper.clear();
        complete = get_range_servers(helper, server, MAX_SLOT_NUMS);
        size = helper.get_array_index();
        if (size > 0)
        {
          for (int32_t i = 0; i < size; i++)
          {
            pserver = *helper.at(i);
            assert(NULL != pserver);
            has_move = pserver->is_alive()
              && !manager_.get_task_manager().exist(pserver->id())
              && manager_.get_task_manager().has_space_do_task_in_machine(pserver->id());
            if (has_move)
            {
              move_split_servers_(source, targets, pserver, percent);
            }
          }
          server = pserver->id();
        }
      }
      while (!complete);
      return TFS_SUCCESS;
    }

    void ServerManager::move_split_servers_(std::multimap<int64_t, ServerCollect*>& source,
        SERVER_TABLE& targets, const ServerCollect* server, const double percent) const
    {
      if (NULL != server)
      {
        const uint64_t current_total_capacity = server->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;
        double current_percent = calc_capacity_percentage(server->use_capacity(), current_total_capacity);
        TBSYS_LOG(DEBUG, "move_split_server: %s, current_percent: %e, balance_percent: %e, percent: %e",
           tbsys::CNetUtil::addrToString(server->id()).c_str(), current_percent, SYSPARAM_NAMESERVER.balance_percent_, percent);
        if ((current_percent > (percent + SYSPARAM_NAMESERVER.balance_percent_))
            || (current_percent >= 1.0))
        {
          source.insert(std::multimap<int64_t, ServerCollect*>::value_type(
                static_cast<int64_t>(current_percent * PERCENTAGE_MAGIC), const_cast<ServerCollect*>(server)));
        TBSYS_LOG(DEBUG, "move_split_server: %s, %ld",
           tbsys::CNetUtil::addrToString(server->id()).c_str(), static_cast<int64_t>(current_percent * PERCENTAGE_MAGIC));
        }
        if (current_percent < percent - SYSPARAM_NAMESERVER.balance_percent_)
        {
          targets.insert(const_cast<ServerCollect*>(server));
        }
      }
    }

    int ServerManager::scan(common::SSMScanParameter& param, int32_t& should, int32_t& start, int32_t& next, bool& all_over) const
    {
      int32_t actual= 0;
      int16_t child_type = param.child_type_;
      if (child_type == SSM_CHILD_SERVER_TYPE_ALL)
      {
        child_type |= SSM_CHILD_SERVER_TYPE_HOLD;
        child_type |= SSM_CHILD_SERVER_TYPE_WRITABLE;
        child_type |= SSM_CHILD_SERVER_TYPE_MASTER;
        child_type |= SSM_CHILD_SERVER_TYPE_INFO;
      }
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      if (start <= servers_.size())
      {
        ServerCollect* pserver = NULL;
        uint64_t start_server = tbsys::CNetUtil::ipToAddr(param.addition_param1_, param.addition_param2_);
        ServerCollect query(start_server);
        SERVER_TABLE_ITER iter = 0 == start_server ? servers_.begin() : servers_.upper_bound(&query);
        while (servers_.end() != iter && actual < should)
        {
          pserver = (*iter);
          assert(NULL != pserver);
          pserver->scan(param, child_type);
          ++next;
          ++iter;
          ++actual;
        }
        all_over = servers_.end() == iter;
        if (!all_over)
        {
          assert(NULL != pserver);
          uint64_t host = pserver->id();
          IpAddr* addr = reinterpret_cast<IpAddr*>(&host);
          param.addition_param1_ = addr->ip_;
          param.addition_param2_ = addr->port_;
        }
        else
        {
          param.addition_param1_ = 0;
          param.addition_param2_ = 0;
        }
      }
      return actual;
    }

    void ServerManager::set_all_server_next_report_time(const time_t now)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        assert(NULL != (*iter));
        (*iter)->set_next_report_block_time(now, random() % 0xFFFFFFF, true);
      }
    }

    int ServerManager::build_relation(ServerCollect* server, const BlockCollect* block,
        const bool writable, const bool master)
    {
      int32_t ret = ((NULL != server) && (NULL != block)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //build relation between dataserver and block
        //add to dataserver's all kind of list
        server->add(block, master, writable);
      }
      return ret;
    }

    bool ServerManager::relieve_relation(ServerCollect* server, const BlockCollect* block)
    {
      bool ret = ((NULL != server) && (NULL != block));
      if (ret)
      {
        ret = server->remove(const_cast<BlockCollect*>(block));
      }
      return ret;
    }

    int ServerManager::choose_writable_block(BlockCollect*& result)
    {
      result = NULL;
      int32_t i = 0;
      rwmutex_.rdlock();
      int32_t count = servers_.size();
      rwmutex_.unlock();
      int32_t ret = TFS_SUCCESS;
      ServerCollect* server = NULL;
      for (i= 0; i < count && NULL == result; ++i)
      {
        ret = choose_writable_server_lock_(server);
        if (TFS_SUCCESS != ret || NULL == server)
          continue;
        ret = server->choose_writable_block(result);
        if (TFS_SUCCESS != ret)
          continue;
      }

      if (NULL == result)
      {
        for (i= 0; i < count && NULL == result; ++i)
        {
          ret = choose_writable_server_random_lock_(server);
          if (TFS_SUCCESS != ret || NULL == server)
            continue;
          ret = server->choose_writable_block(result);
          if (TFS_SUCCESS != ret)
            continue;
        }
      }
      return NULL == result ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
    }

    int ServerManager::choose_writable_server_lock_(ServerCollect*& result)
    {
      result = NULL;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t ret = servers_.empty() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = write_index_;
        ++write_index_;
        if (write_index_ >= servers_.size())
            write_index_ = 0;
        if (index >= servers_.size())
            index = 0;
        result = servers_.at(index);
        assert(NULL != result);
      }
      return NULL != result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::choose_writable_server_random_lock_(ServerCollect*& result)
    {
      result = NULL;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t ret = !servers_.empty() ? TFS_SUCCESS : EXIT_NO_DATASERVER;
      if (TFS_SUCCESS == ret)
      {
        result = servers_.at(random() % servers_.size());
        assert(NULL != result);
      }
      return NULL == result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    //choose one or more servers to create new block
    int ServerManager::choose_create_block_target_server(common::ArrayHelper<ServerCollect*>& result,
        common::ArrayHelper<ServerCollect*>& news, const int32_t count) const
    {
      news.clear();
      std::set<uint32_t> lans;
      get_lans_(lans, result);
      ServerCollect* pserver = NULL;
      int32_t ret = TFS_SUCCESS;
      int32_t index = count;
      while (index-- > 0)
      {
        pserver = NULL;
        ret = choose_replciate_random_choose_server_base_lock_(pserver, result, lans);
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != pserver);
          news.push_back(pserver);
          result.push_back(pserver);
          uint32_t lan =  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
          lans.insert(lan);
        }
      }
      return result.get_array_index() - count;
    }

    //replicate method
    //choose a server to replicate or move
    int ServerManager::choose_replicate_source_server(ServerCollect*& result, const ArrayHelper<ServerCollect*>& source) const
    {
      result = NULL;
      int32_t ret = !source.empty() ? TFS_SUCCESS : EXIT_NO_DATASERVER;
      if (TFS_SUCCESS == ret)
      {
        int32_t size = source.get_array_index();
        int32_t index = size;
        while (index-- > 0 && NULL == result)
        {
          int32_t random_index = random() % size;
          ServerCollect* server = *source.at(random_index);
          assert(NULL != server);
          if (manager_.get_task_manager().has_space_do_task_in_machine(server->id(), false)
              && !manager_.get_task_manager().exist(server->id()))
          {
            result = server;
          }
        }
      }
      return NULL == result ? EXIT_NO_DATASERVER : TFS_SUCCESS;
    }

    int ServerManager::choose_replicate_target_server(ServerCollect*& result,
        const ArrayHelper<ServerCollect*>& except) const
    {
      result = NULL;
      std::set<uint32_t> lans;
      get_lans_(lans, except);
      int32_t ret = choose_replciate_random_choose_server_extend_lock_(result, except, lans);
      return ((ret == TFS_SUCCESS) && (NULL != result)) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    //move method
    int ServerManager::choose_move_target_server(ServerCollect*& result, SERVER_TABLE& sources,
        common::ArrayHelper<ServerCollect*>& except) const
    {
      result = NULL;
      std::set<uint32_t> lans;
      get_lans_(lans, except);
      ServerCollect* pserver = NULL;
      int32_t size = sources.size();
      int32_t index = size, random_index = 0;
      while (index--> 0 && NULL == result)
      {
        random_index = random() % size;
        pserver = sources.at(random_index);
        assert(NULL != pserver);
        uint32_t lan =  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
        TBSYS_LOG(DEBUG, "==============addr: %s, lans : %u", tbsys::CNetUtil::addrToString(pserver->id()).c_str(), lan);
        if (manager_.get_task_manager().has_space_do_task_in_machine(pserver->id(), true)
            && !manager_.get_task_manager().exist(pserver->id())
            && lans.find(lan) == lans.end())
        {
          lans.insert(lan);
          sources.erase(pserver);
          result = pserver;
        }
      }
      return NULL == result ? EXIT_NO_DATASERVER : TFS_SUCCESS;
    }

    int ServerManager::choose_excess_backup_server(ServerCollect*& result, const common::ArrayHelper<ServerCollect*>& sources) const
    {
      result = NULL;
      SORT_MAP sorts;
      GROUP_MAP group;
      ServerCollect* server = NULL;
      for (int32_t i = 0; i < sources.get_array_index(); ++i)
      {
        server = *sources.at(i);
        assert(NULL != server);
        if (server->total_capacity() > 0)
        {
          int64_t use = static_cast<int64_t>(calc_capacity_percentage(server->use_capacity(),
                server->total_capacity()) *  PERCENTAGE_MAGIC);
          sorts.insert(SORT_MAP::value_type(use, server));
          uint32_t lan = Func::get_lan(server->id(), SYSPARAM_NAMESERVER.group_mask_);
          GROUP_MAP_ITER iter = group.find(lan);
          if (group.end() == iter)
          {
            iter = group.insert(GROUP_MAP::value_type(lan, SORT_MAP())).first;
          }
          iter->second.insert(SORT_MAP::value_type(use, server));
        }
      }

      if (0 == SYSPARAM_NAMESERVER.group_mask_)
      {
        result = sorts.empty() ? NULL : sorts.rbegin()->second;
      }
      else
      {
        GROUP_MAP_ITER iter = group.begin();
        for (; iter != group.end() && NULL == result; ++iter)
        {
          if (iter->second.size() > 0)
          {
            result = iter->second.rbegin()->second;
          }
        }
        if (NULL == result)
        {
          result = sorts.empty() ? NULL : sorts.rbegin()->second;
        }
      }
      return NULL == result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::expand_ratio(int32_t& index, const float expand_ratio)
    {
      rwmutex_.rdlock();
      if (index >= servers_.size())
        index = 0;
      ServerCollect* server = NULL;
      if (!servers_.empty())
      {
        server = servers_.at(index);
        ++index;
      }
      rwmutex_.unlock();
      return NULL != server ? server->expand_ratio(expand_ratio) : TFS_SUCCESS;
    }

    int ServerManager::choose_replciate_random_choose_server_base_lock_(ServerCollect*& result,
        const common::ArrayHelper<ServerCollect*>& except, const std::set<uint32_t>& lans) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      bool valid = false;
      result = NULL;
      ServerCollect* pserver = NULL;
      int32_t size = servers_.size();
      int32_t index = size, random_index = 0;
      while (index-- > 0 && NULL == result)
      {
        random_index = random() % size;
        pserver = servers_.at(random_index);
        assert(NULL != pserver);
        valid  = ((!pserver->is_full()) && (!except.find(pserver)));
        if (valid && !lans.empty())
        {
          uint32_t lan =  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
          valid = lans.find(lan) == lans.end();
        }

        if (valid)
        {
          result = pserver;
        }
      }
      return NULL == result ? EXIT_NO_DATASERVER : TFS_SUCCESS;
    }

    int ServerManager::choose_replciate_random_choose_server_extend_lock_(ServerCollect*& result,
        const common::ArrayHelper<ServerCollect*>& except, const std::set<uint32_t>& lans) const
    {
      rwmutex_.rdlock();
      int32_t size = servers_.size();
      rwmutex_.unlock();
      ServerCollect* server = NULL;
      int32_t index = size;
      int32_t ret = TFS_SUCCESS;
      while (index--> 0 && NULL == result)
      {
        ret = choose_replciate_random_choose_server_base_lock_(server, except, lans);
        if (TFS_SUCCESS != ret)
          continue;

        assert(NULL != server);
        if (!manager_.get_task_manager().exist(server->id())
            && manager_.get_task_manager().has_space_do_task_in_machine(server->id(), true))
        {
          result = server;
        }
      }
      return NULL == result ? EXIT_NO_DATASERVER : TFS_SUCCESS;
    }

    void ServerManager::get_lans_(std::set<uint32_t>& lans, const common::ArrayHelper<ServerCollect*>& source) const
    {
      ServerCollect* server = NULL;
      if (!source.empty())
      {
        for (int32_t i = 0; i < source.get_array_index(); ++i)
        {
          server = *source.at(i);
          assert(NULL != server);
          uint32_t lan =  Func::get_lan(server->id(), SYSPARAM_NAMESERVER.group_mask_);
          TBSYS_LOG(DEBUG, "addr: %s, lans : %u", tbsys::CNetUtil::addrToString(server->id()).c_str(), lan);
          lans.insert(lan);
        }
      }
    }

    bool ServerManager::relieve_relation_(ServerCollect* server, const time_t now)
    {
      bool ret = NULL != server;
      if (ret)
      {
        uint32_t begin = 0;
        const int32_t MAX_SLOT_NUMS = 1024;
        BlockCollect* blocks[MAX_SLOT_NUMS];
        common::ArrayHelper<BlockCollect*> helper(MAX_SLOT_NUMS, blocks);
        bool complete = false;
        BlockCollect* pblock = NULL;
        do
        {
          complete = server->get_range_blocks(helper, begin, MAX_SLOT_NUMS);
          for (int32_t i = 0; i < helper.get_array_index(); ++i)
          {
            pblock = *helper.at(i);
            assert(NULL != pblock);
            manager_.get_block_manager().relieve_relation(pblock, server, now);
          }
          if (!helper.empty())
          {
            begin = pblock->id();
          }
        }
        while (!complete);
      }
      return ret;
    }

    #ifdef TFS_GTEST
    void ServerManager::clear_()
    {
      servers_.clear();
      dead_servers_.clear();
    }
    #endif
  }/** nameserver **/
}/** tfs **/
