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
 *      - initial repserver
 *
 */

#include "global_factory.h"
#include "block_collect.h"
#include "server_collect.h"
#include "server_manager.h"
#include "layout_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace nameserver
  {
   ServerManager::ServerManager(LayoutManager& manager):
      servers_(MAX_PROCESS_NUMS, 1024, 0.1),
      wait_free_servers_(MAX_PROCESS_NUMS / 4, 128, 0.1),
      manager_(manager),
      last_traverse_server_(0),
      wait_free_wait_time_(SYSPARAM_NAMESERVER.object_wait_free_time_ms_),
      wait_clear_wait_time_(common::SYSPARAM_NAMESERVER.replicate_wait_time_),
      write_index_(0)
    {
      memset(&global_stat_info_, 0, sizeof(global_stat_info_));
    }

    ServerManager::~ServerManager()
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        this->free(*iter);
      }
      iter = wait_free_servers_.begin();
      for (; iter != wait_free_servers_.end(); ++iter)
      {
        this->free(*iter);
      }
    }

    int ServerManager::apply(const DataServerStatInfo& info, const int64_t now, const int32_t times)
    {
      bool reset = false;
      rwmutex_.wrlock();
      ServerCollect* pserver = get_(info.id_);
      int32_t ret = (NULL == pserver) ? common::TFS_SUCCESS : EXIT_APPLY_LEASE_ALREADY_ISSUED;
      if (common::TFS_SUCCESS == ret)
      {
        ServerCollect* result = NULL;
        ServerCollect query(info.id_);
        SERVER_TABLE_ITER iter  = wait_free_servers_.find(&query);
        pserver = wait_free_servers_.end() == iter ? NULL : (*iter);
        reset = (NULL != pserver && (OBJECT_WAIT_FREE_PHASE_CLEAR == pserver->get_wait_free_phase()));
        if (reset)
        {
          wait_free_servers_.erase(&query);
        }
        else
        {
          pserver = this->malloc(info, now);
        }
        assert(NULL != pserver);
        ret = servers_.insert_unique(result, pserver);
        assert(common::TFS_SUCCESS == ret);
        assert(NULL != result);
      }
      rwmutex_.unlock();

      if (common::TFS_SUCCESS == ret)
      {
        assert(NULL != pserver);
        pserver->set(now, times);
      }

      if (common::TFS_SUCCESS == ret)
      {
        ret = pserver->has_valid_lease(now) ? common::TFS_SUCCESS : EXIT_LEASE_EXPIRED;
      }

      if (common::TFS_SUCCESS == ret)
      {
        if (reset)
        {
          pserver->reset(info, now, manager_);
        }
        pserver->set_status(SERVICE_STATUS_ONLINE);
      }
      return ret;
    }

    int ServerManager::giveup(const int64_t now, const uint64_t server)
    {
      rwmutex_.wrlock();
      ServerCollect* pserver = get_(server);
      int32_t ret = (NULL == pserver) ? EXIT_LEASE_NOT_EXIST : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ServerCollect* result = NULL;
        ret = wait_free_servers_.insert_unique(result, pserver);
        assert(common::TFS_SUCCESS == ret);
        assert(NULL != result);
        servers_.erase(pserver);
      }
      rwmutex_.unlock();
      if (common::TFS_SUCCESS == ret)
      {
        int32_t args = CALL_BACK_FLAG_PUSH|CALL_BACK_FLAG_CLEAR;
        pserver->set_status(SERVICE_STATUS_OFFLINE);
        pserver->set_wait_free_phase(OBJECT_WAIT_FREE_PHASE_CLEAR);
        pserver->set(now, wait_clear_wait_time_);
        pserver->callback(reinterpret_cast<void*>(&args), manager_);
      }
      return ret;
    }

    int ServerManager::renew(const DataServerStatInfo& info, const int64_t now, const int32_t times)
    {
      assert(times > 0);
      rwmutex_.rdlock();
      ServerCollect* pserver = get_(info.id_);
      int32_t ret = (NULL == pserver) ? EXIT_LEASE_NOT_EXIST : common::TFS_SUCCESS;
      if (common::TFS_SUCCESS == ret)
      {
        ret = pserver->has_valid_lease(now) ? common::TFS_SUCCESS : EXIT_LEASE_EXPIRED;
      }
      rwmutex_.unlock();
      if (common::TFS_SUCCESS == ret)
      {
        pserver->renew(info, now, times);
      }
      return ret;
    }

    bool ServerManager::has_valid_lease(const int64_t now, const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      ServerCollect* pserver = get_(server);
      return (NULL == pserver) ? false : pserver->has_valid_lease(now);
    }

    ServerCollect* ServerManager::get(const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return get_(server);
    }

    int64_t ServerManager::size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return servers_.size();
    }

    int64_t ServerManager::wait_free_size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return wait_free_servers_.size();
    }

    int ServerManager::timeout(const int64_t now, NsGlobalStatisticsInfo& stat_info,
        ArrayHelper<ServerCollect*>& report_block_servers, uint64_t& last_traverse_server, bool& all_over)
    {
      ServerCollect* pserver = NULL;
      int32_t expire_count = 0;
      const int32_t MAX_TRAVERSE_COUNT = 256;
      ServerCollect* entry[MAX_TRAVERSE_COUNT];
      ArrayHelper<ServerCollect*> helper(MAX_TRAVERSE_COUNT, entry);
      all_over = traverse(last_traverse_server, servers_, helper);
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        pserver = *helper.at(index);
        assert(NULL != pserver);
        last_traverse_server = pserver->id();
        if (pserver->expire(now))
        {
          ++expire_count;
          giveup(now, last_traverse_server);
        }
        else
        {
          pserver->statistics(stat_info);
          if (pserver->is_report_block(now))
          {
            report_block_servers.push_back(pserver);
          }
        }
      }
      if (all_over)
        global_stat_info_ = stat_info;
      return expire_count;
    }

    int ServerManager::gc(const int64_t now)
    {
      int32_t free_count = 0;
      const int32_t MAX_GC_COUNT = 8;
      ServerCollect* pserver= NULL;
      ServerCollect* entry[MAX_GC_COUNT];
      ArrayHelper<ServerCollect*> helper(MAX_GC_COUNT, entry);
      rwmutex_.rdlock();
      SERVER_TABLE_ITER iter = wait_free_servers_.begin();
      for (; iter != wait_free_servers_.end() && helper.get_array_index() < MAX_GC_COUNT; ++iter)
      {
        pserver = (*iter);
        assert(NULL != pserver);
        if (pserver->expire(now))
          helper.push_back(pserver);
      }
      rwmutex_.unlock();

      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        pserver = *helper.at(index);
        assert(NULL != pserver);
        if (OBJECT_WAIT_FREE_PHASE_CLEAR == pserver->get_wait_free_phase())
        {
          int32_t args = CALL_BACK_FLAG_PUSH|CALL_BACK_FLAG_CLEAR;
          pserver->callback(reinterpret_cast<void*>(&args), manager_);
          pserver->set_wait_free_phase(OBJECT_WAIT_FREE_PHASE_FREE);
          pserver->set(now, wait_free_wait_time_);
        }
        else
        {
          ++free_count;
          rwmutex_.wrlock();
          wait_free_servers_.erase(pserver);
          rwmutex_.unlock();
          pserver->free();
        }
      }
      return free_count;
    }

    bool ServerManager::traverse(const uint64_t start, const SERVER_TABLE& table, ArrayHelper<ServerCollect*>& servers) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return for_each_(start, table, servers);
    }

    ServerCollect* ServerManager::malloc(const common::DataServerStatInfo& info, const int64_t now)
    {
      return new (std::nothrow) ServerCollect(info, now);
    }

    void ServerManager::free(ServerCollect* pserver)
    {
      if (NULL != pserver)
        pserver->free();
    }

    int ServerManager::apply_block(const uint64_t server, common::ArrayHelper<BlockLease>& output)
    {
      int32_t ret = (INVALID_SERVER_ID == server) ? EXIT_SERVER_ID_INVALID_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int64_t now = Func::get_monotonic_time();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.in_apply_block_safe_mode_time(now) ? EXIT_APPLY_BLOCK_SAFE_MODE_TIME_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = DATASERVER_DISK_TYPE_FULL == pserver->get_disk_type() ? TFS_SUCCESS : EXIT_DATASERVER_READ_ONLY;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = pserver->apply_block(manager_, output);
        }
      }
      return ret;
    }

    int ServerManager::apply_block_for_update(const uint64_t server, common::ArrayHelper<common::BlockLease>& output)
    {
      int32_t ret = (INVALID_SERVER_ID == server) ? EXIT_SERVER_ID_INVALID_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int64_t now = Func::get_monotonic_time();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.in_apply_block_safe_mode_time(now) ? EXIT_APPLY_BLOCK_SAFE_MODE_TIME_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = pserver->apply_block_for_update(manager_, output);
        }
      }
      return ret;
    }

    int ServerManager::renew_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<common::BlockLease>& output)
    {
      int32_t ret = (INVALID_SERVER_ID == server && output.get_array_size() >= input.get_array_size()) ? EXIT_SERVER_ID_INVALID_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = pserver->renew_block(input, manager_, output);
        }
      }
      return ret;
    }

    int ServerManager::giveup_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<common::BlockLease>& output)
    {
      int32_t ret = (INVALID_SERVER_ID == server && output.get_array_size() >= input.get_array_size()) ? EXIT_SERVER_ID_INVALID_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = pserver->giveup_block(input, manager_, output);
        }
      }
      return ret;
    }

    void ServerManager::scan(const int32_t flag, tbnet::DataBuffer& output)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for ( ; iter != servers_.end(); iter++)
      {
        if (flag & DATASERVER_TYPE_FULL)
        {
          if (DATASERVER_DISK_TYPE_FULL !=  (*iter)->get_disk_type())
            continue;
        }
        else if (flag & DATASERVER_TYPE_SYSTEM)
        {
          if (DATASERVER_DISK_TYPE_SYSTEM !=  (*iter)->get_disk_type())
            continue;
        }
        output.writeInt64((*iter)->id());
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

    bool ServerManager::get_range_servers(const uint64_t begin, ArrayHelper<ServerCollect*>& result) const
    {
      return traverse(begin, this->servers_, result);
    }

    void ServerManager::set_all_server_next_report_time(const int32_t flag , const time_t now)
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      SERVER_TABLE_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        assert(NULL != (*iter));
        (*iter)->set_next_report_block_time(now, random() % 0xFFFFFFF, flag);
      }
    }

    int ServerManager::build_relation(ServerCollect* server, const uint64_t block,
        const bool writable, const bool master)
    {
      int32_t ret = ((NULL != server) && (INVALID_BLOCK_ID != block)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //build relation between dataserver and block
        //add to dataserver's all kind of list
        ret = server->add(block, master, writable);
        if (EXIT_ELEMENT_EXIST == ret)
          ret = TFS_SUCCESS;
      }
      return ret;
    }

    int ServerManager::relieve_relation(ServerCollect* server, const uint64_t block)
    {
      return ((NULL != server) && (INVALID_SERVER_ID != block)) ? server->remove(block) : EXIT_PARAMETER_ERROR;
    }

    int ServerManager::relieve_relation(const uint64_t server, const uint64_t block)
    {
      int32_t ret = ((INVALID_SERVER_ID != server) && (INVALID_BLOCK_ID != block)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ServerCollect* pserver = get(server);
        ret = (NULL != pserver) ? TFS_SUCCESS : EIXT_SERVER_OBJECT_NOT_FOUND;
        if (TFS_SUCCESS == ret)
          ret = relieve_relation(pserver, block);
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
      uint64_t server = 0;
      const int32_t MAX_SLOT_NUMS = 32;
      ServerCollect* pserver = NULL;
      ServerCollect* servers[MAX_SLOT_NUMS];
      ArrayHelper<ServerCollect*> helper(MAX_SLOT_NUMS, servers);
      std::multimap<int64_t, ServerCollect*> outside;

      do
      {
        helper.clear();
        complete = get_range_servers(server, helper);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          pserver = *helper.at(index);
          assert(NULL != pserver);
          server = pserver->id();
          has_move = pserver->is_alive()
            && !manager_.get_task_manager().exist_server(pserver->id())
            && manager_.get_task_manager().has_space_do_task_in_machine(pserver->id());
          if (has_move)
          {
            move_split_servers_(source, outside,targets, pserver, percent);
          }
        }
      }
      while (!complete);

      if (!targets.empty() && source.empty())
      {
        source = outside;
      }
      return TFS_SUCCESS;
    }

    //choose one or more servers to create new block
    int ServerManager::choose_create_block_target_server(common::ArrayHelper<uint64_t>& result,
        common::ArrayHelper<uint64_t>& news, const int32_t count) const
    {
      news.clear();
      std::set<uint32_t> lans;
      get_lans_(lans, result);
      ServerCollect* pserver = NULL;
      int32_t index = count;
      while (index-- > 0)
      {
        pserver = NULL;
        if (TFS_SUCCESS == choose_replciate_random_choose_server_base_lock_(pserver, result, lans))
        {
          assert(NULL != pserver);
          news.push_back(pserver->id());
          result.push_back(pserver->id());
          uint32_t lan =  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
          lans.insert(lan);
        }
      }
      return count - news.get_array_index();
    }

    //replicate method
    //choose a server to replicate or move
    int ServerManager::choose_replicate_source_server(ServerCollect*& result, const ArrayHelper<uint64_t>& source) const
    {
      result = NULL;
      int32_t ret = !source.empty() ? TFS_SUCCESS : EXIT_NO_DATASERVER;
      if (TFS_SUCCESS == ret)
      {
        int64_t size = source.get_array_index(), index = source.get_array_index();
        while (index-- > 0 && NULL == result)
        {
          int32_t random_index = random() % size;
          uint64_t server = *source.at(random_index);
          assert(INVALID_SERVER_ID != server);
          ServerCollect* pserver = get(server);
          if ((NULL != pserver) && !manager_.get_task_manager().exist_server(server)
              && (manager_.get_task_manager().has_space_do_task_in_machine(server, false)))
          {
            result = pserver;
          }
        }
      }
      return NULL != result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::choose_replicate_target_server(ServerCollect*& result,
        const ArrayHelper<uint64_t>& except) const
    {
      result = NULL;
      std::set<uint32_t> lans;
      get_lans_(lans, except);
      int32_t ret = choose_replciate_random_choose_server_extend_lock_(result, except, lans);
      return ((ret == TFS_SUCCESS) && (NULL != result)) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    //move method
    int ServerManager::choose_move_target_server(ServerCollect*& result, SERVER_TABLE& sources,
        common::ArrayHelper<uint64_t>& except) const
    {
      result = NULL;
      std::set<uint32_t> lans;
      get_lans_(lans, except);
      ServerCollect* pserver = NULL;
      int64_t size = sources.size(), index = sources.size(), random_index = 0;
      while (index--> 0 && NULL == result)
      {
        random_index = random() % size;
        pserver = sources.at(random_index);
        assert(NULL != pserver);
        uint32_t lan =  Func::get_lan(pserver->id(), SYSPARAM_NAMESERVER.group_mask_);
        TBSYS_LOG(DEBUG, "==============addr: %s, lans : %u", tbsys::CNetUtil::addrToString(pserver->id()).c_str(), lan);
        if (manager_.get_task_manager().has_space_do_task_in_machine(pserver->id(), true)
            && !manager_.get_task_manager().exist_server(pserver->id())
            && lans.find(lan) == lans.end()
            && DATASERVER_DISK_TYPE_FULL == pserver->get_disk_type())
        {
          lans.insert(lan);
          sources.erase(pserver);
          result = pserver;
        }
      }
      return (NULL != result) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::choose_excess_backup_server(ServerCollect*& result, const common::ArrayHelper<uint64_t>& sources) const
    {
      result = NULL;
      SORT_MAP sorts;
      GROUP_MAP group, servers;
      for (int64_t index = 0; index < sources.get_array_index(); ++index)
      {
        uint64_t id = *sources.at(index);
        assert(INVALID_SERVER_ID != id);
        ServerCollect* server = get(id);
        if ((NULL != server) && server->total_capacity() > 0)
        {
          int64_t use = static_cast<int64_t>(calc_capacity_percentage(server->use_capacity(),
                server->total_capacity()) *  PERCENTAGE_MAGIC);
          sorts.insert(SORT_MAP::value_type(use, server));
          uint32_t lan = Func::get_lan(server->id(), SYSPARAM_NAMESERVER.group_mask_);
          GROUP_MAP_ITER iter = servers.find(server->id());
          if (servers.end() == iter)
            iter = servers.insert(GROUP_MAP::value_type(server->id(), SORT_MAP())).first;
          iter->second.insert(SORT_MAP::value_type(use, server));
          iter = group.find(lan);
          if (group.end() == iter)
            iter = group.insert(GROUP_MAP::value_type(lan, SORT_MAP())).first;
          iter->second.insert(SORT_MAP::value_type(use, server));
        }
      }

      if (0 == SYSPARAM_NAMESERVER.group_mask_)
      {
        result = sorts.empty() ? NULL : sorts.rbegin()->second;
      }
      else
      {
        GROUP_MAP_ITER iter = servers.begin();
        for (; iter != servers.end() && NULL == result; ++iter)
        {
          if (iter->second.size() > 1u)
            result = iter->second.rbegin()->second;
        }
        if (NULL == result)
        {
          uint32_t nums = 0;
          GROUP_MAP_ITER iter = group.begin();
          for (; iter != group.end(); ++iter)
          {
            if (iter->second.size() > nums)
            {
              nums = iter->second.size();
              result = iter->second.rbegin()->second;
            }
          }
        }
        if (NULL == result)
        {
          result = sorts.empty() ? NULL : sorts.rbegin()->second;
        }
      }
      return NULL != result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::expand_ratio(int32_t& index, const float expand_ratio)
    {
      ServerCollect* server = NULL;
      rwmutex_.rdlock();
      if (!servers_.empty())
      {
        if (index >= servers_.size())
          index = 0;
        server = servers_.at(index);
        ++index;
      }
      rwmutex_.unlock();
      return (NULL != server) ? server->expand_ratio(expand_ratio) : TFS_SUCCESS;
    }

    int ServerManager::calc_single_process_max_network_bandwidth(int32_t& max_mr_network_bandwith,
        int32_t& max_rw_network_bandwith, const DataServerStatInfo& info) const
    {
      UNUSED(info);
      /*if (info.total_network_bandwith_ > 0)
        {
        int32_t capacity = info.total_network_bandwith_ / 12;
        max_mr_network_bandwith = capacity * SYSPARAM_NAMESERVER.max_mr_network_bandwith_ratio_ / 100;
        max_rw_network_bandwith = capacity * SYSPARAM_NAMESERVER.max_rw_network_bandwith_ratio_ / 100;
        }*/

      int32_t capacity = SYSPARAM_NAMESERVER.max_single_machine_network_bandwith_ / 12;
      max_mr_network_bandwith = capacity * SYSPARAM_NAMESERVER.max_mr_network_bandwith_ratio_ / 100;
      max_rw_network_bandwith = capacity * SYSPARAM_NAMESERVER.max_rw_network_bandwith_ratio_ / 100;
      max_mr_network_bandwith = std::max(max_mr_network_bandwith, 2);
      max_rw_network_bandwith = std::max(max_rw_network_bandwith, 4);
      return TFS_SUCCESS;
    }

    int ServerManager::regular_create_block_for_servers(uint64_t& begin, bool& all_over)
    {
      const int32_t MAX_SLOT_NUMS = 32;
      ServerCollect* server = NULL;
      ServerCollect* servers[MAX_SLOT_NUMS];
      ArrayHelper<ServerCollect*> helper(MAX_SLOT_NUMS, servers);
      all_over = traverse(begin, servers_, helper);
      bool promote = true;
      int32_t count = SYSPARAM_NAMESERVER.add_primary_block_count_;
      double total_capacity = global_stat_info_.total_capacity_ <= 0
              ? 1 : global_stat_info_.total_capacity_;
      double use_capacity = global_stat_info_.use_capacity_ <= 0
        ? 0 : global_stat_info_.use_capacity_;
      if (total_capacity <= 0)
        total_capacity = 1;
      double average_used_capacity = use_capacity / total_capacity;
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        server = *helper.at(index);
        begin  = server->id();
        assert(NULL != server);
        int32_t ret = TFS_SUCCESS;
        count = SYSPARAM_NAMESERVER.add_primary_block_count_;
        server->calc_regular_create_block_count(average_used_capacity, manager_,promote, count);
        int64_t now = Func::get_monotonic_time();
        while (TFS_SUCCESS == ret && GFactory::get_runtime_info().is_master() && count-- > 0)
        {
          uint64_t new_block_id = INVALID_BLOCK_ID;
          BlockCollect* pblock = manager_.add_new_block(new_block_id, server, now);
          ret = (pblock != NULL) ? TFS_SUCCESS : EXIT_ADD_NEW_BLOCK_ERROR;
        }
      }
      return TFS_SUCCESS;
    }

    ServerCollect* ServerManager::get_(const uint64_t server) const
    {
      ServerCollect query(server);
      SERVER_TABLE_ITER iter = servers_.find(&query);
      return (servers_.end() != iter && (*iter)->is_alive()) ? (*iter) : NULL;
    }

    void ServerManager::get_lans_(std::set<uint32_t>& lans, const common::ArrayHelper<uint64_t>& source) const
    {
      uint64_t server = INVALID_SERVER_ID;
      for (int64_t index = 0; index < source.get_array_index(); ++index)
      {
        server = *source.at(index);
        assert(INVALID_SERVER_ID != server);
        uint32_t lan =  Func::get_lan(server, SYSPARAM_NAMESERVER.group_mask_);
        TBSYS_LOG(DEBUG, "addr: %s, lans : %u", tbsys::CNetUtil::addrToString(server).c_str(), lan);
        lans.insert(lan);
      }
    }

    void ServerManager::move_split_servers_(std::multimap<int64_t, ServerCollect*>& source,
        std::multimap<int64_t, ServerCollect*>& outside,
        SERVER_TABLE& targets, const ServerCollect* server, const double percent) const
    {
      if (NULL != server)
      {
        const uint64_t current_total_capacity = server->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;
        double current_percent = calc_capacity_percentage(server->use_capacity(), current_total_capacity);
        TBSYS_LOG(DEBUG, "move_split_server: %s, current_percent: %e, balance_percent: %e, percent: %e",
            tbsys::CNetUtil::addrToString(server->id()).c_str(), current_percent, SYSPARAM_NAMESERVER.balance_percent_, percent);
        if (current_percent < percent - SYSPARAM_NAMESERVER.balance_percent_)
        {
          targets.insert(const_cast<ServerCollect*>(server));
        }
        else if ((current_percent > (percent + SYSPARAM_NAMESERVER.balance_percent_))
            || (current_percent >= 1.0))
        {
          source.insert(std::multimap<int64_t, ServerCollect*>::value_type(
                static_cast<int64_t>(current_percent * PERCENTAGE_MAGIC), const_cast<ServerCollect*>(server)));
          TBSYS_LOG(DEBUG, "move_split_server: %s, %ld",
              tbsys::CNetUtil::addrToString(server->id()).c_str(), static_cast<int64_t>(current_percent * PERCENTAGE_MAGIC));
        }
        else
        {
          outside.insert(std::multimap<int64_t, ServerCollect*>::value_type(
                static_cast<int64_t>(current_percent * PERCENTAGE_MAGIC), const_cast<ServerCollect*>(server)));
        }
      }
    }

    /*int ServerManager::choose_writable_block(BlockCollect*& result)
    {
      result = NULL;
      rwmutex_.rdlock();
      int64_t index = 0, count = servers_.size();
      rwmutex_.unlock();
      int32_t ret = TFS_SUCCESS;
      ServerCollect* server = NULL;
      for (index= 0; index < count && NULL == result; ++index)
      {
        result = NULL;
        ret = choose_writable_server_lock_(server);
        if (TFS_SUCCESS != ret || NULL == server)
          continue;
        ret = server->choose_writable_block(manager_, result);
      }

      if (NULL == result)
      {
        for (index = 0; index < count && NULL == result; ++index)
        {
          result = NULL;
          ret = choose_writable_server_random_lock_(server);
          if (TFS_SUCCESS != ret || NULL == server)
            continue;
          ret = server->choose_writable_block(manager_, result);
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
      return NULL != result ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }*/

    int ServerManager::choose_replciate_random_choose_server_base_lock_(ServerCollect*& result,
        const common::ArrayHelper<uint64_t>& except, const std::set<uint32_t>& lans) const
    {
      result = NULL;
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int64_t size = std::min(servers_.size(), SYSPARAM_NAMESERVER.choose_target_server_random_max_nums_);
      int64_t index = size, random_index = 0;
      while (index-- > 0 && NULL == result)
      {
        random_index = random() % servers_.size();
        ServerCollect* pserver = servers_.at(random_index);
        assert(NULL != pserver);
        bool valid  = ((!pserver->is_full()) && (!except.exist(pserver->id()))
                      && (DATASERVER_DISK_TYPE_FULL == pserver->get_disk_type()));
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
      return (NULL != result) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    int ServerManager::choose_replciate_random_choose_server_extend_lock_(ServerCollect*& result,
        const common::ArrayHelper<uint64_t>& except, const std::set<uint32_t>& lans) const
    {
      rwmutex_.rdlock();
      int64_t index = std::min(servers_.size(), SYSPARAM_NAMESERVER.choose_target_server_retry_max_nums_);
      rwmutex_.unlock();
      int32_t ret = TFS_SUCCESS;
      ServerCollect* server = NULL;
      while (index--> 0 && NULL == result)
      {
        ret = choose_replciate_random_choose_server_base_lock_(server, except, lans);
        if (TFS_SUCCESS != ret)
          continue;

        assert(NULL != server);
        if (!manager_.get_task_manager().exist_server(server->id())
            && manager_.get_task_manager().has_space_do_task_in_machine(server->id(), true))
        {
          result = server;
        }
      }
      return (NULL != result) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
    }

    bool ServerManager::for_each_(const uint64_t start, const SERVER_TABLE& table, ArrayHelper<ServerCollect*>& servers) const
    {
      ServerCollect query(start);
      SERVER_TABLE_ITER iter = INVALID_LEASE_ID == start ? table.begin() : table.upper_bound(&query);
      for (; iter != table.end() && servers.get_array_index() < servers.get_array_size(); ++iter)
      {
        servers.push_back((*iter));
      }
      return servers.get_array_index() < servers.get_array_size();
    }
#ifdef TFS_GTEST
    void ServerManager::clear_()
    {
      servers_.clear();
    }
#endif
 }/** nameserver **/
}/** tfs **/
