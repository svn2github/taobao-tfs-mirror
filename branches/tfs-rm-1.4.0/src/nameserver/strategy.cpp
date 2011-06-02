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
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <tbsys.h>
#include "strategy.h"
#include "block_collect.h"
#include "global_factory.h"
#include <numeric>
#include <set>

using namespace tfs::common;
using namespace tbsys;
using namespace std;
namespace tfs
{
  namespace nameserver
  {
    static const int8_t BASE_MULTIPLE = 2;
    template<typename T1, typename T2>
    int32_t percent(T1 v, T2 total)
    {
      if (total == 0)
        total = v == 0 ? 1 : v;
      return std::min(static_cast<int32_t> (10000 * (static_cast<double> (v) / total)), 10000);
    }

    bool check_average(int32_t current_load, int32_t total_load, int64_t use_capacity, int64_t total_use_capacity,
        int64_t alive_ds_count)
    {
      if (alive_ds_count == 0)
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "alive dataserver not found alive_ds_count(%"PRI64_PREFIX"d)", alive_ds_count);
        #endif
        return false;
      }
      int64_t average_load = total_load / alive_ds_count;
      int64_t average_use = total_use_capacity / alive_ds_count;
      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      TBSYS_LOG(DEBUG, "alive_ds_count: %"PRI64_PREFIX"d, total_load: %d, average_load: %"PRI64_PREFIX"d current_load: %d, total_use_capacity: %"PRI64_PREFIX"d,average_use: %"PRI64_PREFIX"d",
        alive_ds_count, total_load, average_load, current_load, total_use_capacity, average_use); 
      #endif

      return (((current_load < average_load * BASE_MULTIPLE) || (total_load == 0)) && ((use_capacity <= average_use * BASE_MULTIPLE)
          || (total_use_capacity == 0)));
    }

    bool BaseStrategy::check(const ServerCollect* server) const
    {
      if (!server->is_alive())
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "dataserver(%s) is dead, can't join ",
            CNetUtil::addrToString(server->id()).c_str());
        #endif
        return false;
      }

      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      TBSYS_LOG(DEBUG, "server(%s),elect_seq_num(%"PRI64_PREFIX"d)",
        tbsys::CNetUtil::addrToString(server->id()).c_str(), server->get_elect_num());
      #endif
      return check_average(server->load(), global_info_.total_load_, server->use_capacity(),
          global_info_.total_capacity_, global_info_.alive_server_count_);
    }

    void BaseStrategy::normalize(const ServerCollect* server) const
    {
      int32_t primary_writable_block_count = server->get_hold_master_size();

      primary_writable_block_count_ = percent(primary_writable_block_count, SYSPARAM_NAMESERVER.max_write_file_count_);

      //seqno_average_num_ = percent(server->get_elect_seq(), total_elect_num_);
      elect_average_num_ = percent(server->get_elect_num(), total_elect_num_);

      if (global_info_.max_block_count_ == 0)
        use_ = percent(server->use_capacity(), server->total_capacity());
      else
        use_ = percent(server->block_count(), global_info_.max_block_count_);

      if (global_info_.max_load_ < 10)
        load_ = percent(server->load(), 3000);
      else
        load_ = percent(server->load(), global_info_.max_load_);
      #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      TBSYS_LOG(DEBUG, "server(%s), elect_seq_num(%"PRI64_PREFIX"d), load(%d), use_(%d), elect_average_num_(%"PRI64_PREFIX"d), seqno_average_num_(%"PRI64_PREFIX"d)",
        tbsys::CNetUtil::addrToString(server->id()).c_str(), server->get_elect_num(), load_, use_,elect_average_num_, seqno_average_num_);
      #endif
    }

    //---------------------------------------------------------
    // WriteStrategy 
    //---------------------------------------------------------
    int64_t WriteStrategy::calc(const ServerCollect* server) const
    {
      if (server->is_full())
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "dataserver(%s) is full , can't join elect list", CNetUtil::addrToString(
            server->id()).c_str());
        #endif
        return 0;
      }
      if (!BaseStrategy::check(server))
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "server(%s) can't join elect list", CNetUtil::addrToString(server->id()).c_str());
        #endif
        return 0;
      }
      return server->get_elect_num();
    }

    // ReplicateDestStrategy
    //---------------------------------------------------------
    int64_t ReplicateDestStrategy::calc(const ServerCollect* server) const
    {
      if (server->is_full())
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "dataserver(%s) is full , can't join elect list", CNetUtil::addrToString(
            server->id()).c_str());
        #endif
        return 0;
      }
      if (!BaseStrategy::check(server))
      {
        #if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "server(%s) can't join elect list", CNetUtil::addrToString(server->id()).c_str());
        #endif
        return 0;
      }
      BaseStrategy::normalize(server);
      return elect_average_num_ * 80 + use_ * 15 + load_ * 5;
    }

    //---------------------------------------------------------
    // ReplicateSourceStrategy
    //---------------------------------------------------------
    int64_t ReplicateSourceStrategy::calc(const ServerCollect* server) const
    {
      BaseStrategy::normalize(server);
      return load_;
    }

    void dump_weigths(const DS_WEIGHT& weights)
    {
      TBSYS_LOG(INFO,"-----------------------dump_weigths(%"PRI64_PREFIX"u)----------------------\n", weights.size());
      DS_WEIGHT::const_iterator it = weights.begin();
      while (it != weights.end())
      {
        TBSYS_LOG(DEBUG,"server(%s), weight(%"PRI64_PREFIX"d), elect_seq_num(%"PRI64_PREFIX"d), total elect_seq_num(%"PRI64_PREFIX"d)\n", tbsys::CNetUtil::addrToString(it->second->id()).c_str(),
              it->first, it->second->get_elect_num(), GFactory::get_global_info().elect_seq_num_);
        ++it;
      }
      TBSYS_LOG(DEBUG, "%s", "-----------------------dump_weigths end----------------------\n");
    }

    //---------------------------------------------------------
    // elect_ds with all kinds of strategy
    //---------------------------------------------------------
    /**
     * elect_ds_exclude_group
     *
     *
     * @param [in] weights: choose dataserver weights, min to max
     * @param [in] elect_count: need dataserver count
     * @param [in] elect_seq: >0, set elect seq
     * [in,out] result: dataserver elected
     *
     * @return  elected count
     */
    //
    int32_t elect_ds_exclude_group(const DS_WEIGHT& weights, const int32_t elect_count, vector<ServerCollect*> & result)
    {
      if (elect_count == 0)
      {
        TBSYS_LOG(DEBUG, "current elect count(%d) <= 0, must be return", elect_count);
        return 0;
      }

      std::set < uint32_t > existlan;
      std::vector<ServerCollect*>::iterator it = result.begin();
      for (; it!= result.end(); ++it)
      {
        uint32_t lan = Func::get_lan((*it)->id(), SYSPARAM_NAMESERVER.group_mask_);
        existlan.insert(lan);
        (*it)->elect_num_inc();
      }

      dump_weigths(weights);

      DS_WEIGHT::const_iterator iter = weights.begin();
      int32_t need_elect_count = elect_count;
      TBSYS_LOG(DEBUG, "weights.size(%u), need_elect_count(%d)", weights.size(), need_elect_count);
      while (iter != weights.end() && need_elect_count > 0)
      {
        uint32_t dlan = Func::get_lan(iter->second->id(), SYSPARAM_NAMESERVER.group_mask_);
        if (existlan.find(dlan) == existlan.end())
        {
          --need_elect_count;
          existlan.insert(dlan);
          result.push_back(iter->second);
          iter->second->elect_num_inc();
          {
            common::RWLock::Lock lock(GFactory::get_global_info(), common::WRITE_LOCKER);
            ++GFactory::get_global_info().elect_seq_num_;
          }
        }
        ++iter;
      }
      TBSYS_LOG(DEBUG, "current elect_count(%d)", elect_count - need_elect_count);
      return elect_count - need_elect_count;
    }

    int32_t elect_ds_normal(const DS_WEIGHT& weights, const int32_t elect_count, vector<ServerCollect*> & result)
    {
      if (elect_count == 0)
        return 0;

      int32_t need_elect_count = elect_count;

      DS_WEIGHT::const_iterator iter = weights.begin();
      while (iter != weights.end() && need_elect_count > 0)
      {
        --need_elect_count;
        result.push_back(iter->second);
        //common::RWLock::Lock lock(GFactory::get_global_info(), common::WRITE_LOCKER);
        //++GFactory::get_global_info().elect_seq_num_;
        //iter->second->elect_num_inc();
        //iter->second->elect_seq_inc(GFactory::get_global_info().elect_seq_num_);
        ++iter;
      }
      return elect_count - need_elect_count;
    }

    int elect_write_server(LayoutManager& meta, const int32_t elect_count, vector<ServerCollect*> & result)
    {
      std::vector<ServerCollect*> except;
      WriteStrategy strategy(GFactory::get_global_info().get_elect_seq_num(), GFactory::get_global_info());
      return elect_ds(strategy, ExcludeGroupElectOperation(), meta, except, elect_count,false, result);
    }

    int elect_replicate_source_ds(LayoutManager& meta, vector<ServerCollect*>& source, vector<ServerCollect*>& except, int32_t elect_count, vector<ServerCollect*>& result)
    {
      vector<ServerCollect*>::const_iterator maxit = std::max_element(source.begin(), source.end(), CompareLoad());

      int32_t max_load = 1;
      if (maxit != source.end())
        max_load = (*maxit)->load();

      NsGlobalStatisticsInfo info;
      info.max_load_ = max_load; // only max_load & alive_server_count could be useful, calc.
      info.alive_server_count_ = source.size();
      // elect seq not used in this case;
      ReplicateSourceStrategy strategy(NsGlobalStatisticsInfo::ELECT_SEQ_NO_INITIALIZE, info);

      return elect_ds(strategy, NormalElectOperation(), meta, source, except, elect_count, true, result);
    }

    int elect_replicate_dest_ds(LayoutManager& meta, vector<ServerCollect*>& except, int32_t elect_count, vector<ServerCollect*> & result) 
    {
      ReplicateDestStrategy strategy(GFactory::get_global_info().get_elect_seq_num(), GFactory::get_global_info());
      return elect_ds(strategy, ExcludeGroupElectOperation(), meta, except, elect_count,true, result);
    }

    static uint32_t get_ip(uint64_t id)
    {
      return (reinterpret_cast<IpAddr*>(&id))->ip_;
    }

    bool elect_move_dest_ds(const std::set<ServerCollect*>& targets, const vector<ServerCollect*> & source, const ServerCollect* mover, ServerCollect** result)
    {
			uint64_t id = 0;
      uint32_t lan = 0;
      int32_t max_load = 1;
      *result = NULL;

      std::set<ServerCollect*>::const_iterator maxit = std::max_element(targets.begin(), targets.end(), CompareLoad());
      if (maxit != targets.end())
        max_load = (*maxit)->load();

      NsGlobalStatisticsInfo ginfo;
      ginfo.max_load_ = max_load; // only max_load & alive_server_count could be useful, calc.
      ginfo.alive_server_count_ = targets.size();
      // elect seq not used in this case;
      ReplicateSourceStrategy strategy(NsGlobalStatisticsInfo::ELECT_SEQ_NO_INITIALIZE, ginfo);

      DS_WEIGHT weights;
      StoreWeight < ReplicateSourceStrategy > store(strategy, weights);
      std::for_each(targets.begin(), targets.end(), store);

      std::set < uint32_t > existlan;
      //std::set < ServerCollect*> exist_server;
      std::vector<ServerCollect*>::const_iterator s_iter = source.begin();
      for (; s_iter != source.end(); ++s_iter)
      {
        lan = Func::get_lan((*s_iter)->id(), SYSPARAM_NAMESERVER.group_mask_);
        existlan.insert(lan);
        TBSYS_LOG(DEBUG, "exist server(%s)", tbsys::CNetUtil::addrToString((*s_iter)->id()).c_str());
        //exist_server.insert((*s_iter));
      }

      ServerCollect* first_result = NULL;
      DS_WEIGHT::const_iterator iter = weights.begin();
      while (iter != weights.end())
      {
				id = iter->second->id();
        lan = Func::get_lan(id, SYSPARAM_NAMESERVER.group_mask_);
        TBSYS_LOG(DEBUG, "server(%s) find(%d)", tbsys::CNetUtil::addrToString(id).c_str(),
          existlan.find(lan) == existlan.end()/*, exist_server.find(iter->second) == exist_server.end()*/);
        if ((first_result == NULL) 
            && (existlan.find(lan) == existlan.end()))
            //&& (exist_server.find(iter->second) == exist_server.end()))
        {
          first_result = iter->second;
        }

        if ((*result == NULL)
            && (existlan.find(lan) == existlan.end())
            && (get_ip(id) == get_ip(mover->id())))
            //&& (exist_server.find(iter->second) == exist_server.end()))
        {
          *result = iter->second;
        }

        if ((first_result != NULL) && (*result != NULL))
        {
          break;
        }
        ++iter;
      }

      if (*result == NULL)
      {
        *result = first_result;
      }
      return (*result != NULL);
    }

    int delete_excess_backup(const std::vector<ServerCollect*> & source, int32_t count, std::vector<ServerCollect*> & result, DeleteExcessBackupStrategy flag)
    {
      bool bret = flag == DELETE_EXCESS_BACKUP_STRATEGY_BY_GROUP ? SYSPARAM_NAMESERVER.group_mask_ == 0 ? false : true : true; 
      if (bret)
      {
        if (flag == DELETE_EXCESS_BACKUP_STRATEGY_BY_GROUP)
        {
          count = 0;
        }

        std::multimap<int32_t, ServerCollect*> tmp;
        std::multimap<int32_t, ServerCollect*> middle_result;

        std::vector<ServerCollect*>::const_iterator r_iter = source.begin();
        for (; r_iter != source.end(); ++r_iter)
        {
          tmp.insert(std::multimap<int32_t, ServerCollect*>::value_type((*r_iter)->block_count(), (*r_iter)));
        }

        if (SYSPARAM_NAMESERVER.group_mask_ == 0)
        {
          middle_result.insert(tmp.rbegin(), tmp.rend());
        }
        else
        {
          uint32_t lanip = 0;
          std::set<uint32_t> groups;
          std::multimap<int32_t, ServerCollect*>::const_reverse_iterator iter = tmp.rbegin();
          for (; iter != tmp.rend(); ++iter)
          {
            lanip = Func::get_lan(iter->second->id(), SYSPARAM_NAMESERVER.group_mask_);
            std::pair<std::set<uint32_t>::iterator, bool> res = groups.insert(lanip);
            if (!res.second)
            {
              result.push_back(iter->second);
            }
            else
            {
              middle_result.insert(std::multimap<int32_t, ServerCollect*>::value_type(iter->second->block_count(), iter->second));
            }
          }
        }

        count -= result.size();
        if (count <= 0)
          return result.size();;

        std::multimap<int32_t, ServerCollect*>::const_reverse_iterator iter = middle_result.rbegin();
        for (; iter != middle_result.rend() && count > 0; ++iter, count--)
        {
          result.push_back(iter->second);
        }
      }
      return result.size();;
    }
  }
}

