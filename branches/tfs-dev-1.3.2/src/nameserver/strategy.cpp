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
#include <numeric>

using namespace tfs::common;
using namespace tbsys;
namespace tfs
{
  namespace nameserver
  {

    template<typename T1, typename T2>
    int32_t percent(T1 v, T2 total)
    {
      if (total == 0)
        total = v;
      return std::min(static_cast<int32_t> (100 * (static_cast<double> (v) / total)), 100);
    }

    bool check_average(int32_t current_load, int32_t total_load, int64_t use_capacity, int64_t total_use_capacity,
        int64_t alive_ds_count)
    {
      if (alive_ds_count == 0)
      {
        TBSYS_LOG(DEBUG, "alive dataserver not found alive_ds_count(%"PRI64_PREFIX"d)", alive_ds_count);
        return 0;
      }
      int64_t average_load = total_load / alive_ds_count;
      int64_t average_use = total_use_capacity / alive_ds_count;

      return (((current_load < average_load * 2) || (total_load == 0)) && ((use_capacity <= average_use * 2)
          || (total_use_capacity == 0)));
    }

    int BaseStrategy::check(const ServerCollect* server_collect) const
    {
      if (!server_collect->is_alive())
      {
        TBSYS_LOG(DEBUG, "dataserver(%s) is dead, can't join ",
            CNetUtil::addrToString(server_collect->get_ds()->id_).c_str());
        return 0;
      }
      const DataServerStatInfo* ds_stat_info = server_collect->get_ds();

      return check_average(ds_stat_info->current_load_, global_info_.total_load_, ds_stat_info->use_capacity_,
          global_info_.total_capacity_, global_info_.alive_server_count_);
    }

    void BaseStrategy::normalize(const ServerCollect* server_collect) const
    {
      int32_t primary_writable_block_count =
          static_cast<int32_t> (server_collect->get_primary_writable_block_list()->size());

      const DataServerStatInfo* ds_stat_info = server_collect->get_ds();

      primary_writable_block_count_ = percent(primary_writable_block_count, SYSPARAM_NAMESERVER.max_write_file_count_);

      seqno_ = percent(server_collect->get_elect_seq(), elect_seqno_);

      if (global_info_.max_block_count_ == 0)
        use_ = percent(ds_stat_info->use_capacity_, ds_stat_info->total_capacity_);
      else
        use_ = percent(ds_stat_info->block_count_, global_info_.max_block_count_);

      if (global_info_.max_load_ < 10)
        load_ = percent(ds_stat_info->current_load_, 3000);
      else
        load_ = percent(ds_stat_info->current_load_, global_info_.max_load_);
    }

    //---------------------------------------------------------
    // WriteStrategy 
    //---------------------------------------------------------
    int64_t WriteStrategy::calc(const ServerCollect* server_collect) const
    {
      if (server_collect->is_disk_full())
      {
        TBSYS_LOG(DEBUG, "dataserver(%s) is full , can't join elect list", CNetUtil::addrToString(
            server_collect->get_ds()->id_).c_str());
        return 0;
      }
      if (BaseStrategy::check(server_collect) == 0)
      {
        TBSYS_LOG(DEBUG, "BaseStrategy::check == 0 , can't join elect list");
        return 0;
      }
      return server_collect->get_elect_seq();
    }

    //---------------------------------------------------------
    // ReplicateStrategy
    //---------------------------------------------------------
    int ReplicateStrategy::check(const ServerCollect* server_collect) const
    {
      int64_t count = get_ds_count(counter_, server_collect->get_ds()->id_);

      copy_count_ = percent(count, SYSPARAM_NAMESERVER.replicate_max_count_per_server_);
      if (count >= SYSPARAM_NAMESERVER.replicate_max_count_per_server_)
        return 0x00;
      return 0x01;
    }

    uint32_t ReplicateStrategy::get_ds_ip(const uint64_t ds_id)
    {
      return reinterpret_cast<IpAddr*> (const_cast<uint64_t*> (&ds_id))->ip_;
    }

    int64_t ReplicateStrategy::inc_ds_count(counter_type& counter, const uint64_t ds_id)
    {
      counter_type::iterator iter = counter.find(ds_id);
      if (iter == counter.end())
      {
        counter[ds_id] = 1;
        return 1;
      }
      ++iter->second;
      return iter->second;
    }

    int64_t ReplicateStrategy::dec_ds_count(counter_type& counter, const uint64_t ds_id)
    {
      counter_type::iterator iter = counter.find(ds_id);
      if (iter == counter.end())
        return 0;
      --iter->second;
      if (iter->second <= 0)
      {
        counter.erase(iter);
        return 0;
      }
      return iter->second;
    }

    int64_t ReplicateStrategy::get_ds_count(const counter_type& counter, const uint64_t ds_id)
    {
      counter_type::const_iterator iter = counter.find(ds_id);
      if (iter == counter.end())
        return 0;
      return iter->second;
    }

    //---------------------------------------------------------
    // ReplicateDestStrategy
    //---------------------------------------------------------
    int64_t ReplicateDestStrategy::calc(const ServerCollect* server_collect) const
    {
      if (server_collect->is_disk_full())
        return 0;
      if (!BaseStrategy::check(server_collect))
        return 0;
      if (!ReplicateStrategy::check(server_collect))
        return 0;
      BaseStrategy::normalize(server_collect);
      return seqno_ * 60 + copy_count_ * 20 + use_ * 15 + load_ * 5;
    }

    //---------------------------------------------------------
    // ReplicateSourceStrategy
    //---------------------------------------------------------
    int64_t ReplicateSourceStrategy::calc(const ServerCollect* server_collect) const
    {
      if (!ReplicateStrategy::check(server_collect))
        return 0;
      BaseStrategy::normalize(server_collect);
      return copy_count_ * 70 + load_ * 30;
    }

    void dump_weigths(const multimap<int32_t, ServerCollect*>& weights)
    {
      printf("-----------------------dump_weigths(%"PRI64_PREFIX"u)----------------------\n", weights.size());
      multimap<int32_t, ServerCollect*>::const_iterator it = weights.begin();
      while (it != weights.end())
      {
        printf("server(%s), weight(%d)", tbsys::CNetUtil::addrToString(it->second->get_ds()->id_).c_str(), it->first);
        ++it;
      }
      printf("-----------------------dump_weigths end----------------------\n");
    }

    //---------------------------------------------------------
    // elect_ds with all kinds of strategy
    //---------------------------------------------------------
    /**
     * elect_ds_exclude_group
     *
     *
     * @param [in] weights: choose DataServerStatInfo weights, min to max
     * @param [in] elect_count: need DataServerStatInfo count
     * @param [in] elect_seq: >0, set elect seq
     * [in,out] elect_ds_list: DataServerStatInfos elected
     *
     * @return  elected count
     */
    //
    int32_t elect_ds_exclude_group(const DS_WEIGHT& weights, const int32_t elect_count, int64_t& elect_seq,
        VUINT64& elect_ds_list)
    {
      if (elect_count == 0)
      {
        TBSYS_LOG(DEBUG, "current elect count(%d) <= 0, must be return", elect_count);
        return 0;
      }

      std::set < uint32_t > existlan;
      for (uint32_t i = 0; i < elect_ds_list.size(); ++i)
      {
        uint32_t lan = Func::get_lan(elect_ds_list[i], SYSPARAM_NAMESERVER.group_mask_);
        existlan.insert(lan);
      }

      //dump_weigths(weights);

      DS_WEIGHT::const_iterator iter = weights.begin();
      int32_t need_elect_count = elect_count;
      TBSYS_LOG(DEBUG, "weights.size(%u), need_elect_count(%d)", weights.size(), need_elect_count);
      DataServerStatInfo* ds_stat_info = NULL;
      while (iter != weights.end() && need_elect_count > 0)
      {
        ds_stat_info = iter->second->get_ds();
        uint32_t dlan = Func::get_lan(ds_stat_info->id_, SYSPARAM_NAMESERVER.group_mask_);
        if (existlan.find(dlan) == existlan.end())
        {
          existlan.insert(dlan);
          elect_ds_list.push_back(ds_stat_info->id_);
          if (elect_seq > 0)
            iter->second->elect(++elect_seq);
          --need_elect_count;
        }
        ++iter;
      }
      TBSYS_LOG(DEBUG, "current elect_count(%d)", elect_count - need_elect_count);
      return elect_count - need_elect_count;
    }

    int32_t elect_ds_normal(const DS_WEIGHT& weights, const int32_t elect_count, int64_t& elect_seq,
        VUINT64& elect_ds_list)
    {
      if (elect_count == 0)
        return 0;

      int32_t need_elect_count = elect_count;

      DS_WEIGHT::const_iterator iter = weights.begin();
      while (iter != weights.end() && need_elect_count > 0)
      {
        elect_ds_list.push_back(iter->second->get_ds()->id_);
        if (elect_seq > 0)
          iter->second->elect(++elect_seq);
        --need_elect_count;
        ++iter;
      }
      return elect_count - need_elect_count;
    }

    int32_t elect_write_ds(const LayoutManager& meta, const int32_t elect_count, VUINT64& elect_ds_list)
    {
      WriteStrategy strategy(meta.get_elect_seq(), *meta.get_ns_global_info());
      int64_t elect_seq = meta.get_elect_seq();
      int32_t ret = elect_ds(strategy, ExcludeGroupElectOperation(), meta, elect_count, elect_seq, elect_ds_list);
      meta.set_elect_seq(elect_seq);
      return ret;
    }

    int32_t elect_replicate_source_ds(const vector<ServerCollect*>& ds_list,
        const ReplicateStrategy::counter_type& src_counter, const int32_t elect_count, VUINT64& elect_ds_list)
    {
      vector<ServerCollect*>::const_iterator maxit = std::max_element(ds_list.begin(), ds_list.end(), CompareLoad());

      int32_t max_load = 1;
      if (maxit != ds_list.end())
        max_load = (*maxit)->get_ds()->current_load_;

      NsGlobalInfo info;
      info.max_load_ = max_load; // only max_load & alive_server_count could be useful, calc.
      info.alive_server_count_ = ds_list.size();
      // elect seq not used in this case;
      ReplicateSourceStrategy strategy(1, info, src_counter);

      int64_t seq = 0;
      return elect_ds(strategy, NormalElectOperation(), ds_list, elect_count, seq, elect_ds_list);
    }

    int32_t elect_replicate_dest_ds(const LayoutManager& meta,
        const ReplicateSourceStrategy::counter_type& dest_counter, const int32_t elect_count, VUINT64& elect_ds_list)
    {
      ReplicateDestStrategy strategy(meta.get_elect_seq(), *meta.get_ns_global_info(), dest_counter);
      int64_t elect_seq = meta.get_elect_seq();
      int32_t ret = elect_ds(strategy, ExcludeGroupElectOperation(), meta, elect_count, elect_seq, elect_ds_list);
      meta.set_elect_seq(elect_seq);
      return ret;
    }

    bool elect_move_dest_ds(const vector<ServerCollect*>& ds_list,
        const ReplicateDestStrategy::counter_type& dest_counter, const VUINT64& elect_ds_list, const uint64_t src_ds,
        uint64_t & dest_ds)
    {
      vector<ServerCollect*>::const_iterator maxit = std::max_element(ds_list.begin(), ds_list.end(), CompareLoad());

      int32_t max_load = 1;
      if (maxit != ds_list.end())
        max_load = (*maxit)->get_ds()->current_load_;

      NsGlobalInfo ginfo;
      ginfo.max_load_ = max_load; // only max_load & alive_server_count could be useful, calc.
      ginfo.alive_server_count_ = ds_list.size();
      // elect seq not used in this case;
      ReplicateSourceStrategy strategy(1, ginfo, dest_counter);

      DS_WEIGHT weights;
      StoreWeight < ReplicateSourceStrategy > store(strategy, weights);
      std::for_each(ds_list.begin(), ds_list.end(), store);

      std::set < uint32_t > existlan;
      uint32_t elect_ds_list_size = elect_ds_list.size();
      for (uint32_t i = 0; i < elect_ds_list_size; ++i)
      {
        uint32_t lan = Func::get_lan(elect_ds_list[i], SYSPARAM_NAMESERVER.group_mask_);
        existlan.insert(lan);
      }

      dest_ds = 0;
      uint64_t first_elect_ds = 0;
      uint32_t dlan = 0;
      DataServerStatInfo* ds_stat_info = NULL;
      DS_WEIGHT::const_iterator iter = weights.begin();
      while (iter != weights.end())
      {
        ds_stat_info = iter->second->get_ds();

        dlan = Func::get_lan(ds_stat_info->id_, SYSPARAM_NAMESERVER.group_mask_);

        if ((first_elect_ds == 0) && (existlan.find(dlan) == existlan.end()))
        {
          first_elect_ds = ds_stat_info->id_;
        }

        if ((dest_ds == 0) && (existlan.find(dlan) == existlan.end()) && (ReplicateStrategy::get_ds_ip(src_ds)
            == ReplicateStrategy::get_ds_ip(ds_stat_info->id_)))
        {
          dest_ds = ds_stat_info->id_;
        }

        if ((first_elect_ds != 0) && (dest_ds != 0))
        {
          break;
        }
        ++iter;
      }

      if (dest_ds == 0)
      {
        dest_ds = first_elect_ds;
      }
      return (dest_ds != 0);
    }
  }
}

