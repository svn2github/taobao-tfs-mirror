 /*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: strategy.h 490 2011-06-14 03:11:02Z duanfei@taobao.com $
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
#ifndef TFS_NAMESERVER_STATEGY_H_
#define TFS_NAMESERVER_STATEGY_H_

#include <map>
#include "ns_define.h"
#include "server_collect.h"
#include "layout_manager.h"
#include "common/lock.h"

namespace tfs
{
  namespace nameserver
  {
    static bool exist(std::vector<ServerCollect*>& table,ServerCollect* except)
    {
      return table.empty() ? false : find(table.begin(), table.end(), except) != table.end();
    }

    typedef std::multimap<int64_t, ServerCollect*> DS_WEIGHT;
    struct AddUseCapacity
    {
      uint32_t operator()(const uint32_t acc, const ServerCollect* server) const
      {
        return acc + server->use_capacity();
      }
    };

    struct CompareLoad
    {
      uint32_t operator()(const ServerCollect* l, const ServerCollect* r) const
      {
        return l->load() < r->load();
      }
    };

    struct CompareBlockCount
    {
      uint32_t operator()(const ServerCollect* l, const ServerCollect* r) const
      {
        return l->block_count() < r->block_count();
      }
    };

    class BaseStrategy
    {
      public:
        BaseStrategy(int64_t seq, const NsGlobalStatisticsInfo& g) :
          seqno_average_num_(0), elect_average_num_(0), total_elect_num_(seq), primary_writable_block_count_(0), use_(0), load_(0), global_info_(g)
        {
        }
        virtual ~BaseStrategy() {}
        void normalize(const ServerCollect* server) const;
        virtual bool check(const ServerCollect* server) const;

      protected:
        mutable int64_t seqno_average_num_;
        mutable int64_t elect_average_num_;
        mutable int64_t total_elect_num_;
        mutable int32_t primary_writable_block_count_;
        mutable int32_t use_;
        mutable int32_t load_;

      private:
        DISALLOW_COPY_AND_ASSIGN( BaseStrategy);
        NsGlobalStatisticsInfo global_info_;
    };

    class WriteStrategy: public BaseStrategy
    {
      public:
        WriteStrategy(int64_t seq, const NsGlobalStatisticsInfo& g) :
          BaseStrategy(seq, g) {}
        virtual ~WriteStrategy() {}
        virtual int64_t calc(const ServerCollect* server) const;
      private:
        DISALLOW_COPY_AND_ASSIGN( WriteStrategy);
    };

    class ReplicateDestStrategy: public BaseStrategy 
    {
      public:
        ReplicateDestStrategy(uint32_t seq, const NsGlobalStatisticsInfo& g):
          BaseStrategy(seq, g) {}
        virtual ~ReplicateDestStrategy() {}
        virtual int64_t calc(const ServerCollect* server) const;
      private:
        DISALLOW_COPY_AND_ASSIGN( ReplicateDestStrategy);
    };

    class ReplicateSourceStrategy: public BaseStrategy 
    {
      public:
        ReplicateSourceStrategy(uint32_t seq, const NsGlobalStatisticsInfo& g) :
          BaseStrategy(seq, g) {}
        virtual ~ReplicateSourceStrategy() {}
        virtual int64_t calc(const ServerCollect* server) const;
      private:
        DISALLOW_COPY_AND_ASSIGN( ReplicateSourceStrategy);
    };

    template<typename Strategy>
      class StoreWeight
      {
        public:
          StoreWeight(Strategy& strategy, DS_WEIGHT& weight) :
            strategy_(strategy), weights_(weight) {}
          virtual ~StoreWeight() {}
          void operator()(const ServerCollect* server) const
          {
            int64_t weight = strategy_.calc(server);
            TBSYS_LOG(DEBUG, "server: %s weight: %"PRI64_PREFIX"d", tbsys::CNetUtil::addrToString(server->id()).c_str(), weight);
            if (weight > 0)
              weights_.insert(std::make_pair(weight, const_cast<ServerCollect*> (server)));
          }
          const DS_WEIGHT& get_weight() const
          {
            return weights_;
          }
        private:
          Strategy& strategy_;
          DS_WEIGHT& weights_;
      };

    int32_t elect_ds_exclude_group(const DS_WEIGHT& weights, const int32_t elect_count, std::vector<ServerCollect*> & elect_ds_list);
    int32_t elect_ds_normal(const DS_WEIGHT& weights, const int32_t elect_count,std::vector<ServerCollect*> & elect_ds_list);

    struct ExcludeGroupElectOperation
    {
      int32_t operator()(const DS_WEIGHT& weights, const int32_t elect_count, std::vector<ServerCollect*> & elect_ds_list) const
      {
        return elect_ds_exclude_group(weights, elect_count, elect_ds_list);
      }
    };

    struct NormalElectOperation
    {
      int32_t operator()(const DS_WEIGHT& weights, const int32_t elect_count, std::vector<ServerCollect*> & elect_ds_list) const
      {
        return elect_ds_normal(weights, elect_count, elect_ds_list);
      }
    };

    template<typename Strategy, typename ElectType>
      int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& source,
          std::vector<ServerCollect*>& except, int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*>& result)
      {
        DS_WEIGHT weights;
        StoreWeight<Strategy> store(strategy, weights);
        std::vector<ServerCollect*>::iterator iter = source.begin();
        for (; iter != source.end(); ++iter)
        {
          //if (!exist(except, (*iter)))
          bool has_valid = check_server_in_plan ? ((!meta.find_server_in_plan((*iter)))
                                             && (!exist(except, (*iter)))) : (!exist(except, (*iter)));
          if (has_valid)
          {
            common::RWLock::Lock lock((*(*iter)), common::READ_LOCKER);
            store(*iter);
          }
        }

        return op(weights, elect_count, result);
      }

    template<typename Strategy, typename ElectType>
      int32_t elect_ds(Strategy& strategy, ElectType op, LayoutManager& meta, std::vector<ServerCollect*>& except,
          int32_t elect_count, bool check_server_in_plan, std::vector<ServerCollect*> & result)
      {
        bool has_valid = false;
        DS_WEIGHT weights;
        StoreWeight<Strategy> store(strategy, weights);
        const SERVER_MAP& ds_map = meta.servers_; 
        SERVER_MAP::const_iterator iter = ds_map.begin();
        for (; iter != ds_map.end(); ++iter)
        {
          has_valid = check_server_in_plan ? ((!meta.find_server_in_plan(iter->second))
                                             && (!exist(except, iter->second))) : (!exist(except, iter->second));
          if(has_valid)
          {
            common::RWLock::Lock lock((*iter->second), common::READ_LOCKER);
            store(iter->second);
          }
        }
        TBSYS_LOG(INFO, "plan hold server size: %u, server size: %u", meta.server_to_task_.size(), meta.servers_.size());

        return op(weights, elect_count, result);
      }

    int elect_write_server(LayoutManager& meta, const int32_t elect_count, std::vector<ServerCollect*> & result);
    int elect_replicate_source_ds(LayoutManager& meta, std::vector<ServerCollect*>& source, std::vector<ServerCollect*>& except, int32_t elect_count, std::vector<ServerCollect*>& result);
    int elect_replicate_dest_ds(LayoutManager& meta, std::vector<ServerCollect*>& except, int32_t elect_count, std::vector<ServerCollect*> & result); 
    bool elect_move_dest_ds(const std::set<ServerCollect*>& targets, const std::vector<ServerCollect*> & source, const ServerCollect* mover, ServerCollect** result);
    int delete_excess_backup(const std::vector<ServerCollect*> & ds_list, int32_t count, std::vector<ServerCollect*> & result, common::DeleteExcessBackupStrategy falg = common::DELETE_EXCESS_BACKUP_STRATEGY_NORMAL);
  }
}
#endif 
