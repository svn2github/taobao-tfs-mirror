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
#ifndef TFS_NAMESERVER_STATEGY_H_
#define TFS_NAMESERVER_STATEGY_H_

#include <map>
#include "ns_define.h"
#include "layout_manager.h"

namespace tfs
{
  namespace nameserver
  {
    typedef std::multimap<int32_t, ServerCollect*> DS_WEIGHT;
    struct AddLoad
    {
      int32_t operator()(const int32_t acc, const ServerCollect* server_collect) const
      {
        return acc + server_collect->get_ds()->current_load_;
      }
    };

    struct AddUseCapacity
    {
      uint32_t operator()(const uint32_t acc, const ServerCollect* server_collect) const
      {
        return acc + server_collect->get_ds()->use_capacity_;
      }
    };

    struct CompareLoad
    {
      uint32_t operator()(const ServerCollect* l, const ServerCollect* r) const
      {
        return l->get_ds()->current_load_ < r->get_ds()->current_load_;
      }
    };

    struct CompareBlockCount
    {
      uint32_t operator()(const ServerCollect* l, const ServerCollect* r) const
      {
        return l->get_ds()->block_count_ < r->get_ds()->block_count_;
      }
    };

    class BaseStrategy
    {
    public:
      BaseStrategy(int64_t seq, const NsGlobalInfo& g) :
        seqno_(0), primary_writable_block_count_(0), use_(0), load_(0), elect_seqno_(seq), global_info_(g)
      {
      }
      virtual ~BaseStrategy()
      {
      }

      void normalize(const ServerCollect* server_collect) const;
      virtual int check(const ServerCollect* server_collect) const;

    protected:
      mutable int64_t seqno_;
      mutable int32_t primary_writable_block_count_;
      mutable int32_t use_;
      mutable int32_t load_;

    private:
      DISALLOW_COPY_AND_ASSIGN( BaseStrategy);
      int64_t elect_seqno_;
      NsGlobalInfo global_info_;
    };

    class WriteStrategy: public BaseStrategy
    {
    public:
      WriteStrategy(int64_t seq, const NsGlobalInfo& g) :
        BaseStrategy(seq, g)
      {
      }
      virtual ~WriteStrategy()
      {
      }
      virtual int64_t calc(const ServerCollect* server_collect) const;
    private:
      DISALLOW_COPY_AND_ASSIGN( WriteStrategy);
    };

    class ReplicateStrategy: public BaseStrategy
    {
    public:
      typedef std::map<uint64_t, uint32_t> counter_type;
      typedef counter_type::iterator couter_iterator;
      typedef counter_type::const_iterator counter_const_iterator;

      static int64_t inc_ds_count(counter_type& counter, const uint64_t server_id);
      static int64_t dec_ds_count(counter_type& counter, const uint64_t server_id);
      static int64_t get_ds_count(const counter_type& counter, const uint64_t server_id);
      static uint32_t get_ds_ip(uint64_t server_id);
    public:
      ReplicateStrategy(int64_t seq, const NsGlobalInfo& g, const counter_type & c) :
        BaseStrategy(seq, g), counter_(c)
      {
      }
      virtual ~ReplicateStrategy()
      {
      }
      virtual int check(const ServerCollect* server_collect) const;
    protected:
      mutable int32_t copy_count_;
      const counter_type & counter_;
    private:
      DISALLOW_COPY_AND_ASSIGN( ReplicateStrategy);
    };

    class ReplicateDestStrategy: public ReplicateStrategy
    {
    public:
      ReplicateDestStrategy(uint32_t seq, const NsGlobalInfo& g, const counter_type & c) :
        ReplicateStrategy(seq, g, c)
      {
      }
      virtual ~ReplicateDestStrategy()
      {
      }
      ;
      virtual int64_t calc(const ServerCollect* server_collect) const;
    private:
      DISALLOW_COPY_AND_ASSIGN( ReplicateDestStrategy);
    };

    class ReplicateSourceStrategy: public ReplicateStrategy
    {
    public:
      ReplicateSourceStrategy(uint32_t seq, const NsGlobalInfo& g, const counter_type & c) :
        ReplicateStrategy(seq, g, c)
      {
      }
      virtual ~ReplicateSourceStrategy()
      {
      }
      virtual int64_t calc(const ServerCollect* server_collect) const;
    private:
      DISALLOW_COPY_AND_ASSIGN( ReplicateSourceStrategy);
    };

    template<typename Strategy>
    class StoreWeight
    {
    public:
      StoreWeight(Strategy& strategy, DS_WEIGHT& weight) :
        strategy_(strategy), weights_(weight)
      {
      }
      virtual ~StoreWeight()
      {
      }
      void operator()(const ServerCollect* server_collect) const
      {
        int64_t weight = strategy_.calc(server_collect);
        TBSYS_LOG(DEBUG, "weight(%"PRI64_PREFIX"d)", weight);
        if (weight > 0)
          weights_.insert(std::make_pair(weight, const_cast<ServerCollect*> (server_collect)));
      }
      const DS_WEIGHT& get_weight() const
      {
        return weights_;
      }
    private:
      Strategy& strategy_;
      DS_WEIGHT& weights_;
    };

    int32_t elect_ds_exclude_group(const DS_WEIGHT& weights, const int32_t elect_count, int64_t & elect_seq,
        common::VUINT64& elect_ds_list);

    int32_t elect_ds_normal(const DS_WEIGHT& weights, const int32_t elect_count, int64_t & elect_seq,
        common::VUINT64& elect_ds_list);

    struct ExcludeGroupElectOperation
    {
      int32_t operator()(const DS_WEIGHT& weights, const int32_t elect_count, int64_t& elect_seq,
          common::VUINT64& elect_ds_list) const
      {
        return elect_ds_exclude_group(weights, elect_count, elect_seq, elect_ds_list);
      }
    };

    struct NormalElectOperation
    {
      int32_t operator()(const DS_WEIGHT& weights, const int32_t elect_count, int64_t & elect_seq,
          common::VUINT64& elect_ds_list) const
      {
        return elect_ds_normal(weights, elect_count, elect_seq, elect_ds_list);
      }
    };

    template<typename Strategy, typename ElectType>
    int32_t elect_ds(Strategy& strategy, ElectType op, const std::vector<ServerCollect*>& ds_list,
        const int32_t elect_count, int64_t & elect_seq, common::VUINT64& elect_ds_list)
    {
      DS_WEIGHT weights;
      StoreWeight<Strategy> store(strategy, weights);
      std::for_each(ds_list.begin(), ds_list.end(), store);
      int32_t result = op(weights, elect_count, elect_seq, elect_ds_list);
      if (elect_seq <= 0)
      {
        elect_seq = 1;
        uint32_t ds_size = ds_list.size();
        for (uint32_t i = 0; i < ds_size; ++i)
        {
          ds_list[i]->elect(1);
        }
      }
      return result;
    }

    template<typename Strategy, typename ElectType>
    int32_t elect_ds(Strategy& strategy, ElectType op, const LayoutManager& meta, const int32_t elect_count,
        int64_t& elect_seq, common::VUINT64& elect_ds_list)
    {
      DS_WEIGHT weights;
      StoreWeight<Strategy> store(strategy, weights);
      const common::SERVER_MAP* ds_map = meta.get_ds_map();
      common::SERVER_MAP::const_iterator iter = ds_map->begin();
      for (; iter != ds_map->end(); ++iter)
      {
        store(iter->second);
      }

      int32_t result = op(weights, elect_count, elect_seq, elect_ds_list);

      if (elect_seq <= 0)
      {
        elect_seq = 1;
        iter = ds_map->begin();
        for (; iter != ds_map->end(); ++iter)
        {
          iter->second->elect(1);
        }
      }
      return result;
    }

    int32_t elect_write_ds(const LayoutManager& meta, const int32_t elect_count, common::VUINT64& elect_ds_list);

    int32_t elect_replicate_source_ds(const std::vector<ServerCollect*>& ds_list,
        const ReplicateStrategy::counter_type& src_counter, const int32_t elect_count, common::VUINT64& elect_ds_list);

    int32_t elect_replicate_dest_ds(const LayoutManager& meta, const ReplicateStrategy::counter_type& dest_counter,
        const int32_t elect_count, common::VUINT64& elect_ds_list);

    bool elect_move_dest_ds(const std::vector<ServerCollect*>& ds_list, const ReplicateStrategy::counter_type& dest_counter,
        const common::VUINT64& elect_ds_list, const uint64_t src_ds, uint64_t & dest_ds);

  }
}
#endif 
