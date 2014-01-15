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

#ifndef TFS_NAMESERVER_SERVER_MANAGER_H_
#define TFS_NAMESERVER_SERVER_MANAGER_H_

#include <map>
#include <stdint.h>
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class LayoutManager;
    struct ServerIdCompare
    {
      bool operator ()(const ServerCollect* lhs, const ServerCollect* rhs) const
      {
        assert(NULL != lhs);
        assert(NULL != rhs);
        return lhs->id() < rhs->id();
      }
    };
    class ServerManager
    {
      typedef std::multimap<int64_t, ServerCollect*> SORT_MAP;
      typedef SORT_MAP::iterator SORT_MAP_ITER;
      typedef SORT_MAP::const_iterator SORT_MAP_CONST_ITER;
      typedef std::map<int64_t, SORT_MAP > GROUP_MAP;
      typedef GROUP_MAP::iterator GROUP_MAP_ITER;
      typedef GROUP_MAP::const_iterator GROUP_MAP_CONST_ITER;
      typedef common::TfsSortedVector<ServerCollect*,ServerIdCompare> SERVER_TABLE;
      typedef SERVER_TABLE::iterator SERVER_TABLE_ITER;
      #ifdef TFS_GTEST
      friend class ServerManagerTest;
      friend class LayoutManagerTest;
      void clear_();
      FRIEND_TEST(ServerManagerTest, add_remove_get);
      FRIEND_TEST(ServerManagerTest, pop_from_dead_queue);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      #endif
      public:
      explicit ServerManager(LayoutManager& manager);
      virtual ~ServerManager();

      int apply(const common::DataServerStatInfo& info, const int64_t now, const int32_t times);
      int giveup(const int64_t now, const uint64_t server);
      int renew(const common::DataServerStatInfo& info, const int64_t now, const int32_t times);
      bool has_valid_lease(const int64_t now, const uint64_t server) const;

      ServerCollect* get(const uint64_t server) const;
      int64_t size() const;
      int64_t wait_free_size() const;

      int timeout(const int64_t now, NsGlobalStatisticsInfo& stat_info,
        common::ArrayHelper<ServerCollect*>& report_block_servers, uint64_t& last_traverse_server, bool& all_over);
      int gc(const int64_t now);
      bool traverse(const uint64_t start, const SERVER_TABLE& table, common::ArrayHelper<ServerCollect*>& servers) const;

      ServerCollect* malloc(const common::DataServerStatInfo& info, const int64_t now);
      void free(ServerCollect* pserver);

      int apply_block(const uint64_t server, common::ArrayHelper<common::BlockLease>& output);
      int apply_block_for_update(const uint64_t server, common::ArrayHelper<common::BlockLease>& output);
      int renew_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<common::BlockLease>& output);
      int giveup_block(const uint64_t server, const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<common::BlockLease>& output);

      int scan(common::SSMScanParameter& param, int32_t& should, int32_t& start, int32_t& next, bool& all_over) const;

      bool get_range_servers(const uint64_t begin, common::ArrayHelper<ServerCollect*>& result) const;

      void set_all_server_next_report_time(const int32_t flag , const time_t now = common::Func::get_monotonic_time());

      int build_relation(ServerCollect* server, const uint64_t block,
          const bool writable, const bool master);

      int relieve_relation(ServerCollect* server, const uint64_t block);
      int relieve_relation(const uint64_t server, const uint64_t block);

      int move_statistic_all_server_info(int64_t& total_capacity, int64_t& total_use_capacity,
          int64_t& alive_server_nums) const;
      int move_split_servers(std::multimap<int64_t, ServerCollect*>& source,
          SERVER_TABLE& targets, const double percent) const;

      //choose one or more servers to create new block
      int choose_create_block_target_server(common::ArrayHelper<uint64_t>& result,
          common::ArrayHelper<uint64_t>& news, const int32_t count) const;

      //choose a server to replicate or move
      //replicate method
      int choose_replicate_source_server(ServerCollect*& result, const common::ArrayHelper<uint64_t>& source) const;
      int choose_replicate_target_server(ServerCollect*& result, const common::ArrayHelper<uint64_t>& except) const;

      //move method
      int choose_move_target_server(ServerCollect*& result,SERVER_TABLE& source,
          common::ArrayHelper<uint64_t>& except) const;

      //delete method
      int choose_excess_backup_server(ServerCollect*& result, const common::ArrayHelper<uint64_t>& sources) const;

      int expand_ratio(int32_t& index, const float expand_ratio = 0.1);

      int calc_single_process_max_network_bandwidth(int32_t& max_mr_network_bandwith,
            int32_t& max_rw_network_bandwith, const common::DataServerStatInfo& info) const;

      int regular_create_block_for_servers(uint64_t& begin, bool& all_over);

      //int choose_writable_block(BlockCollect*& result);

      private:
      DISALLOW_COPY_AND_ASSIGN(ServerManager);
      ServerCollect* get_(const uint64_t server) const;
      bool traverse_(const uint64_t start, SERVER_TABLE& table, common::ArrayHelper<ServerCollect*>& leases) const;

      void get_lans_(std::set<uint32_t>& lans, const common::ArrayHelper<uint64_t>& source) const;

      void move_split_servers_(std::multimap<int64_t, ServerCollect*>& source,
          std::multimap<int64_t, ServerCollect*>& outside,
          SERVER_TABLE& targets, const ServerCollect* server, const double percent) const;

      //int choose_writable_server_lock_(ServerCollect*& result);
      //int choose_writable_server_random_lock_(ServerCollect*& result);

      int choose_replciate_random_choose_server_base_lock_(ServerCollect*& result,
          const common::ArrayHelper<uint64_t>& except, const std::set<uint32_t>& lans) const;
      int choose_replciate_random_choose_server_extend_lock_(ServerCollect*& result,
          const common::ArrayHelper<uint64_t>& except, const std::set<uint32_t>& lans) const;

      bool for_each_(const uint64_t start, const SERVER_TABLE& table, common::ArrayHelper<ServerCollect*>& servers) const;

      private:
      SERVER_TABLE servers_;
      SERVER_TABLE wait_free_servers_;
      LayoutManager& manager_;
      mutable common::RWLock rwmutex_;
      uint64_t last_traverse_server_;
      NsGlobalStatisticsInfo global_stat_info_;
      int32_t wait_free_wait_time_;
      int32_t wait_clear_wait_time_;
      int32_t write_index_;
    };
  }/** nameserver **/
}/** tfs **/


#endif /* SERVER_MANAGER_H_*/
