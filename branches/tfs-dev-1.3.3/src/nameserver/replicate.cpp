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
#include <Memory.hpp>
#include "message/client_pool.h"
#include "message/client.h"
#include "replicate.h"
#include "nameserver.h"
#include "common/error_msg.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace nameserver
  {

    //---------------------------------------------------------
    // ReplicateLauncher
    //---------------------------------------------------------
    ReplicateLauncher::ReplicateLauncher(MetaManager &m) :
      meta_mgr_(m), executor_(m), stop_balance_count_(0), pause_flag_(PAUSE_FLAG_RUN), destroy_flag_(
          NS_DESTROY_FLAGS_NO)
    {
      first_thread_.set_thread_parameter(&executor_, 1, this);
      second_thread_.set_thread_parameter(&executor_, 1, this);
    }

    void ReplicateLauncher::initialize()
    {
      first_thread_.start();
      second_thread_.start();
    }

    void ReplicateLauncher::destroy()
    {
      first_thread_.stop();
      second_thread_.stop();
      destroy_flag_ = NS_DESTROY_FLAGS_YES;
    }

    void ReplicateLauncher::wait_for_shut_down()
    {
      first_thread_.wait();
      second_thread_.wait();
    }

    int ReplicateLauncher::emergency_replicate(uint32_t block_id)
    {
      BlockCollect* block_collect = meta_mgr_.get_block_ds_mgr().get_block_collect(block_id);
      if ((block_collect != NULL) && (block_collect->get_ds()->size()
          < static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_)))
      {
        first_thread_.push(block_id);
        return TFS_SUCCESS;
      }
      return EXIT_GENERAL_ERROR;
    }

    /**
     * scans block_ds_map, find block which replica less than min_replication_
     * 1) 1 replica, replicating immediately.
     * 2) >1 replica, but < min_replication_, build replicate plan.
     * @param block_collect : check block replica size whether need replicate.
     * @return true : need build global plan 
     */
    bool ReplicateLauncher::check(const BlockCollect* block_collect)
    {
      if ((pause_flag_ & PAUSE_FLAG_PAUSE))
      {
        TBSYS_LOG(INFO, "replicate check paused.");
        return false;
      }
      time_t now = time(NULL) - SYSPARAM_NAMESERVER.replicate_wait_time_;

      uint32_t block_id = block_collect->get_block_info()->block_id_;
      uint32_t size = block_collect->get_ds()->size();

      TBSYS_LOG(DEBUG, "block(%u) ,has dataserver size(%u)", block_id, size);
      if (size < static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_))
      {
        if (size == 0)
        {
          TBSYS_LOG(ERROR, "block(%u) has been lost, do not replicate", block_id);
          return false;
        }
        else if ((size == 1) && (SYSPARAM_NAMESERVER.min_replication_ > 2))
        {
          TBSYS_LOG(DEBUG, "emergency replicate block:%u", block_id);
          first_thread_.push(block_id);
          ++stop_balance_count_;
          return false;
        }
        else if (block_collect->get_last_leave_time() < now)
        {
          TBSYS_LOG(DEBUG, "block_id(%u)build replicate plan", block_id);
          ++stop_balance_count_;
          return true;
        }
      }
      return false;
    }

    void ReplicateLauncher::sort(REPL_BLOCK_LIST &repl_block_list)
    {
      INT64_INT_MAP src_block_ds_map;
      INT64_INT_MAP dest_block_ds_map;
      int32_t src_size = 0;
      int32_t dest_size = 0;
      uint32_t i = 0;
      ReplBlock* replicate_block = NULL;
      const uint32_t replicate_block_size = repl_block_list.size();

      for (i = 0; i < replicate_block_size; ++i)
      {
        replicate_block = repl_block_list.at(i);
        if (src_block_ds_map.find(replicate_block->source_id_) == src_block_ds_map.end())
        {
          src_block_ds_map[replicate_block->source_id_] = ++src_size;
        }
        if (dest_block_ds_map.find(replicate_block->destination_id_) == dest_block_ds_map.end())
        {
          dest_block_ds_map[replicate_block->destination_id_] = ++dest_size;
        }
      }

      int32_t total_size = src_size * dest_size;
      int32_t src_ds_count = 0;
      int32_t dest_ds_count = 0;
      int32_t code = 0;
      INT_MAP result_maps;
      INT_MAP_ITER iter;
      for (i = 0; i < replicate_block_size; ++i)
      {
        replicate_block = repl_block_list.at(i);
        src_ds_count = src_block_ds_map[replicate_block->source_id_];
        dest_ds_count = dest_block_ds_map[replicate_block->destination_id_];
        if (dest_ds_count >= src_ds_count)
        {
          code = (dest_ds_count - src_ds_count) * src_size + src_ds_count;
        }
        else
        {
          code = (dest_ds_count - src_ds_count + dest_size) * src_size + src_ds_count;
        }

        iter = result_maps.find(code);
        if (iter == result_maps.end())
        {
          replicate_block->server_count_ = code;
          result_maps[code] = 1;
        }
        else
        {
          replicate_block->server_count_ = iter->second * total_size + code;
          iter->second++;
        }
      }
      std::sort(repl_block_list.begin(), repl_block_list.end(), SeqNoSort());
    }

    int ReplicateLauncher::build_plan(const VUINT32& lose_block_list)
    {
      if (second_thread_.executing_)
      {
        TBSYS_LOG(INFO, "replicate is run");
        return TFS_SUCCESS;
      }

      if ((pause_flag_ & PAUSE_FLAG_PAUSE))
      {
        TBSYS_LOG(INFO, "build replicate plan paused.");
        return TFS_SUCCESS;
      }

      LayoutManager & block_ds_map = meta_mgr_.get_block_ds_mgr();
      uint32_t ds_size = static_cast<uint32_t> (block_ds_map.get_alive_ds_size());
      uint32_t lose_block_size = lose_block_list.size();
      if ((lose_block_size == 0) || (ds_size == 0))
      {
        TBSYS_LOG(WARN, "lose block size <= 0 or dataserver size <= 0, no need replicate");
        return EXIT_GENERAL_ERROR;
      }

      int32_t agv_size = static_cast<int32_t> (1.1 * static_cast<double> (lose_block_size)
          / static_cast<double> (ds_size)) + 1;

      TBSYS_LOG(INFO, "build  plan, dataserver total(%u), agv_size(%d), lose block count(%u)", ds_size, agv_size,
          lose_block_size);

      REPL_BLOCK_LIST replicate_block_queue;
      ReplicateStrategy::counter_type ds_src_counter;
      ReplicateStrategy::counter_type ds_dest_counter;

      vector<ServerCollect*> ds_collect_list;
      vector < uint64_t > elect_src_ds_list(1, 0);
      uint64_t source_id = 0;
      uint64_t destination_id;
      uint32_t block_id = 0;
      BlockCollect* block_collect = NULL;
      ServerCollect* server_collect = NULL;

      int32_t ret = 0;
      srand( time(NULL));
      int32_t start_index = __gnu_cxx::abs(rand() % lose_block_size);

      for (uint32_t i = 0; i < lose_block_size; ++i)
      {
        block_id = lose_block_list[(start_index + i) % lose_block_size];
        block_collect = block_ds_map.get_block_collect(block_id);
        if (block_collect == NULL)
          continue;
        executor_.mutex_.wrlock();
        ret = executor_.check(block_id, SYSPARAM_NAMESERVER.replicate_max_time_, false, true);
        executor_.mutex_.unlock();
        if (ret == 0)
        {
          TBSYS_LOG(ERROR, "replicating is exist,block: %u", block_id);
          continue;
        }
        if (meta_mgr_.get_lease_clerk().has_valid_lease(block_id))
        {
          TBSYS_LOG(DEBUG, "block:%u is on writing, has valid lease", block_id);
          continue;
        }

        ds_collect_list.clear();
        elect_src_ds_list.clear();
        VUINT64 elect_dest_ds_list = *block_collect->get_ds();
        if (elect_dest_ds_list.size() >= static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_))
        {
          TBSYS_LOG(WARN, "block(%u) in dataserver size(%u) >= min_replication(%u), not replicate",
              elect_dest_ds_list.size(), block_id, SYSPARAM_NAMESERVER.min_replication_);
          continue;
        }

        for (uint32_t j = 0; j < elect_dest_ds_list.size(); ++j)
        {
          server_collect = block_ds_map.get_ds_collect(elect_dest_ds_list[j]);
          if (server_collect != NULL)
            ds_collect_list.push_back(server_collect);
        }

        ret = elect_replicate_source_ds(ds_collect_list, ds_src_counter, 1, elect_src_ds_list);
        if (ret == 1)
        {
          source_id = elect_src_ds_list.back();
        }
        else
        {
          TBSYS_LOG(ERROR, "build_plan block:%u cannot found source dataserver", block_id);
          continue;
        }

        // elect dest dataserver for replicate.
        {
          ScopedRWLock scoped_lock(block_ds_map.get_server_mutex(), READ_LOCKER);
          ret = elect_replicate_dest_ds(block_ds_map, ds_dest_counter, 1, elect_dest_ds_list);
        }
        if (ret == 1)
        {
          destination_id = elect_dest_ds_list.back();
        }
        else
        {
          TBSYS_LOG(ERROR, "build_plan block:%u cannot found destination dataserver", block_id);
          continue;
        }

        ReplicateStrategy::inc_ds_count(ds_src_counter, source_id);
        ReplicateStrategy::inc_ds_count(ds_dest_counter, destination_id);

        TBSYS_LOG(DEBUG, "build_plan block(%u) replicate from (%s) to (%s)", block_id, tbsys::CNetUtil::addrToString(
            source_id).c_str(), tbsys::CNetUtil::addrToString(destination_id).c_str());

        ReplBlock *replicate_block = new ReplBlock();
        replicate_block->block_id_ = block_id;
        replicate_block->source_id_ = source_id;
        replicate_block->destination_id_ = destination_id;
        replicate_block_queue.push_back(replicate_block);
      }

      sort(replicate_block_queue);
      if (replicate_block_queue.size() > 0)
      {
        second_thread_.push(replicate_block_queue);
      }
      TBSYS_LOG(DEBUG, "replicate plan build complete block size:%u", replicate_block_queue.size());
      return TFS_SUCCESS;
    }

    int ReplicateLauncher::balance()
    {
      if (destroy_flag_ == NS_DESTROY_FLAGS_YES)
        return TFS_SUCCESS;

      if ((pause_flag_ & PAUSE_FLAG_PAUSE_BALANCE))
      {
        TBSYS_LOG(INFO, "pause balance.");
        return TFS_SUCCESS;
      }

      LayoutManager&block_ds_map = meta_mgr_.get_block_ds_mgr();

      int64_t current_stop_balance_count = stop_balance_count_;
      int32_t ds_size = block_ds_map.get_alive_ds_size();

      if (ds_size <= 1)
      {
        TBSYS_LOG(ERROR, "ds_size(%d) <= 1, must be stop balance", ds_size);
        return TFS_SUCCESS;
      }

      if (executor_.get_replicating_map().size() >= static_cast<uint32_t> (ds_size / 2))
      {
        TBSYS_LOG(ERROR, "replicating size(%u) > ds_size(%d)", executor_.get_replicating_map().size(), ds_size / 2);
        return TFS_SUCCESS;
      }

      int64_t total_block_count = 0;
      int64_t total_capacity = 0;
      const SERVER_MAP* ds_maps = block_ds_map.get_ds_map();
      SERVER_MAP::const_iterator iter = ds_maps->begin();
      DataServerStatInfo* ds_stat_info = NULL;
      for (; iter != ds_maps->end(); ++iter)
      {
        if (!iter->second->is_alive())
          continue;
        ds_stat_info = iter->second->get_ds();
        total_block_count += ds_stat_info->block_count_;
        total_capacity += (ds_stat_info->total_capacity_ * SYSPARAM_NAMESERVER.max_use_capacity_ratio_) / 100;
        total_capacity -= ds_stat_info->use_capacity_;
      }

      int64_t total_bytes = block_ds_map.cacl_all_block_bytes();
      int64_t block_count = block_ds_map.cacl_all_block_count();
      int64_t block_size = 0;
      if (total_bytes > 0 && block_count > 0)
      {
        block_size = total_bytes / block_count;
      }
      else
      {
        block_size = SYSPARAM_NAMESERVER.max_block_size_;
      }

      total_capacity += total_block_count * block_size;
      if (total_capacity == 0)
      {
        TBSYS_LOG(ERROR, "total_capacity(%"PRI64_PREFIX"d)", total_capacity);
        return TFS_SUCCESS;
      }

      TBSYS_LOG(INFO, "build move plan");
      INT64_INT_MAP ds_src_maps;
      INT64_INT_MAP ds_dest_maps;
      vector<ServerCollect*> dest_ds_list_desc;
      int64_t max_src_count = 0;
      int64_t max_dest_count = 0;
      int64_t current_block_count = 0;
      int64_t current_average_block_size = 0;
      int64_t should_block_count = 0;
      for (iter = ds_maps->begin(); iter != ds_maps->end(); ++iter)
      {
        if (!iter->second->is_alive())
          continue;
        ds_stat_info = iter->second->get_ds();
        current_block_count = ds_stat_info->block_count_;
        current_average_block_size = ((ds_stat_info->total_capacity_ * SYSPARAM_NAMESERVER.max_use_capacity_ratio_)
            / 100 - ds_stat_info->use_capacity_ + current_block_count * block_size);

        should_block_count = current_average_block_size * total_block_count / total_capacity;

        TBSYS_LOG(INFO, "dataserver(%s), should block count(%"PRI64_PREFIX"d), current block count(%"PRI64_PREFIX"d)",
            tbsys::CNetUtil::addrToString(iter->first).c_str(),should_block_count, current_block_count);

        if (should_block_count + 10 < current_block_count)
        {
          ds_src_maps.insert(INT64_INT_MAP::value_type(iter->first, current_block_count - should_block_count));
          max_src_count += (current_block_count - should_block_count);
        }
        else if (should_block_count > current_block_count)
        {
          ds_dest_maps.insert(INT64_INT_MAP::value_type(iter->first, should_block_count - current_block_count));
          max_dest_count += (should_block_count - current_block_count);
          dest_ds_list_desc.push_back(iter->second);
        }
      }

      if ((ds_dest_maps.size() == 0) || (ds_src_maps.size() == 0))
      {
        TBSYS_LOG(INFO, "block without moving data");
        return TFS_SUCCESS;
      }
      else
      {
        TBSYS_LOG(INFO, "src(%u),dest(%u)", ds_src_maps.size(), ds_dest_maps.size());
      }

      if (max_src_count > max_dest_count)
      {
        max_src_count = max_dest_count;
      }

      if (max_src_count > static_cast<int64_t> (ds_dest_maps.size()
          * SYSPARAM_NAMESERVER.replicate_max_count_per_server_))
      {
        max_src_count = ds_dest_maps.size() * SYSPARAM_NAMESERVER.replicate_max_count_per_server_;
      }

      ReplicateExecutor::REPL_BLOCK_MAP need_move_block_map;
      ReplicateStrategy::counter_type ds_dest_counter;
      INT64_INT_MAP_ITER ds_src_iter = ds_src_maps.begin();
      INT64_INT_MAP_ITER ds_dest_iter;
      vector<ServerCollect*>::iterator server_collect_iter;
      set<uint32_t>::iterator block_list_iter;
      ServerCollect* server_collect = NULL;
      BlockCollect* block_collect = NULL;
      VUINT64 ds_list;
      uint32_t block_id = 0;
      int32_t move_count = 0;
      int32_t elect_count = 0;
      uint64_t server_id = 0;
      for (; ds_src_iter != ds_src_maps.end(); ++ds_src_iter)
      {
        server_collect = block_ds_map.get_ds_collect(ds_src_iter->first);
        if (server_collect == NULL)
          continue;

        const set<uint32_t>& blks = server_collect->get_block_list();

        move_count = ds_src_iter ->second;
        elect_count = 0;
        server_id = 0;

        block_list_iter = blks.begin();
        for (; block_list_iter != blks.end(); ++block_list_iter)
        {
          block_id = (*block_list_iter);
          block_collect = block_ds_map.get_block_collect(block_id);
          if (block_collect == NULL)
            continue;
          if (!block_collect->is_full())
            continue;
          if (need_move_block_map.find(block_id) != need_move_block_map.end())
            continue;

          ds_list = *(block_collect->get_ds());

          if (ds_list.size() != static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_))
            continue;

          VUINT64::iterator where = find(ds_list.begin(), ds_list.end(), ds_src_iter->first);
          if (where == ds_list.end())
            continue;
          ds_list.erase(where);

          bool bret = elect_move_dest_ds(dest_ds_list_desc, ds_dest_counter, ds_list, ds_src_iter->first, server_id);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "cannot elect move dest server block:%u, source:%s,dest:%s", block_id,
                tbsys::CNetUtil::addrToString(ds_src_iter->first).c_str(),
                tbsys::CNetUtil::addrToString(server_id).c_str());
            continue;
          }
          else
          {
            ReplicateStrategy::inc_ds_count(ds_dest_counter, server_id);
          }

          ReplBlock *replicate_block = new ReplBlock();
          replicate_block->block_id_ = block_id;
          replicate_block->source_id_ = ds_src_iter->first;
          replicate_block->destination_id_ = server_id;
          need_move_block_map.insert(ReplicateExecutor::REPL_BLOCK_MAP::value_type(replicate_block->block_id_,
              replicate_block));

          --move_count;
          --max_src_count;
          ++elect_count;

          ds_dest_iter = ds_dest_maps.find(server_id);
          if (ds_dest_iter != ds_dest_maps.end())
          {
            ds_dest_iter->second--;
            if (ds_dest_iter->second <= 0)
            {
              ds_dest_maps.erase(ds_dest_iter);
              server_collect_iter = dest_ds_list_desc.begin();
              for (; server_collect_iter != dest_ds_list_desc.end(); ++server_collect_iter)
              {
                server_collect = (*server_collect_iter);
                if (server_collect->get_ds()->id_ == server_id)
                {
                  dest_ds_list_desc.erase(server_collect_iter);
                  break;
                }
              }
            }
          }
          if (dest_ds_list_desc.size() == 0)
            break;
          if (move_count <= 0 || max_src_count <= 0)
            break;
          if (elect_count >= SYSPARAM_NAMESERVER.replicate_max_count_per_server_)
            break;
        }
        if (max_src_count <= 0)
          break;
      }

      TBSYS_LOG(INFO, "need to move the data block size: %u", need_move_block_map.size());
      REPL_BLOCK_LIST need_move_block_list;
      ReplBlock* replicate_block = NULL;
      ReplicateExecutor::REPL_BLOCK_MAP_ITER rep_iter = need_move_block_map.begin();
      for (; rep_iter != need_move_block_map.end(); ++rep_iter)
      {
        replicate_block = rep_iter->second;
        need_move_block_list.push_back(replicate_block);
        TBSYS_LOG(DEBUG, "move plan: block:%u, from %s to %s", replicate_block->block_id_,
            tbsys::CNetUtil::addrToString(replicate_block->source_id_).c_str(), tbsys::CNetUtil::addrToString(
                replicate_block->destination_id_).c_str());
      }
      sort(need_move_block_list);

      int32_t retry = 0;
      uint32_t oldone = 0;
      uint32_t need_move_block_list_size = need_move_block_list.size();
      uint32_t uncomplete_move_block_num = need_move_block_list_size;
      while (uncomplete_move_block_num && retry < 10)
      {
        oldone = uncomplete_move_block_num;
        uncomplete_move_block_num = 0;
        for (uint32_t i = 0; i < need_move_block_list_size; ++i)
        {
          replicate_block = need_move_block_list.at(i);

          if (replicate_block->block_id_ == 0)
            continue;
          if (executor_.is_replicating_block(replicate_block, SYSPARAM_NAMESERVER.replicate_max_count_per_server_))
          {
            TBSYS_LOG(INFO, "move plans to give up: block:%u, from %s to %s", replicate_block->block_id_,
                tbsys::CNetUtil::addrToString(replicate_block->source_id_).c_str(), tbsys::CNetUtil::addrToString(
                    replicate_block->destination_id_).c_str());
            uncomplete_move_block_num++;
            continue;
          }
          if (NS_DESTROY_FLAGS_YES == destroy_flag_)
            break;
          block_collect = block_ds_map.get_block_collect(replicate_block->block_id_);
          if (block_collect == NULL)
          {
            replicate_block->block_id_ = 0;
          }

          /*
           {
           ScopedRWLock scoped_lock(metaData.m_writableMutex, WRITE_LOCKER);
           block_ds_map.removeWBlock(replicate_block->block_id_);
           }
           */

          executor_.send_replicate_cmd(replicate_block->source_id_, replicate_block->destination_id_,
              replicate_block->block_id_, REPLICATE_BLOCK_MOVE_FLAG_YES);
          replicate_block->block_id_ = 0;

          if (ds_size != block_ds_map.get_alive_ds_size())
            break;
          if (stop_balance_count_ > current_stop_balance_count)
            break;
        }

        TBSYS_LOG(INFO, "total moved count: %u, failed to move the block number: %u", need_move_block_list_size,
            uncomplete_move_block_num);

        if (NS_DESTROY_FLAGS_YES == destroy_flag_)
          break;
        if (ds_size != block_ds_map.get_alive_ds_size())
          break;
        if (stop_balance_count_ > current_stop_balance_count)
          break;

        Func::sleep(10, reinterpret_cast<int32_t*> (&destroy_flag_));

        if (NS_DESTROY_FLAGS_YES == destroy_flag_)
          break;
        if (ds_size != block_ds_map.get_alive_ds_size())
          break;
        if (stop_balance_count_ > current_stop_balance_count)
          break;

        if ((pause_flag_ & PAUSE_FLAG_PAUSE_BALANCE))
          break;

        if (oldone == uncomplete_move_block_num)
        {
          retry++;
        }
        else
        {
          retry = 0;
        }
      }

      TBSYS_LOG(INFO, "balance plan exit, remain uncomplete move(%u)", uncomplete_move_block_num);
      rep_iter = need_move_block_map.begin();
      for (; rep_iter != need_move_block_map.end(); ++rep_iter)
      {
        tbsys::gDelete(rep_iter->second);
      }
      return TFS_SUCCESS;
    }

    int ReplicateLauncher::handle_replicate_complete(ReplicateBlockMessage* message)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_.get_fs_name_system()->get_ns_global_info();
      int32_t ret = TFS_SUCCESS;
      if (message->get_command() == COMMAND_REPL_COMPLETE)
        ret = executor_.complete(message->get_repl_block(), true);
      else if (message->get_command() == COMMAND_REPL_FAILURE)
        ret = executor_.complete(message->get_repl_block(), false);

      if ((ret == STATUS_MESSAGE_OK || ret == STATUS_MESSAGE_REMOVE) && message->get_command() == COMMAND_REPL_COMPLETE
          && ngi->owner_role_ == NS_ROLE_MASTER)//master
      {
        meta_mgr_.get_oplog_sync_mgr()->register_msg(message);
      }
      if (ngi->owner_role_ == NS_ROLE_MASTER)//master
      {
        message->reply_message(new StatusMessage(ret));
      }
      return (ret == STATUS_MESSAGE_OK || ret == STATUS_MESSAGE_REMOVE) ? TFS_SUCCESS : ret;
    }

    int ReplicateLauncher::handle_replicate_info_msg(ReplicateInfoMessage* message)
    {
      ScopedRWLock lock(executor_.mutex_, READ_LOCKER);
      message->set_replicating_map(executor_.replicating_map_);
      message->set_source_ds_counter(executor_.src_ds_counter_);
      message->set_dest_ds_counter(executor_.dest_ds_counter_);
      return TFS_SUCCESS;
    }

    //---------------------------------------------------------
    // ReplicateExecutor
    //---------------------------------------------------------
    ReplicateExecutor::~ReplicateExecutor()
    {
      clear_all();
    }

    void ReplicateExecutor::clear_all()
    {
      ScopedRWLock lock(mutex_, WRITE_LOCKER);
      REPL_BLOCK_MAP_ITER iter = replicating_map_.begin();
      for (; iter != replicating_map_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
      replicating_map_.clear();
    }

    int ReplicateExecutor::execute(const uint32_t block_id, void*)
    {
      LayoutManager& block_ds_map = meta_mgr_.get_block_ds_mgr();
      BlockCollect *block_collect = block_ds_map.get_block_collect(block_id);
      if (block_collect == NULL)
      {
        TBSYS_LOG(ERROR, "block(%u) not found", block_id);
        return EXIT_BLOCK_NOT_FOUND;
      }
      if (meta_mgr_.get_lease_clerk().has_valid_lease(block_id))
      {
        TBSYS_LOG(DEBUG, "block(%u) is on writing, has valid lease", block_id);
        return EXIT_GENERAL_ERROR;
      }
      VUINT64 servers = *block_collect->get_ds();
      int32_t current_ds_size = static_cast<int32_t> (servers.size());

      if (current_ds_size == 0)
      {
        TBSYS_LOG(ERROR, "block(%u) backup is zero", block_id);
        return EXIT_NO_REPLICATE;
      }
      if (current_ds_size > 1)
      {
        TBSYS_LOG(ERROR, "block(%u) ds_size > 1, no need do emergency", block_id);
        return EXIT_GENERAL_ERROR;
      }

      if (current_ds_size >= SYSPARAM_NAMESERVER.min_replication_)
      {
        TBSYS_LOG(INFO, "block(%u), current_ds_size(%d) >= min_replication(%d), must be stop replicate", block_id,
            current_ds_size, SYSPARAM_NAMESERVER.min_replication_);
        return TFS_SUCCESS;
      }
      if (current_ds_size >= block_ds_map.get_ds_size())
      {
        TBSYS_LOG(ERROR, "block(%u), not found dest ds, current_ds_size(%u) >= total_ds_size(%u)", block_id,
            current_ds_size, block_ds_map.get_ds_size());
        return TFS_SUCCESS;
      }

      // --------------------------------------------------------
      // check current replicating status.
      // --------------------------------------------------------
      mutex_.wrlock();
      int32_t ret = check(block_id, SYSPARAM_NAMESERVER.replicate_max_time_ / 2, false, true);
      mutex_.unlock();
      if (ret == 0)
      {
        TBSYS_LOG(ERROR, "block(%u), replicating is exist", block_id);
        return TFS_SUCCESS;
      }

      // --------------------------------------------------------
      // check source and destination DataServerStatInfos.
      // --------------------------------------------------------
      uint64_t source_id = servers.back();
      mutex_.rdlock();
      int64_t replicating_count = ReplicateStrategy::get_ds_count(src_ds_counter_, source_id);
      ReplicateStrategy::counter_type counter = dest_ds_counter_;
      mutex_.unlock();

      if (replicating_count > SYSPARAM_NAMESERVER.replicate_max_count_per_server_)
      {
        TBSYS_LOG(
            ERROR,
            "replicate block(%u), cannot found a source ds, load over or replicate too many, \
                    source_id(%"PRI64_PREFIX"u), current replicating(%"PRI64_PREFIX"d)",
            block_id, source_id, replicating_count);
        return EXIT_GENERAL_ERROR;
      }

      {
        ScopedRWLock scoped_lock(block_ds_map.get_server_mutex(), READ_LOCKER);
        ret = elect_replicate_dest_ds(block_ds_map, counter, 1, servers);
      }

      if (ret == 0)
      {
        TBSYS_LOG(ERROR, "replicate block:%u, cannot found a dest server, all servers are load over", block_id);
        return EXIT_GENERAL_ERROR;
      }

      return send_replicate_cmd(source_id, servers.back(), block_id, REPLICATE_BLOCK_MOVE_FLAG_NO);
    }

    int ReplicateExecutor::send_replicate_cmd(const uint64_t source_id, const uint64_t destination_id,
        const uint32_t block_id, int32_t is_move)
    {
      ReplicateBlockMessage dsmessage;
      ReplBlock *replb = new ReplBlock();
      replb->block_id_ = block_id;
      replb->source_id_ = source_id;
      replb->destination_id_ = destination_id;
      replb->start_time_ = time(NULL);
      replb->is_move_ = is_move;
      BlockCollect *block_collect = meta_mgr_.get_block_ds_mgr().get_block_collect(block_id);
      replb->server_count_ = block_collect->get_ds()->size();

      dsmessage.set_repl_block(replb);
      dsmessage.set_command(COMMAND_REPLICATE);
      int iret = send_message_to_server(source_id, &dsmessage, NULL);
      if (iret != TFS_SUCCESS)
      {
        tbsys::gDelete(replb);
        TBSYS_LOG(ERROR, "send replicate command faild, block(%u), %s===>%s, is_move(%s)", block_id,
            tbsys::CNetUtil::addrToString(source_id).c_str(), tbsys::CNetUtil::addrToString(destination_id).c_str(),
            is_move == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes");
        return iret;
      }
      mutex_.wrlock();
      if (replicating_map_.find(block_id) == replicating_map_.end())
      {
        ReplicateStrategy::inc_ds_count(src_ds_counter_, source_id);
        ReplicateStrategy::inc_ds_count(dest_ds_counter_, destination_id);
        replicating_map_.insert(REPL_BLOCK_MAP::value_type(block_id, replb));
        replb = NULL;
      }
      mutex_.unlock();
      TBSYS_LOG(ERROR, "send replicate command successful, block(%u), %s===>%s, is_move(%s)", block_id,
          tbsys::CNetUtil::addrToString(source_id).c_str(), tbsys::CNetUtil::addrToString(destination_id).c_str(),
          is_move == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes");
      tbsys::gDelete(replb);
      return iret;
    }

    int ReplicateExecutor::complete(const ReplBlock* cmd, bool success)
    {
      uint32_t block_id = cmd->block_id_;
      uint64_t source_id = cmd->source_id_;
      uint64_t destination_id = cmd->destination_id_;
      int32_t is_move = cmd->is_move_;
      // when recv complete message, server_count used by load_error
      TBSYS_LOG(DEBUG, "Replicate Complete Result: block_id: %u,src_ds: %s, dest_ds: %s,"
        "move: %s, success: %s", block_id, tbsys::CNetUtil::addrToString(source_id).c_str(),
          tbsys::CNetUtil::addrToString(destination_id).c_str(), is_move == REPLICATE_BLOCK_MOVE_FLAG_YES ? "yes"
              : "no", success ? "true" : "false");
      int ret = TFS_SUCCESS;
      bool rsucc = false;
      LayoutManager& block_ds_map = meta_mgr_.get_block_ds_mgr();
      BlockCollect *block_collect = block_ds_map.get_block_collect(block_id);
      if (success && block_collect != NULL)
      {
        ServerCollect* destcol = block_ds_map.get_ds_collect(destination_id);
        if (destcol != NULL)
          block_ds_map.build_ds_block_relation(block_id, destination_id, false);
        // is move? release source_id & block_id
        if (is_move && block_collect->get_ds()->size() > static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_))
        {
          block_ds_map.release_ds_relation(block_id, source_id);
          ret = STATUS_MESSAGE_REMOVE;
        }
        block_collect->set_load_error(0);
        rsucc = true;
      }

      NsRuntimeGlobalInformation* ngi = meta_mgr_.get_fs_name_system()->get_ns_global_info();
      if (ngi->owner_role_ != NS_ROLE_MASTER) //only master do check
        return ret;

      mutex_.wrlock();
      int32_t delret = check(block_id, SYSPARAM_NAMESERVER.replicate_max_time_, true, false);
      mutex_.unlock();

      if (delret >= 0)
      {
        TBSYS_LOG(INFO, "%s %s, %s to %s block(%u) is_move(%s)", is_move ? "move" : "replicate", rsucc ? "succeed"
            : "failed", tbsys::CNetUtil::addrToString(source_id).c_str(),
            tbsys::CNetUtil::addrToString(destination_id).c_str(), block_id,
            is_move == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes");
      }
      else
      {
        TBSYS_LOG(INFO, "not found %s %s to %s block(%u) is_move(%s)", is_move ? "move" : "replicate",
            tbsys::CNetUtil::addrToString(source_id).c_str(), tbsys::CNetUtil::addrToString(destination_id).c_str(),
            block_id, is_move == REPLICATE_BLOCK_MOVE_FLAG_NO ? "no" : "yes");
      }
      return ret;
    }

    int ReplicateExecutor::execute(const vector<ReplBlock*> replicate_paln, void* args)
    {
      uint32_t replicate_paln_size = replicate_paln.size();
      TBSYS_LOG(DEBUG, "execute replicat plan size: %u", replicate_paln_size);
      uint32_t old_done = 0;
      uint32_t done = replicate_paln_size;
      uint32_t i = 0;
      int32_t retry = 0;
      ReplBlock* replicate_block = NULL;
      BlockCollect* block_collect = NULL;

      LayoutManager& block_ds_map = meta_mgr_.get_block_ds_mgr();

      ReplicateLauncher* laucher = static_cast<ReplicateLauncher*> (args);

      while (done && retry < 10)
      {
        old_done = done;
        done = 0;
        for (i = 0; i < replicate_paln_size; ++i)
        {
          if ((laucher->destroy_flag_ == NS_DESTROY_FLAGS_YES) || (laucher->pause_flag_
              & ReplicateLauncher::PAUSE_FLAG_PAUSE))
          {
            break;
          }

          replicate_block = replicate_paln.at(i);

          if (replicate_block->block_id_ == 0)
            continue;

          if (is_replicating_block(replicate_block, SYSPARAM_NAMESERVER.replicate_max_count_per_server_))
          {
            done++;
            continue;
          }
          block_collect = block_ds_map.get_block_collect(replicate_block->block_id_);

          if (block_collect == NULL || block_collect->get_ds()->size()
              >= static_cast<uint32_t> (SYSPARAM_NAMESERVER.min_replication_))
          {
            replicate_block->block_id_ = 0;
            continue;
          }

          send_replicate_cmd(replicate_block->source_id_, replicate_block->destination_id_, replicate_block->block_id_,
              REPLICATE_BLOCK_MOVE_FLAG_NO);

          replicate_block->block_id_ = 0;
        }

        TBSYS_LOG(INFO, "total: (u%), replicate successful total count(%u), not replicate total count(%u)",
            replicate_paln_size, done, replicating_map_.size());

        if ((laucher->destroy_flag_ == NS_DESTROY_FLAGS_YES) || (laucher->pause_flag_
            & ReplicateLauncher::PAUSE_FLAG_PAUSE))
        {
          break;
        }

        Func::sleep(SYSPARAM_NAMESERVER.replicate_check_interval_, reinterpret_cast<int32_t*> (&laucher->destroy_flag_));
        if ((laucher->destroy_flag_ == NS_DESTROY_FLAGS_YES) || (laucher->pause_flag_
            & ReplicateLauncher::PAUSE_FLAG_PAUSE))
        {
          break;
        }

        if (old_done == done)
        {
          retry++;
        }
        else
        {
          retry = 0;
        }
      }

      // free
      for (uint32_t j = 0; j < replicate_paln_size; ++j)
      {
        replicate_block = replicate_paln.at(j);
        tbsys::gDelete(replicate_block);
      }

      if (retry >= 10 && replicating_map_.size() == 0)
      {
        TBSYS_LOG(WARN, "retry full times but cannot get any improvement, is anything wrong? %u,%u",
            src_ds_counter_.size(), dest_ds_counter_.size());

        ScopedRWLock lock(mutex_, WRITE_LOCKER);
        src_ds_counter_.clear();
        dest_ds_counter_.clear();
      }
      TBSYS_LOG(DEBUG, "execute replicat plan over, retry count(%d)", retry);
      return TFS_SUCCESS;
    }

    bool ReplicateExecutor::is_replicating_block(const ReplBlock* cmd, const int64_t count)
    {
      ScopedRWLock lock(mutex_, WRITE_LOCKER);
      int iret = check(cmd->block_id_, SYSPARAM_NAMESERVER.replicate_max_time_, false, true);
      if (iret == -1)
      {
        TBSYS_LOG(INFO, "block(%u) not found", cmd->block_id_);
        return false;
      }
      bool ret = ReplicateStrategy::get_ds_count(src_ds_counter_, cmd->source_id_) >= count
          || ReplicateStrategy::get_ds_count(dest_ds_counter_, cmd->destination_id_) >= count;
      return ret;
    }

    bool ReplicateExecutor::is_replicating_block(const uint32_t block_id)
    {
      ScopedRWLock lock(mutex_, READ_LOCKER);
      return (check(block_id, SYSPARAM_NAMESERVER.replicate_max_time_, false, false) == 0);
    }

    /**
     *
     *@return 1: time out, 0: no time out, -1 : record not exist
     */
    int ReplicateExecutor::check(const uint32_t block_id, const time_t max_time, const bool complete,
        const bool timeout)
    {
      bool is_timeout = false;
      REPL_BLOCK_MAP_ITER iter = replicating_map_.find(block_id);
      if (iter != replicating_map_.end())
      {
        is_timeout = iter->second->start_time_ < (time(NULL) - max_time);
        if (complete || (timeout && is_timeout))
        {
          ReplicateStrategy::dec_ds_count(src_ds_counter_, iter->second->source_id_);
          ReplicateStrategy::dec_ds_count(dest_ds_counter_, iter->second->destination_id_);
          tbsys::gDelete(iter->second);
          replicating_map_.erase(iter);
        }
        return is_timeout ? 1 : 0;
      }
      return -1;
    }

    void ReplicateExecutor::clear_replicating_info()
    {
      ScopedRWLock lock(mutex_, WRITE_LOCKER);
      TBSYS_LOG(INFO, "clear_replicating_info current counter: source(%u), dest(%u), replicate map(%u)",
          src_ds_counter_.size(), dest_ds_counter_.size(), replicating_map_.size());
      src_ds_counter_.clear();
      dest_ds_counter_.clear();
      replicating_map_.clear();
    }

  }
}
