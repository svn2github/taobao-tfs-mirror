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

#include <time.h>
#include <iostream>
#include <functional>
#include <numeric>
#include <tbsys.h>
#include <Memory.hpp>
#include "strategy.h"
#include "ns_define.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "message/block_info_message.h"
#include "message/replicate_block_message.h"
#include "message/compact_block_message.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t LayoutManager::INTERVAL = 10;
    const int8_t LayoutManager::LOAD_BASE_MULTIPLE = 2;
    const int8_t LayoutManager::ELECT_SEQ_INITIALIE_NUM = 1;

    const std::string LayoutManager::dynamic_parameter_str[] = {
      "min_replication",
      "max_replication",
      "max_write_file_count",
      "max_use_capacity_ratio",
      "heart_interval",
      "replicate_wait_time",
      "compact_delete_ratio",
      "compact_max_load",
      "plan_run_flag",
      "run_plan_expire_interval",
      "run_plan_ratio",
      "object_dead_max_time",
      "balance_max_diff_block_num",
      "log_level",
      "add_primary_block_count",
      "build_plan_interval",
      "replicate_ratio",
      "max_wait_write_lease",
      "tmp",
      "cluster_index",
      "build_plan_default_wait_time"
  };

  static int find_servers_difference(const std::vector<ServerCollect*>& first,
                                     const std::vector<ServerCollect*>& second,
                                     std::vector<ServerCollect*>& result)
  {
    if (second.empty())
    {
      result.assign(first.begin(), first.end());
    }
    else
    {
      std::vector<ServerCollect*>::const_iterator iter = second.begin();
      std::vector<ServerCollect*>::const_iterator rt;
      for (; iter != second.end(); ++iter)
      {
        bool bfind = false;
        rt = first.begin();
        for (; rt != first.end(); rt++)
        {
          if ((*rt)->id() == (*iter)->id())
          {
            bfind = true;
            break;
          }
        }
        if (!bfind)
        {
          result.push_back((*iter));
        }
      }
    }
    return result.size();
  }

  LayoutManager::LayoutManager():
      build_plan_thread_(0),
      run_plan_thread_(0),
      check_dataserver_thread_(0),
      oplog_sync_mgr_(*this),
      block_chunk_(NULL),
      block_chunk_num_(0),
      write_index_(-1),
      write_second_index_(-1),
      last_rotate_log_time_(0),
      max_block_id_(0),
      alive_server_size_(0),
      interrupt_(INTERRUPT_NONE),
      plan_run_flag_(PLAN_RUN_FLAG_REPLICATE),
      client_request_server_(*this)
    {
      srand(time(NULL));
      tzset();
      zonesec_ = 86400 + timezone;
      plan_run_flag_ |= PLAN_RUN_FLAG_MOVE;
      plan_run_flag_ |= PLAN_RUN_FLAG_COMPACT;
      plan_run_flag_ |= PLAN_RUN_FLAG_DELETE;
    }

    LayoutManager::~LayoutManager()
    {
      build_plan_thread_ = 0;
      run_plan_thread_ = 0;
      check_dataserver_thread_ = 0;

      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        if (block_chunk_ != NULL)
          block_chunk_[i] = 0;
      }
      tbsys::gDeleteA(block_chunk_);

      SERVER_MAP::iterator iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
      servers_.clear();
      servers_index_.clear();

      {
        pending_plan_list_.clear();
        running_plan_list_.clear();
        finish_plan_list_.clear();
      }

      {
        block_to_task_.clear();
        server_to_task_.clear();
      }
    }

    ClientRequestServer& LayoutManager::get_client_request_server()
    {
      return client_request_server_;
    }

    int32_t LayoutManager::get_alive_server_size() const
    {
      return alive_server_size_;
    }

    int LayoutManager::initialize(const int32_t chunk_num)
    {
      block_chunk_num_ = chunk_num == 0 ? 32 : chunk_num > 1024 ? 1024 : chunk_num;
      TBSYS_LOG(INFO, "initialize %"PRI64_PREFIX"d block_chunks", block_chunk_num_);
      tbsys::gDeleteA(block_chunk_);
      block_chunk_ = new BlockChunkPtr[block_chunk_num_];
      for (int32_t i = 0; i < block_chunk_num_; i++)
      {
        block_chunk_[i] = new BlockChunk();
      }
      int32_t iret = TFS_ERROR;

#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      iret = oplog_sync_mgr_.initialize();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize oplog sync manager fail, must be exit, iret: %d", iret);
      }
      else
      {
      //initialize thread
      build_plan_thread_ = new BuildPlanThreadHelper(*this);
      check_dataserver_thread_ = new CheckDataServerThreadHelper(*this);
      run_plan_thread_ = new RunPlanThreadHelper(*this);
#if defined(TFS_NS_INTEGRATION)
      run_plan_thread_ = new RunPlanThreadHelper(*this);
#endif
      }
#endif
      return iret;
    }

    BlockChunkPtr LayoutManager::get_chunk(const uint32_t block_id) const
    {
      assert(block_chunk_num_ > 0);
      assert(block_chunk_ != NULL);
      return block_chunk_[block_id % block_chunk_num_];
    }

    ServerCollect* LayoutManager::get_server(const uint64_t server)
    {
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      return get_server_(server);
    }

    void LayoutManager::wait_for_shut_down()
    {
      if (build_plan_thread_ != 0)
      {
        build_plan_thread_->join();
      }
      if (run_plan_thread_ != 0)
      {
        run_plan_thread_->join();
      }
      if (check_dataserver_thread_ != 0)
      {
        check_dataserver_thread_->join();
      }

      oplog_sync_mgr_.wait_for_shut_down();
    }

    void LayoutManager::destroy()
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
        build_plan_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        run_plan_monitor_.notifyAll();
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.notifyAll();
      }
      oplog_sync_mgr_.destroy();
    }

    OpLogSyncManager& LayoutManager::get_oplog_sync_mgr()
    {
      return oplog_sync_mgr_;
    }

    int LayoutManager::open_helper_create_new_block_by_id(uint32_t block_id)
    {
      int32_t iret = TFS_SUCCESS;
      BlockChunkPtr ptr = get_chunk(block_id);
      ptr->rdlock();//lock
      BlockCollect* block = ptr->find(block_id);
      if (block == NULL)//block not found by block_id
      {
        ptr->unlock();//unlock
        block = add_new_block(block_id);
        if (block == NULL)
        {
          TBSYS_LOG(ERROR, "add new block: %u failed because there's any dataserver was found", block_id);
          iret = EXIT_NO_DATASERVER;
        }
        else
        {
          ptr->rdlock();
          block = ptr->find(block_id);
          if (block == NULL)
          {
            TBSYS_LOG(ERROR, "add new block: %u failed because there's any dataserver was found", block_id);
            iret = EXIT_NO_DATASERVER;
          }
        }
      }

      if (TFS_SUCCESS == iret)
      {
        if (NULL != block)
        {
          if (block->get_hold_size() == 0)
          {
            if (block->get_creating_flag() == BlockCollect::BLOCK_CREATE_FLAG_YES)
            {
              TBSYS_LOG(ERROR, "block: %u found meta data, but creating by another thread, must be return", block_id);
              iret = EXIT_NO_BLOCK;
            }
            else
            {
              TBSYS_LOG(ERROR, "block: %u found meta data, but no dataserver hold it.", block_id);
              iret = EXIT_NO_DATASERVER;
            }
          }
        }
      }
      ptr->unlock();
      return iret;
    }

    int LayoutManager::update_block_info(
        const BlockInfo& new_block_info,
        const uint64_t id,
        const time_t now,
        const bool addnew)
    {
      ServerCollect* server = get_server(id);
      if (NULL != server)
      {
        server->touch(now);
      }

      int32_t iret = TFS_SUCCESS;
      bool isnew = false;
      if (TFS_SUCCESS == iret)
      {
        BlockChunkPtr ptr = get_chunk(new_block_info.block_id_);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(new_block_info.block_id_);
        if ((block == NULL) && addnew)
        {
          block = ptr->add(new_block_info.block_id_, now);
          isnew = block != NULL;
          if (block != NULL && server != NULL)
          {
            iret = build_relation(block, server, now);
            if (iret != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "it's error that build relation betweed block: %u and server: %s",
                  new_block_info.block_id_, CNetUtil::addrToString(server->id()).c_str());
            }
          }
        }
        if (TFS_SUCCESS == iret)
        {
          iret = NULL == block ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "it's error that update block: %u information because block: %u not found",
                new_block_info.block_id_, new_block_info.block_id_);
          }
        }

        if (TFS_SUCCESS == iret)
        {
          if (block->version() > new_block_info.version_)//check version
          {
            //version error
            TBSYS_LOG(ERROR, "it's error that update block: %u information because old version: %d >= new version: %d",
                new_block_info.block_id_, block->version(), new_block_info.version_);
            iret = EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR;
          }
        }

        if (TFS_SUCCESS == iret)
        {
          block->update(new_block_info);//update block information

          if (block->is_relieve_writable_relation())
          {
            block->relieve_relation();
          }
        }
      }
      if (TFS_SUCCESS == iret)
      {
        //write oplog
        std::vector<uint32_t> blocks;
        std::vector<uint64_t> servers;
        blocks.push_back(new_block_info.block_id_);
        servers.push_back(id);
        iret = block_oplog_write_helper(isnew ? OPLOG_INSERT : OPLOG_UPDATE, new_block_info, blocks, servers);
      }
      return iret;
    }

    int LayoutManager::repair(const uint32_t block_id, const uint64_t server,
        const int32_t flag, const time_t now, std::string& error_msg)
    {
      TBSYS_LOG(DEBUG, "repair, block: %u, server: %s", block_id, tbsys::CNetUtil::addrToString(server).c_str());
      char msg[512] = {'\0'};
      std::vector<ServerCollect*> hold;
      int32_t iret = TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        BlockChunkPtr ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        iret = NULL == block ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS != iret)
          snprintf(msg, 512, "repair block, block collect not found by block: %u", block_id);
        else
          hold = block->get_hold();
      }

      int32_t hold_size = static_cast<int32_t>(hold.size());
      //need repair this block.
      if (TFS_SUCCESS == iret)
      {
        if ((flag == UPDATE_BLOCK_MISSING)
            && (hold_size >= SYSPARAM_NAMESERVER.min_replication_))
        {
          snprintf(msg, 512, "already got block: %u,  replicate: %d", block_id, hold_size);
          iret = EXIT_BLOCK_NOT_FOUND;//
        }
      }

      if (TFS_SUCCESS == iret)
      {
        iret = hold_size <= 0 ? EXIT_DATASERVER_NOT_FOUND : TFS_SUCCESS;
        if (TFS_SUCCESS != iret)
        {
          snprintf(msg, 512, "repair block: %u no any dataserver hold it", block_id);
        }
      }

      std::vector<ServerCollect*> runer;
      if (TFS_SUCCESS == iret)
      {
        std::vector<ServerCollect*>::iterator iter = hold.begin();
        for (; iter != hold.end(); ++iter)
        {
          if ((*iter)->id() != server)
          {
            runer.push_back((*iter));
            break;
          }
        }
        iret = runer.empty() ? EXIT_NO_DATASERVER : TFS_SUCCESS;
        if (TFS_SUCCESS != iret)
        {
          snprintf(msg, 512, "repair block: %u no any other dataserver: %d hold a correct replica", block_id, hold_size);
        }
      }

      if (TFS_SUCCESS == iret)
      {
        GFactory::get_lease_factory().cancel(block_id);
        ServerCollect* dest_server = get_server(server);
        runer.push_back(dest_server);

        BlockChunkPtr ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(block_id);
        iret = relieve_relation(block, dest_server, now) ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(WARN, "relieve relation failed between block: %u and server: %s", block_id, CNetUtil::addrToString(dest_server->id()).c_str());
        }
        else
        {
          ReplicateTaskPtr task = new ReplicateTask(this, PLAN_PRIORITY_EMERGENCY, block_id, now, now, runer, 0);
          iret = add_task(task) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(WARN, "add task(ReplicateTask) failed, block: %u", block_id);
          }
          else
          {
            task->dump(TBSYS_LOG_LEVEL_DEBUG, "repair,");
          }
        }
      }
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", msg);
        error_msg.append(msg);
      }
      return TFS_SUCCESS == iret ? STATUS_MESSAGE_REMOVE : iret;
    }

    int LayoutManager::scan(SSMScanParameter& param)
    {
      int32_t start = (param.start_next_position_ & 0xFFFF0000) >> 16;
      int32_t next  = 0;
      int32_t should= (param.should_actual_count_ & 0xFFFF0000) >> 16;
      int32_t actual= 0;
      int32_t jump_count = 0;
      bool    end = true;
      bool    all_over = true;
      bool    cutover_chunk = ((param.end_flag_) & SSM_SCAN_CUTOVER_FLAG_YES);

      if (param.type_ & SSM_TYPE_SERVER)
      {
        int16_t child_type = param.child_type_;
        if (child_type == SSM_CHILD_SERVER_TYPE_ALL)
        {
          child_type |= SSM_CHILD_SERVER_TYPE_HOLD;
          child_type |= SSM_CHILD_SERVER_TYPE_WRITABLE;
          child_type |= SSM_CHILD_SERVER_TYPE_MASTER;
          child_type |= SSM_CHILD_SERVER_TYPE_INFO;
        }
        bool has_server = start <= alive_server_size_ ;
        if (has_server)
        {
          int32_t jump_count = 0;
          uint64_t start_server = CNetUtil::ipToAddr(param.addition_param1_, param.addition_param2_);
          RWLock::Lock lock(server_mutex_, READ_LOCKER);
          SERVER_MAP::const_iterator iter = start_server <= 0 ? servers_.begin() : servers_.find(start_server);
          for (; iter != servers_.end(); ++iter, ++jump_count)
          {
            if (iter->second->is_alive())
            {
              ++actual;
              iter->second->scan(param, child_type);
              if (actual == should)
              {
                ++iter;
                ++jump_count;
                break;
              }
            }
          }
          all_over = iter == servers_.end();
          next = (actual == should && start + jump_count < alive_server_size_) ? start + jump_count : start;
          if (!all_over)
          {
            uint64_t host = iter->second->id();
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
      }
      else if (param.type_ & SSM_TYPE_BLOCK)
      {
        bool has_block = (start <= block_chunk_num_);
        if (has_block)
        {
          next = start;
          for (; next < block_chunk_num_; ++next, ++start, cutover_chunk = true)
          {
            RWLock::Lock lock(*(block_chunk_[next]),  READ_LOCKER);
            jump_count += block_chunk_[next]->scan(param, actual, end, should, cutover_chunk);
            if (actual == should)
            {
              next = end ? start + 1 : start;
              break;
            }
          }
          all_over = start < block_chunk_num_? start == block_chunk_num_ - 1? end ? true : false : false : true;
          cutover_chunk = end;
        }
      }
      next &= 0x0000FFFF;
      param.start_next_position_ &= 0xFFFF0000;
      param.start_next_position_ |= next;
      actual &= 0x0000FFFF;
      param.should_actual_count_ &= 0xFFFF0000;
      param.should_actual_count_ |= actual;
      param.end_flag_ = all_over ? SSM_SCAN_END_FLAG_YES : SSM_SCAN_END_FLAG_NO;
      param.end_flag_ <<= 4;
      param.end_flag_ &= 0xF0;
      param.end_flag_ |= cutover_chunk ? SSM_SCAN_CUTOVER_FLAG_YES : SSM_SCAN_CUTOVER_FLAG_NO;
      return TFS_SUCCESS;
    }

    int LayoutManager::dump_plan(tbnet::DataBuffer& stream)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      stream.writeInt32(running_plan_list_.size() + pending_plan_list_.size());
      std::set<TaskPtr, TaskCompare>::iterator iter = running_plan_list_.begin();
      for (; iter != running_plan_list_.end(); ++iter)
      {
        (*iter)->dump(stream);
      }
      iter = pending_plan_list_.begin();
      for (; iter != pending_plan_list_.end(); ++iter)
      {
        (*iter)->dump(stream);
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::dump_plan(void)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      std::set<TaskPtr, TaskCompare>::iterator iter = running_plan_list_.begin();
      for (; iter != running_plan_list_.end(); ++iter)
      {
        (*iter)->dump(TBSYS_LOG_LEVEL_DEBUG);
      }
      iter = pending_plan_list_.begin();
      for (; iter != pending_plan_list_.end(); ++iter)
      {
        (*iter)->dump(TBSYS_LOG_LEVEL_DEBUG);
      }
      return TFS_SUCCESS;
    }

    int LayoutManager::handle_task_complete(common::BasePacket* msg)
    {
      //handle complete message
      bool bret = msg != NULL;
      if (bret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (ngi.owner_role_ == NS_ROLE_MASTER)//master
        {
          std::vector<uint32_t> works;//find block
          switch (msg->getPCode())
          {
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
              {
                CompactBlockCompleteMessage* compact_msg = dynamic_cast<CompactBlockCompleteMessage*>(msg);
                works.push_back(compact_msg->get_block_id());
              }
              break;
            case REPLICATE_BLOCK_MESSAGE:
              {
                ReplicateBlockMessage* replicate_msg = dynamic_cast<ReplicateBlockMessage*>(msg);
                works.push_back(replicate_msg->get_repl_block()->block_id_);
              }
              break;
            case REMOVE_BLOCK_RESPONSE_MESSAGE:
              {
                RemoveBlockResponseMessage* rm_msg = dynamic_cast<RemoveBlockResponseMessage*>(msg);
                works.push_back(rm_msg->get_block_id());
              }
              break;
            default:
              TBSYS_LOG(WARN, "unkonw message PCode = %d", msg->getPCode());
              break;
          }

          std::vector<TaskPtr> complete;
          bool all_complete_flag = true;
          bool bfind = false;
          TaskPtr task = 0;
          std::vector<uint32_t>::iterator v_iter= works.begin();
          for (; v_iter != works.end(); ++v_iter, all_complete_flag = true)
          {
            {
              RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
              std::map<uint32_t, TaskPtr>::iterator iter = block_to_task_.find((*v_iter));
              bfind = iter != block_to_task_.end();
              if (bfind)
              {
                task = iter->second;
              }
            }
            if (bfind)
            {
              if (task->handle_complete(msg, all_complete_flag) != TFS_SUCCESS)
              {
                task->dump(TBSYS_LOG_LEVEL_ERROR, "handle complete message fail");
              }

              if (all_complete_flag)
              {
                complete.push_back(task);
              }

              task->dump(TBSYS_LOG_LEVEL_INFO, "handle message complete, show result");
            }
          }

          std::vector<TaskPtr>::iterator iter = complete.begin();
          {
            RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
            for (; iter != complete.end(); ++iter)//relieve reliation
            {
              task = *iter;
              block_to_task_.erase(task->block_id_);
              std::vector<ServerCollect*>::iterator r_iter = task->runer_.begin();
              for (; r_iter != task->runer_.end(); ++r_iter)
              {
                server_to_task_.erase((*r_iter));
              }
            }
          }

          tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
          iter = complete.begin();
          for (; iter != complete.end(); ++iter)
          {
            finish_plan_list_.push_back((*iter));

            GFactory::get_timer()->cancel((*iter));

            std::set<TaskPtr, TaskCompare>::iterator r_iter = running_plan_list_.find((*iter));
            if (r_iter != running_plan_list_.end())
              running_plan_list_.erase(r_iter);//remove task from running_plan_list
          }
        }
        else if (ngi.owner_role_ == NS_ROLE_SLAVE)//slave
        {
          bool all_complete_flag = true;
          std::vector<ServerCollect*> runer;
          TaskPtr task  = 0;
          switch (msg->getPCode())
          {
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
              task = new CompactTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer, 0);
              break;
            case REPLICATE_BLOCK_MESSAGE:
              {
                ReplicateBlockMessage* replicate_msg = dynamic_cast<ReplicateBlockMessage*>(msg);
                task = replicate_msg->get_move_flag() == REPLICATE_BLOCK_MOVE_FLAG_NO
                  ? new ReplicateTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer, 0)
                  : new MoveTask(this, PLAN_PRIORITY_NONE, 0, 0, 0, runer, 0);
                break;
              }
            default:
              TBSYS_LOG(WARN, "unkonw message PCode = %d", msg->getPCode());
              break;
          }
          if (task != 0)
          {
            if (task->handle_complete(msg, all_complete_flag) != TFS_SUCCESS)
            {
              TBSYS_LOG(ERROR, "%s", "slave handle message failed");
            }
            task = 0;
          }
        }
      }
      return bret ? TFS_SUCCESS : TFS_ERROR;
    }

    void LayoutManager::interrupt(const uint8_t interrupt, const time_t now)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.dump(TBSYS_LOG_LEVEL_INFO);
      if ((ngi.owner_role_ == NS_ROLE_MASTER)
          && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
          && (ngi.switch_time_ < now))
      {
        TBSYS_LOG(DEBUG, "%s", "notify build plan thread");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
        interrupt_ |= interrupt;
        build_plan_monitor_.notify();
      }
    }

    /*
     * expire blocks on DataServerStatInfo only post expire message to ds, dont care result.
     * @param [in] ds_id  DataServerStatInfo id the one who post to.
     * @param [in] block_id block id, the one need expired.
     * @return TFS_SUCCESS success.
     */
    int LayoutManager::rm_block_from_ds(const uint64_t ds_id, const uint32_t block_id)
    {
      TBSYS_LOG(INFO, "remove  block: %u on server : %s", block_id, tbsys::CNetUtil::addrToString(ds_id).c_str());
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      RemoveBlockMessage rbmsg;
      rbmsg.add_remove_id(block_id);

      BlockInfo info;
      memset(&info, 0, sizeof(info));
      info.block_id_ = block_id;
      std::vector<uint32_t> blocks;
      std::vector<uint64_t> servers;
      blocks.push_back(block_id);
      servers.push_back(ds_id);
      block_oplog_write_helper(OPLOG_RELIEVE_RELATION, info, blocks, servers);

      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = client->async_post_request(servers, &rbmsg, ns_async_callback);
      }
      return iret;
#else
      return TFS_SUCCESS;
#endif
    }

    int LayoutManager::rm_block_from_ds(const uint64_t ds_id, const std::vector<uint32_t>& blocks)
    {
      TBSYS_LOG(INFO, "remove  block count: %u on server : %s", blocks.size(),
          tbsys::CNetUtil::addrToString(ds_id).c_str());
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      RemoveBlockMessage rbmsg;
      rbmsg.set_remove_list(blocks);
      BlockInfo info;
      memset(&info, 0, sizeof(info));
      std::vector<uint64_t> servers;
      servers.push_back(ds_id);
      block_oplog_write_helper(OPLOG_RELIEVE_RELATION, info, blocks, servers);

      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = client->async_post_request(servers, &rbmsg, ns_async_callback);
      }
      return iret;
#else
      return TFS_SUCCESS;
#endif
    }

    ServerCollect* LayoutManager::get_server_(const uint64_t server)
    {
      SERVER_MAP::const_iterator iter = servers_.find(server);
      return ((iter == servers_.end())
          || (iter->second != NULL && !iter->second->is_alive())) ? NULL : iter->second;
    }

    /**
     * dataserver join this nameserver, update dataserver of information
     * and update global statistic information
     * @param[in] info: dataserver of information
     * @param[in] isnew: true if new dataserver
     * @param[in] now: current time
     * @return  return TFS_SUCCESS if success, otherwise failed
     */
    int LayoutManager::add_server(const DataServerStatInfo& info, const time_t now, bool& isnew)
    {
      //update server information
      isnew = false;
      ServerCollect* server_collect = NULL;
      {
        RWLock::Lock lock(server_mutex_, WRITE_LOCKER);
        SERVER_MAP::iterator iter = servers_.find(info.id_);
        if ((iter == servers_.end())
            || (iter->second == NULL)
            || (!iter->second->is_alive()))
        {
          if (iter != servers_.end())
          {
            --alive_server_size_;
            std::vector<ServerCollect*>::iterator where =
              std::find(servers_index_.begin(), servers_index_.end(), iter->second);
            if (where != servers_index_.end())
            {
              servers_index_.erase(where);
            }
            iter->second->set_dead_time(now);
            GFactory::get_gc_manager().add(iter->second);
            std::vector<stat_int_t> stat(1, iter->second->block_count());
            GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);
            servers_.erase(iter);
          }
          ARG_NEW(server_collect, ServerCollect, info, now);
          std::pair<SERVER_MAP::iterator, bool> res =
            servers_.insert(SERVER_MAP::value_type(info.id_, server_collect));
          if (!res.second)
          {
            tbsys::gDelete(server_collect);
            return TFS_ERROR;
          }
          isnew = true;
          ++alive_server_size_;
          servers_index_.push_back(server_collect);
          std::vector<stat_int_t> stat(1, info.block_count_);
          GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat);
        }
        else
        {
          server_collect = iter->second;
        }
      }
      server_collect->update(info, now, isnew);

      //update global statistic information
      GFactory::get_global_info().update(info, isnew);
      GFactory::get_global_info().dump();
      return TFS_SUCCESS;
    }

    /**
     * dataserver exit, when nameserver cannot keep touch with dataserver.
     * release all relation of blocks belongs to it
     * @param[in] server: dataserver's id (ip&port)
     * @param[in] now : current time
     * @return
     */
    int LayoutManager::remove_server(const uint64_t server, const time_t now)
    {
      TBSYS_LOG(WARN, "server: %s exit", CNetUtil::addrToString(server).c_str());
      //remove ServerCollect
      ScopedRWLock scoped_lock(server_mutex_, WRITE_LOCKER);
      SERVER_MAP::iterator iter = servers_.find(server);
      if (iter != servers_.end())
      {
        std::vector<stat_int_t> stat(1, iter->second->block_count());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);

        //release all relations of blocks belongs to it
        relieve_relation(iter->second, now);
        iter->second->dead();
        std::vector<ServerCollect*>::iterator where =
          std::find(servers_index_.begin(), servers_index_.end(), iter->second);
        if (where != servers_index_.end())
        {
          servers_index_.erase(where);
        }
        iter->second->set_dead_time(now);
        GFactory::get_gc_manager().add(iter->second);
        servers_.erase(iter);
        --alive_server_size_;
      }
      return TFS_SUCCESS;
    }

    /**
     * add new BlockCollect object base on block_id
     * @param[in] block_id : block id
     * @return  NULL if not found or add failed
     */
    BlockCollect* LayoutManager::add_block(uint32_t& block_id)
    {
      if (block_id == 0)
        return NULL;
      BlockChunkPtr ptr = get_chunk(block_id);
      return ptr->add(block_id, time(NULL));
    }

    int LayoutManager::add_new_block_helper_rm_block(const uint32_t block_id, std::vector<ServerCollect*>& servers)
    {
      int32_t iret = !servers.empty() && 0 != block_id ? TFS_SUCCESS : TFS_ERROR;
      NewClient* client = NewClientManager::get_instance().create_client();
      iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        int8_t count = 0;
        uint8_t send_id = 0;
        std::vector<uint64_t> success;
        std::vector<uint64_t> fail;
        std::vector<ServerCollect*>::iterator iter = servers.begin();
        RemoveBlockMessage rbmsg;
        rbmsg.add_remove_id(block_id);
        for (; iter != servers.end(); ++iter)
        {
          do
          {
            iret = client->post_request((*iter)->id(), &rbmsg, send_id);
          }
          while (count < 3 && TFS_SUCCESS != iret);
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return iret;
    }

    ServerCollect* LayoutManager::find_server_in_vec(const std::vector<ServerCollect*>& servers, const uint64_t server_id)
    {
      ServerCollect* object = NULL;
      std::vector<ServerCollect*>::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        if ((*iter)->id() == server_id)
        {
          object = (*iter);
          break;
        }
      }
      return object;
    }

    int LayoutManager::add_new_block_helper_write_log(const uint32_t block_id, const std::vector<ServerCollect*>& servers)
    {
      int32_t iret = block_id != 0 && !servers.empty() ?  TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        BlockInfo info;
        memset(&info, 0, sizeof(info));
        info.block_id_ = block_id;
        info.seq_no_ = 1;
        std::vector<uint32_t> blocks;
        std::vector<uint64_t> tmp;
        blocks.push_back(block_id);
        std::vector<ServerCollect*>::const_iterator iter = servers.begin();
        for (; iter != servers.end(); ++iter)
        {
          tmp.push_back((*iter)->id());
        }
        iret = block_oplog_write_helper(OPLOG_INSERT, info, blocks, tmp);
      }
      return iret;
    }

    int LayoutManager::add_new_block_helper_send_msg(const uint32_t block_id, const std::vector<ServerCollect*>& servers)
    {
      int32_t iret = !servers.empty() && block_id != 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
        iret = TFS_SUCCESS;
#else
        NewClient* client = NewClientManager::get_instance().create_client();
        iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
#endif
        if (TFS_SUCCESS == iret)
        {
          NewBlockMessage msg;
          msg.add_new_id(block_id);
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
          uint8_t send_id = 0;
#endif
          std::vector<ServerCollect*> send_msg_success;
          std::vector<ServerCollect*> send_msg_fail;
          std::vector<ServerCollect*>::const_iterator iter = servers.begin();
          for (; iter != servers.end(); ++iter)
          {
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
            send_msg_success.push_back((*iter));
#else
            //send add new block message to dataserver
            iret = client->post_request((*iter)->id(), &msg, send_id);
            if (TFS_SUCCESS != iret)
            {
              send_msg_fail.push_back((*iter));
              TBSYS_LOG(DEBUG, "send 'New block: %u' msg to server : %s fail",
                  block_id, CNetUtil::addrToString((*iter)->id()).c_str());
              break;
            }
            else
            {
              send_msg_success.push_back((*iter));
              TBSYS_LOG(DEBUG, "send 'New block: %u' msg to server : %s successful",
                  block_id, CNetUtil::addrToString((*iter)->id()).c_str());
            }
#endif
          }

#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
          if (!send_msg_success.empty())
          {
            std::vector<ServerCollect*> success;
            client->wait();
            NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
            NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
            if (TFS_SUCCESS == iret
                && send_msg_success.size() == servers.size())//post all message successful
            {
              iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                ServerCollect* object = NULL;
                NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
                StatusMessage* message = NULL;
                for (; iter != sresponse->end(); ++iter)
                {
                  message =  dynamic_cast<StatusMessage*>((iter->second.second));
                  if (STATUS_MESSAGE_OK == message->get_status())
                  {
                    object = find_server_in_vec(send_msg_success, iter->second.first);
                    if (NULL != object)
                    {
                      success.push_back(object);
                    }
                  }
                }
                if (success.size() != servers.size())//add block fail, rollback
                {
                  iret = TFS_ERROR;
                  std::string needs;
                  std::string success_servers;
                  print_servers(servers, needs);
                  print_servers(success, success_servers);
                  TBSYS_LOG(ERROR, "add block: %u fail, we'll rollback, servers: %s, success: %s",
                    block_id, needs.c_str(), success_servers.c_str());
                  add_new_block_helper_rm_block(block_id, success);
                }
                else
                {
                  std::string success_servers;
                  print_servers(success, success_servers);
                  TBSYS_LOG(INFO, "add block: %u on servers: %s successful", block_id, success_servers.c_str());
                }
              }
              else
              {
                std::string needs;
                print_servers(servers, needs);
                TBSYS_LOG(ERROR, "add block: %u, send message to server: %s fail", block_id, needs.c_str());
              }
            }
            else // post message fail, rollback
            {
              iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS == iret)
                {
                  ServerCollect* object = NULL;
                  NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
                  StatusMessage* message = NULL;
                  for (; iter != sresponse->begin(); ++iter)
                  {
                    message =  dynamic_cast<StatusMessage*>((iter->second.second));
                    if (STATUS_MESSAGE_OK == message->get_status())
                    {
                      object = find_server_in_vec(send_msg_success, iter->second.first);
                      if (NULL != object)
                      {
                        success.push_back(object);
                      }
                    }
                  }
                  iret = TFS_ERROR;
                  add_new_block_helper_rm_block(block_id, success);
                }
              }
              std::string success_servers;
              std::string needs;
              print_servers(servers, needs);
              print_servers(success, success_servers);
              TBSYS_LOG(ERROR, "add block: %u fail, we'll rollback, servers: %s, success: %s",
                  block_id, needs.c_str(), success_servers.c_str());
            }
          }
#endif
        }
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        NewClientManager::get_instance().destroy_client(client);
#endif
      }
      return iret;
    }

    int LayoutManager::add_new_block_helper_build_relation(const uint32_t block_id, const std::vector<ServerCollect*>& servers)
    {
      //build relation
      std::vector<ServerCollect*>::const_iterator iter = servers.begin();
      time_t now = time(NULL);
      BlockChunkPtr ptr = get_chunk(block_id);
      RWLock::Lock lock(*ptr, WRITE_LOCKER);
      BlockCollect* block = ptr->find(block_id);
      int32_t iret = NULL != block ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        for (; iter != servers.end(); ++iter)
        {
          iret = build_relation(block, (*iter), now);
          if (iret != TFS_SUCCESS)
          {
            TBSYS_LOG(WARN, "build relation fail between dataserver: %s and block: %u", CNetUtil::addrToString((*iter)->id()).c_str(), block->id());
            break;
          }
        }
        //create new block complete
        block->set_create_flag();
      }
      return iret;
    }

    BlockCollect* LayoutManager::add_new_block_helper_create_by_id(uint32_t block_id, time_t now)
    {
      BlockCollect* block = NULL;
      int32_t iret = block_id != 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        if (alive_server_size_ > 0)
        {
          BlockChunkPtr ptr = 0;
          std::vector<ServerCollect*> need;
          std::vector<ServerCollect*> exist;
          bool new_create_block_collect = false;
          {
            ptr = get_chunk(block_id);
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            block = ptr->find(block_id);
            if (NULL == block)//block not found in nameserver meta
            {
              block = ptr->add(block_id, now);//create block && insert block map
              if (NULL != block)
              {
                new_create_block_collect = true;
                block->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
              }
            }
            iret = NULL != block ? TFS_SUCCESS : TFS_ERROR;
            if (NULL != block && block->get_hold_size() > 0)
            {
              need.assign(block->get_hold().begin(), block->get_hold().end());
              exist.assign(block->get_hold().begin(), block->get_hold().end());
            }
          }

          if (TFS_SUCCESS == iret)//find or create block successful
          {
            int32_t count = SYSPARAM_NAMESERVER.min_replication_ - need.size();
            if (count > 0)
            {
              {
                RWLock::Lock lock(server_mutex_, READ_LOCKER);
                elect_write_server(*this, count, need);
              }

              if (static_cast<int32_t>(need.size()) < SYSPARAM_NAMESERVER.min_replication_)
              {
                iret = TFS_ERROR;
                TBSYS_LOG(ERROR, "create block: %u by block id fail, dataserver is not enough", block_id);
                if (new_create_block_collect)
                {
                  RWLock::Lock lock(*ptr, WRITE_LOCKER);
                  ptr->remove(block_id);
                }
              }
              else//elect dataserver successful
              {
                std::vector<ServerCollect*> servers;
                //std::set_difference(need.begin(), need.end(), exist.begin(), exist.end(), servers.begin(), ServerSetDifferencHelper());
                find_servers_difference(need, exist, servers);
                iret = servers.empty() ? TFS_ERROR : TFS_SUCCESS;
                if (TFS_SUCCESS == iret)
                {
                  iret = add_new_block_helper_send_msg(block_id, servers);
                  if (TFS_SUCCESS == iret)
                  {
                    //build relation
                    iret = add_new_block_helper_build_relation(block_id, servers);
                    if (TFS_SUCCESS == iret)
                    {
                      add_new_block_helper_write_log(block_id, servers);
                    }
                  }//end send message to dataserver successful
                }
              }//end elect dataserver successful
            }//end if (count >0)
          }//end find or create block successful
        }//end if(alive_server_size> 0)
      }//end if (bret)
      return iret == TFS_SUCCESS ? block : NULL;
    }

    BlockCollect* LayoutManager::add_new_block_helper_create_by_system(uint32_t& block_id, ServerCollect* server, time_t now)
    {
      BlockCollect* block = NULL;
      int32_t iret = block_id != 0 || alive_server_size_ <= 0 ? TFS_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == iret)
      {
        std::vector<ServerCollect*> need;
        if (server != NULL)
        {
          int64_t use_capacity = GFactory::get_global_info().use_capacity_ <= 0 ? alive_server_size_ : GFactory::get_global_info().use_capacity_;
          if (server->is_writable(use_capacity/alive_server_size_))
          {
            need.push_back(server);
          }
        }
        int32_t count = SYSPARAM_NAMESERVER.max_replication_ - need.size();
        if (count > 0)
        {
          RWLock::Lock lock(server_mutex_, READ_LOCKER);
          elect_write_server(*this, count, need);
        }
        iret = static_cast<int32_t>(need.size()) >= SYSPARAM_NAMESERVER.min_replication_ ? TFS_SUCCESS : TFS_ERROR; 
        if (TFS_SUCCESS == iret)
        {
          BlockChunkPtr ptr = 0;
          block_id = get_alive_block_id();
          iret = INVALID_BLOCK_ID == block_id ? TFS_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == iret)
          {
            //add block collect object
            ptr = get_chunk(block_id);
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            block = ptr->add(block_id, now);
            iret = NULL != block ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              block->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
            }
            else
            {
              TBSYS_LOG(ERROR, "add new block: %u fail", block_id);
            }
          }
          if (TFS_SUCCESS == iret)//add block collect object successful
          {
            iret = add_new_block_helper_send_msg(block_id, need);
            if (TFS_SUCCESS == iret)
            {
              //build relation
              iret = add_new_block_helper_build_relation(block_id, need);
              if (TFS_SUCCESS == iret)
              {
                add_new_block_helper_write_log(block_id, need);
              }
            }//end send message to dataserver successful
            else
            {
              if (0 != ptr)
              {
                RWLock::Lock lock(*ptr, WRITE_LOCKER);
                ptr->remove(block_id);
              }
            }
          }
        }
      }//end if (TFS_SUCCESS == iret) check parameter
      return TFS_SUCCESS == iret ? block : NULL;
    }

    BlockCollect* LayoutManager::add_new_block(uint32_t& block_id, ServerCollect* server, time_t now)
    {
      return block_id != 0 ? add_new_block_helper_create_by_id(block_id, now)
        : add_new_block_helper_create_by_system(block_id, server, now);
    }
    /**
     * dsataserver start, send heartbeat message to nameserver.
     * update all relations of blocks belongs to it
     * @param [in] dsInfo: dataserver system info , like capacity, load, etc..
     * @param [in] blocks: data blocks' info which belongs to dataserver.
     * @param [out] expires: need expire blocks
     * @return success or failure
     */
    int LayoutManager::update_relation(ServerCollect* server, const std::vector<BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires, const time_t now)
    {
      int32_t iret = ((server != NULL && server->is_alive())) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        //release relation
        iret = relieve_relation(server, now) ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(WARN, "relieve relation failed in dataserver: %s", CNetUtil::addrToString(server->id()).c_str());
        }
        else
        {
          uint32_t blocks_size = blocks.size();
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          for (uint32_t i = 0; i < blocks_size; ++i)
          {
            if (blocks[i].block_id_ == 0)
            {
              TBSYS_LOG(WARN, "dataserver: %s report, block == 0", tbsys::CNetUtil::addrToString(server->id()).c_str());
              continue;
            }

            bool first = false;
            bool force_be_master = false;

            // check block version, rebuilding relation.
            BlockChunkPtr ptr = get_chunk(blocks[i].block_id_);
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            BlockCollect* block = ptr->find(blocks[i].block_id_);
            if (block == NULL)
            {
              TBSYS_LOG(INFO, "block: %u not found in dataserver: %s, must be create",
                  blocks[i].block_id_, tbsys::CNetUtil::addrToString(server->id()).c_str());
              block = ptr->add(blocks[i].block_id_, now);
              first = true;
            }
            if (!block->check_version(server, ngi.owner_role_, first,
                  blocks[i], expires, force_be_master, now))
            {
              continue;//version error, not argeed
            }

            //build relation
            iret = build_relation(block, server, force_be_master);
            if (iret != TFS_SUCCESS)
            {
              TBSYS_LOG(WARN, "build relation fail between dataserver: %s and block: %u", CNetUtil::addrToString(server->id()).c_str(), block->id());
              break;
            }
          }
        }
      }
      return iret;
    }

    int LayoutManager::build_relation(BlockCollect* block, ServerCollect* server, const time_t now, const bool force)
    {
      int32_t iret = (block != NULL && server != NULL && server->is_alive()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        bool writable = false;
        BlockChunkPtr ptr = get_chunk(block->id());
        iret = ptr->connect(block, server, now, force, writable) ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS != iret)
        {
          ptr->remove(block->id());
          TBSYS_LOG(WARN, "build relation fail between dataserver: %s and block: %u", CNetUtil::addrToString(server->id()).c_str(), block->id());
        }
        else
        {
          //build relation between dataserver and block
          //add to dataserver's all kind of list
          iret = server->add(block, writable) ? TFS_SUCCESS : TFS_ERROR;
        }
      }
      return iret;
    }

    bool LayoutManager::relieve_relation(BlockCollect* block, ServerCollect* server, time_t now)
    {
      bool bret= (block != NULL &&  server != NULL);
      if (bret)
      {
        //release relation between block and dataserver
        bool bremove = block->remove(server, now);
        if (!bremove)
        {
          TBSYS_LOG(ERROR, "failed when relieve between block: %u and dataserver: %s",
              block->id(), CNetUtil::addrToString(server->id()).c_str());
        }
        bool sremove = server->remove(block);
        if (!sremove)
        {
          TBSYS_LOG(ERROR, "failed when relieve between block: %u and dataserver: %s",
              block->id(), CNetUtil::addrToString(server->id()).c_str());
        }
        bret = bremove && sremove;
      }
      return bret;
    }

    bool LayoutManager::relieve_relation(ServerCollect* server, const time_t now)
    {
      bool bret = server != NULL;
      return bret ? server->clear(*this, now) : bret;
    }

    void LayoutManager::rotate(time_t now)
    {
      if ((now % 86400 >= zonesec_)
          && (now % 86400 < zonesec_ + 300)
          && (last_rotate_log_time_ < now - 600))
      {
        last_rotate_log_time_ = now;
        oplog_sync_mgr_.rotate();
        TBSYS_LOGGER.rotateLog(NULL);
      }
    }

    uint32_t LayoutManager::get_alive_block_id()
    {
      uint32_t block_id = oplog_sync_mgr_.generation();
      while (true)
      {
        BlockChunkPtr ptr = get_chunk(block_id);
        RWLock::Lock lock(*ptr, READ_LOCKER);
        if (!ptr->exist(block_id))
          break;
        block_id = oplog_sync_mgr_.generation();
      }
      return block_id;
    }

    int LayoutManager::touch(uint64_t server, time_t now, bool promote)
    {
      ServerCollect* object = NULL;
      object = get_server(server);
      return touch(object, now, promote);
    }

    int64_t LayoutManager::calc_all_block_bytes() const
    {
      int64_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        ret += block_chunk_[i]->calc_all_block_bytes();
      }
      return ret;
    }

    int64_t LayoutManager::calc_all_block_count() const
    {
      int64_t ret = 0;
      for (int32_t i = 0; i < block_chunk_num_; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        ret += block_chunk_[i]->calc_size();
      }
      return ret;
    }

    BlockCollect* LayoutManager::elect_write_block()
    {
      BlockCollect* block = NULL;
      ServerCollect* server = NULL;
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      const int32_t count = static_cast<int32_t>(servers_index_.size());
      int64_t loop = 0;
      int64_t index = 0;

      while(count > 0 && block == NULL && loop < count)
      {
        ++loop;
        {
          tbutil::Mutex::Lock r_lock(elect_index_mutex_);
          ++write_index_;
          if (write_index_ >= count)
            write_index_ = 0;
          index = write_index_;
        }
        server = servers_index_[index];
        block = server->elect_write_block();
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "block: %u server: %s, write_index: %"PRI64_PREFIX"d, count: %u", block != NULL ? block->id() : 1, tbsys::CNetUtil::addrToString(server->id()).c_str(), index, count);
#endif
      }

      loop = 0;
      while(count > 0 && block == NULL && loop < count)
      {
        ++loop;
        {
          tbutil::Mutex::Lock r_lock(elect_index_mutex_);
          ++write_second_index_;
          if (write_second_index_ >= count)
            write_second_index_ = 0;
          index = write_second_index_;
        }
        server = servers_index_[index];
        block = server->force_elect_write_block();
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "block: %u server: %s, second_write_index: %"PRI64_PREFIX"d, count: %u", block != NULL ? block->id() : 0, tbsys::CNetUtil::addrToString(server->id()).c_str(), index, count);
#endif
      }
      return block;
    }

    /**
     * dataserver is the need to add testing of new block
     * @param[in] server: dataserver object
     * @param[in] promote:  true: check writable block, false: no check
     * @return: return 0 if no need add, otherwise need block count
     */
    int LayoutManager::touch(ServerCollect* server, time_t now, bool promote)
    {
      int32_t iret = server != NULL ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        int32_t count = SYSPARAM_NAMESERVER.add_primary_block_count_;
        server->touch(*this, now, promote, count);
        if (GFactory::get_runtime_info().owner_role_ == NS_ROLE_MASTER)
        {
          TBSYS_LOG(DEBUG, "need add new block count: %d", count);
          uint32_t new_block_id = 0;
          BlockCollect* block = NULL;
          for (int32_t i = 0; i < count; i++, new_block_id = 0)
          {
            block = add_new_block(new_block_id, server, now);
            if (NULL == block)
            {
              iret = TFS_ERROR;
              TBSYS_LOG(ERROR, "add block: %u failed", new_block_id);
              break;
            }
          }
        }
      }
      return iret;
    }

    void LayoutManager::check_server()
    {
      {
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.safe_mode_time_));
#endif
      }
      bool isnew = true;
      VUINT64 dead_servers;
      NsGlobalStatisticsInfo stat_info;
      ServerCollect *server = NULL;
      std::list<ServerCollect*> alive_servers;
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
#endif
      {
        server = NULL;
        dead_servers.clear();
        alive_servers.clear();
        memset(&stat_info, 0, sizeof(NsGlobalStatisticsInfo));
        time_t now = time(NULL);
        {
          //check dataserver is alive
          RWLock::Lock lock(server_mutex_, WRITE_LOCKER);
          SERVER_MAP_ITER iter = servers_.begin();
          for (; iter != servers_.end(); ++iter)
          {
            server = iter->second;
            if (!server->is_alive(now))
            {
              if (test_server_alive(server->id()) == TFS_SUCCESS)
              {
                server->touch(now);
              }
              else
              {
                server->dead();
                dead_servers.push_back(server->id());
              }
            }
            else
            {
              server->statistics(stat_info, isnew);
              alive_servers.push_back(server);
            }
          }
        }

        // write global information
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(INFO, "dead dataserver size: %u, alive dataserver size: %u", dead_servers.size(), alive_servers.size());
        GFactory::get_global_info().dump();
#endif
        GFactory::get_global_info().update(stat_info);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        GFactory::get_global_info().dump();
#endif

        std::list<ServerCollect*>::iterator it = alive_servers.begin();
        for (; it != alive_servers.end(); ++it)
        {
          TBSYS_LOG(DEBUG, "server touch, block count: %u, master block count: %u", (*it)->block_count(), (*it)->get_hold_master_size());
          touch((*it), now, true);
        }

        VUINT64::iterator iter = dead_servers.begin();
        for (; iter != dead_servers.end(); ++iter)
        {
          remove_server((*iter), now);
        }

        if (!dead_servers.empty())
        {
          interrupt(INTERRUPT_ALL, now);
        }
        tbutil::Monitor<tbutil::Mutex>::Lock lock(check_server_monitor_);
        check_server_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
      }
    }

    int LayoutManager::block_oplog_write_helper(const int32_t cmd, const BlockInfo& info, const std::vector<uint32_t>& blocks, const std::vector<uint64_t>& servers)
    {
      BlockOpLog oplog;
      memset(&oplog, 0, sizeof(oplog));
      oplog.info_ = info;
      oplog.cmd_ = cmd;
      oplog.blocks_ = blocks;
      oplog.servers_ = servers;
      int64_t size = oplog.length();
      int64_t pos = 0;
      char buf[size];
      int32_t iret = oplog.serialize(buf, size, pos);
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", "oplog serialize error");
      }
      else
      {
        // treat log operation as a trivial thing..
        // don't rollback the insert operation, cause block meta info
        // build from all dataserver, log info not been very serious.
        iret = oplog_sync_mgr_.log(OPLOG_TYPE_BLOCK_OP, buf, size);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "write oplog failed, block: %u", info.block_id_);
        }
      }
      return iret;
    }

    int32_t LayoutManager::AddLoad::operator()(const int32_t acc, const ServerCollect* const server)
    {
      assert(server != NULL);
      return acc + server->load();
    }

    bool LayoutManager::BlockNumComp::operator()(const common::DataServerStatInfo& x,
        const common::DataServerStatInfo& y)
    {
      return x.block_count_ < y.block_count_;
    }
    LayoutManager::GetAliveServer::GetAliveServer(common::VUINT64& servers)
      :servers_(servers)
    {
    }

    bool LayoutManager::GetAliveServer::operator() (const std::pair<uint64_t, ServerCollect*>& node)
    {
      if (node.second->is_alive())
      {
        servers_.push_back(node.second->id());
        return true;
      }
      return false;
    }

    int LayoutManager::set_runtime_param(uint32_t value1, uint32_t value2, char *retstr)
    {
      bool bret = retstr != NULL;
      if (bret)
      {
        retstr[0] = '\0';
        int32_t index = (value1 & 0x0FFFFFFF);
        int32_t set = (value1& 0xF0000000);
        int32_t tmp = 0;
        int32_t* param[] =
        {
          &SYSPARAM_NAMESERVER.min_replication_,
          &SYSPARAM_NAMESERVER.max_replication_,
          &SYSPARAM_NAMESERVER.max_write_file_count_,
          &SYSPARAM_NAMESERVER.max_use_capacity_ratio_,
          &SYSPARAM_NAMESERVER.heart_interval_,
          &SYSPARAM_NAMESERVER.replicate_wait_time_,
          &SYSPARAM_NAMESERVER.compact_delete_ratio_,
          &SYSPARAM_NAMESERVER.compact_max_load_,
          &plan_run_flag_,
          &SYSPARAM_NAMESERVER.run_plan_expire_interval_,
          &SYSPARAM_NAMESERVER.run_plan_ratio_,
          &SYSPARAM_NAMESERVER.object_dead_max_time_,
          &SYSPARAM_NAMESERVER.balance_max_diff_block_num_,
          &TBSYS_LOGGER._level,
          &SYSPARAM_NAMESERVER.add_primary_block_count_,
          &SYSPARAM_NAMESERVER.build_plan_interval_,
          &SYSPARAM_NAMESERVER.replicate_ratio_,
          &SYSPARAM_NAMESERVER.max_wait_write_lease_,
          &tmp,
          &SYSPARAM_NAMESERVER.cluster_index_,
          &SYSPARAM_NAMESERVER.build_plan_default_wait_time_,
        };
        int32_t size = sizeof(param) / sizeof(int32_t*);
        if (index < 0x01 || index > size)
        {
          snprintf(retstr, 256, "index : %d invalid.", index);
          TBSYS_LOG(ERROR, "index: %d invalid.", index);
          return TFS_SUCCESS;
        }
        int32_t* current_value = param[index - 1];
        if (set)
        {
          *current_value = (int32_t)(value2 & 0xFFFFFFFF);
        }
        else
        {
          snprintf(retstr, 256, "%d", *current_value);
        }
        TBSYS_LOG(INFO, "index: %d set: %d name: %s value: %d", index, set, dynamic_parameter_str[index - 1].c_str(), *current_value);
      }
      return bret ? TFS_SUCCESS : TFS_ERROR;
    }

    int LayoutManager::build_plan()
    {
      bool bwait = true;
      bool interrupt = true;
      int64_t emergency_replicate_count = 0;
      int64_t current_plan_seqno = 1;
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      {
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
        build_plan_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.safe_mode_time_));
#endif
      }
#if (!defined(TFS_NS_GTEST))
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
#endif
      {
        time_t now = time(NULL);
        int64_t should  = static_cast<int64_t>((alive_server_size_ * SYSPARAM_NAMESERVER.run_plan_ratio_)/ 100);
        int64_t current = 0;
        int64_t need =  0;
        int64_t adjust = 0;
        int64_t total = 0;
        {
          tbutil::Monitor<tbutil::Mutex>::Lock lock(build_plan_monitor_);
          if (ngi.owner_role_ == NS_ROLE_SLAVE)
          {
            build_plan_monitor_.wait();
          }

          time_t wait_time = interrupt ? 0 : !bwait ? SYSPARAM_NAMESERVER.build_plan_default_wait_time_ : ngi.switch_time_ > now ? ngi.switch_time_ - now : SYSPARAM_NAMESERVER.build_plan_interval_;
          bwait = true;
          interrupt = false;
          build_plan_monitor_.timedWait(tbutil::Time::seconds(wait_time));

          current = get_pending_plan_size() + get_running_plan_size();
          need    = should - current;
          total   = need;

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "wait_time: %"PRI64_PREFIX"d, switch_time: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d", wait_time, ngi.switch_time_, now);
#endif
        }

        //checkpoint
        rotate(now);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        TBSYS_LOG(DEBUG, "SYSPARAM_NAMESERVER.run_plan_ratio_: %d, alive_server_size_: %d", SYSPARAM_NAMESERVER.run_plan_ratio_, alive_server_size_);
#endif

        if (need <= 0)
        {
          bwait = !(current >= should && current > 0);
          TBSYS_LOG(WARN, "plan size: %"PRI64_PREFIX"d > should: %"PRI64_PREFIX"d, nothing to do", current, should);
        }

        TBSYS_LOG(INFO, "current plan size: %"PRI64_PREFIX"d, should: %"PRI64_PREFIX"d, need: %"PRI64_PREFIX"d",
            current, should, need);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
        std::vector<uint32_t> blocks;
        bool bret = false;
        if ((plan_run_flag_ & PLAN_RUN_FLAG_REPLICATE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build replicate plan failed");
          }
          TBSYS_LOG(INFO, "adjust: %"PRI64_PREFIX"d, remainder emergency replicate count: %"PRI64_PREFIX"d",
              adjust, emergency_replicate_count);
          bwait = (emergency_replicate_count <= 0);
          emergency_replicate_count = 0;
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_MOVE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_balance_plan(current_plan_seqno, now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build balance plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_COMPACT)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_compact_plan(current_plan_seqno, now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build compact plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_DELETE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_redundant_plan(current_plan_seqno, now, need, blocks);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build redundant plan failed");
          }
        }

#else
        bool bret = false;
        if ((plan_run_flag_ & PLAN_RUN_FLAG_REPLICATE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build replicate plan failed");
          }
          TBSYS_LOG(INFO, "adjust: %"PRI64_PREFIX"d, remainder emergency replicate count: %"PRI64_PREFIX"d",
              adjust, emergency_replicate_count);
          bwait = (emergency_replicate_count <= 0);
          emergency_replicate_count = 0;
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_MOVE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_balance_plan(current_plan_seqno, now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build balance plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_COMPACT)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_compact_plan(current_plan_seqno, now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build compact plan failed");
          }
        }

        if ((plan_run_flag_ & PLAN_RUN_FLAG_DELETE)
            && (!(interrupt_ & INTERRUPT_ALL))
            && (need > 0))
        {
          bret = build_redundant_plan(current_plan_seqno, now, need);
          if (!bret)
          {
            TBSYS_LOG(ERROR, "%s", "build redundant plan failed");
          }
        }
#endif
        if((interrupt_ & INTERRUPT_ALL))
        {
          interrupt = true;
          tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
          TBSYS_LOG(INFO, "receive interrupt: %d, pending plan list size: %u", interrupt_, pending_plan_list_.size());
          std::set<TaskPtr, TaskCompare>::iterator iter = pending_plan_list_.begin();
          for (; iter != pending_plan_list_.end(); ++iter)
            finish_plan_list_.push_back((*iter));
          pending_plan_list_.clear();
        }
        interrupt_ = INTERRUPT_NONE;
        TBSYS_LOG(INFO, "build plan complete, complete: %"PRI64_PREFIX"d", ((total  + adjust)- need));
        ++current_plan_seqno;
      }
      return TFS_SUCCESS;
    }

    void LayoutManager::run_plan()
    {
      const NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      {
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        run_plan_monitor_.timedWait(tbutil::Time::seconds(SYSPARAM_NAMESERVER.safe_mode_time_));
#endif
      }

      int32_t iret = TFS_ERROR;
      TaskPtr task = 0;
#if !defined(TFS_NS_GTEST)
      while (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
#endif
      {
        task = 0;
        run_plan_monitor_.lock();
        while ((pending_plan_list_.empty())
            && (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES))
        {
          run_plan_monitor_.wait();
        }
        if (!pending_plan_list_.empty())//find task
        {
          std::set<TaskPtr, TaskCompare>::const_iterator iter = pending_plan_list_.begin();
          if (iter != pending_plan_list_.end())
          {
            task = (*iter);
          }
        }
        run_plan_monitor_.unlock();

        //handle task
        if (0 != task)
        {
          iret = task->handle();
          if (TFS_SUCCESS != iret)
          {
            task->dump(TBSYS_LOG_LEVEL_ERROR, "task handle fail");
            RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
            block_to_task_.erase(task->block_id_);
            std::vector<ServerCollect*>::iterator i_iter = task->runer_.begin();
            for (; i_iter != task->runer_.end(); ++i_iter)
            {
              server_to_task_.erase((*i_iter));
            }
          }

          run_plan_monitor_.lock();
          if (TFS_SUCCESS == iret)
          {
            std::pair<std::set<TaskPtr, TaskCompare>::iterator, bool> res = running_plan_list_.insert((task));
            if (!res.second)
            {
              TBSYS_LOG(WARN, "%s", "task exist");
            }
            else
            {
              GFactory::get_timer()->schedule(task, tbutil::Time::seconds(task->end_time_ - task->begin_time_));
            }
          }
          std::set<TaskPtr, TaskCompare>::const_iterator iter = pending_plan_list_.find(task);
          if (pending_plan_list_.end() != iter)
          {
            pending_plan_list_.erase(iter);
          }
          else
          {
            task->dump(TBSYS_LOG_LEVEL_DEBUG, "task object not found in pending_plan_list,");
          }
          run_plan_monitor_.unlock();
        }
      }
    }

    void LayoutManager::destroy_plan()
    {
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
        pending_plan_list_.clear();
        running_plan_list_.clear();
        finish_plan_list_.clear();
      }

      {
        RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
        block_to_task_.clear();
        server_to_task_.clear();
      }
    }

    void LayoutManager::find_need_replicate_blocks(const int64_t need,
        const time_t now,
        int64_t& emergency_replicate_count,
        std::multimap<PlanPriority, BlockCollect*>& middle)
    {
      for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
        BLOCK_MAP::const_iterator iter = blocks.begin();
        for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          PlanPriority level = PLAN_PRIORITY_NONE;
          if ((level = iter->second->check_replicate(now)) >= PLAN_PRIORITY_NORMAL)
          {
            if (level == PLAN_PRIORITY_EMERGENCY)
            {
              ++emergency_replicate_count;
            }
            middle.insert(std::pair<PlanPriority, BlockCollect*>(level, iter->second));
          }
        }
      }
    }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_replicate_plan(const int64_t plan_seqno,
        const time_t now,
        int64_t& need,
        int64_t& adjust,
        int64_t& emergency_replicate_count,
        std::vector<uint32_t>& blocks)
#else
      bool LayoutManager::build_replicate_plan(const int64_t plan_seqno,
          const time_t now,
          int64_t& need,
          int64_t& adjust,
          int64_t& emergency_replicate_count)
#endif
      {
        std::multimap<PlanPriority, BlockCollect*> middle;

        find_need_replicate_blocks(need, now, emergency_replicate_count, middle);

        TBSYS_LOG(INFO, "block count: %"PRI64_PREFIX"d, emergency_replicate_count: %"PRI64_PREFIX"d, need: %"PRI64_PREFIX"d", calc_all_block_count(), emergency_replicate_count, need);

        //adjust plan size
        if (emergency_replicate_count > need)
        {
          adjust = need;
          need += need;
        }

        uint32_t block_id  = 0;
        bool has_replicate = false;
        bool all_find_flag = true;
        std::vector<ServerCollect*> except;
        std::vector<ServerCollect*> source;
        std::multimap<PlanPriority, BlockCollect*>::const_reverse_iterator iter = middle.rbegin();
        for (; iter != middle.rend() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          except.clear();
          {
            BlockChunkPtr ptr = get_chunk(iter->second->id());
            RWLock::Lock lock(*ptr, READ_LOCKER);
            has_replicate = ((!GFactory::get_lease_factory().has_valid_lease(iter->second->id()))
                && (!find_block_in_plan(iter->second->id()))
                && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
            if (has_replicate)
            {
              source = iter->second->get_hold();
              block_id = iter->second->id();
            }
          }
          if (has_replicate)//need replicate
          {
            //elect source server
            std::vector<ServerCollect*> target(source);
            find_server_in_plan_helper(source, except);
            std::vector<ServerCollect*> runer;
            std::vector<ServerCollect*> result;
            int32_t count = 0;
            {
              RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
              count = elect_replicate_source_ds(*this, source, except,1, result);
            }
            if (1 != count)
            {
              TBSYS_LOG(WARN, "replicate block: %u cannot found source dataserver", block_id);
              continue;
            }
            runer.push_back(result.back());

            //elect target server
            except = source;
            {
              RWLock::Lock rlock(server_mutex_, READ_LOCKER);
              RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
              count = elect_replicate_dest_ds(*this, except, 1, target);
            }
            if (1 != count)
            {
              TBSYS_LOG(WARN, "replicate block: %u cannot found target dataserver", block_id);
              continue;
            }
            runer.push_back(target.back());
            ReplicateTaskPtr task = new ReplicateTask(this, iter->first, block_id,now, now, runer, plan_seqno);
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            task->dump(TBSYS_LOG_LEVEL_DEBUG);
#endif
            if (!add_task(task))
            {
              task = 0;
              TBSYS_LOG(ERROR, "add task(replicate) fail, block: %u", block_id);
              continue;
            }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
            TBSYS_LOG(DEBUG, "add task, type: %d", task->type_);
#endif
            --need;
            --emergency_replicate_count;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
            blocks.push_back(task->block_id_);
#endif
          }
        }
        return true;
      }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_compact_plan(const int64_t plan_seqno,const time_t now, int64_t& need, std::vector<uint32_t>& plans)
#else
      bool LayoutManager::build_compact_plan(const int64_t plan_seqno, const time_t now, int64_t& need)
#endif
      {
        bool has_compact = false;
        bool all_find_flag = false;
        std::vector<ServerCollect*> except;
        for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
        {
          RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
          const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
          BLOCK_MAP::const_iterator iter = blocks.begin();
          for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
          {
            {
              has_compact = ((iter->second->check_compact())
                  && (!find_block_in_plan(iter->second->id()))
                  && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
            }

            if (has_compact)
            {
              CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, iter->second->id(), now, now, iter->second->get_hold(), plan_seqno);
              if (!add_task(task))
              {
                task = 0;
                TBSYS_LOG(ERROR, "add task(compact) fail, block: %u", iter->second->id());
                continue;
              }
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
              TBSYS_LOG(DEBUG, "add task, type: %d", task->type_);
#endif
              --need;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
              plans.push_back(task->block_id_);
#endif
            }
          }
        }
        return true;
      }

    int64_t LayoutManager::calc_average_block_size()
    {
      int64_t actual_total_block_num = calc_all_block_count();
      int64_t total_bytes = calc_all_block_bytes();
      return (actual_total_block_num > 0 && total_bytes > 0)
        ? total_bytes/actual_total_block_num : SYSPARAM_NAMESERVER.max_block_size_;
    }

    /**
     * statistic all dataserver 's information(capactiy, block count, load && alive server size)
     */
    void LayoutManager::statistic_all_server_info(const int64_t need,
        const int64_t average_block_size,
        double& total_capacity,
        int64_t& total_block_count,
        int64_t& total_load,
        int64_t& alive_server_size)
    {
      UNUSED(average_block_size);
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      SERVER_MAP::const_iterator iter = servers_.begin();
      for (; iter != servers_.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
      {
        if (iter->second->is_alive())
        {
          total_block_count += iter->second->block_count();
          total_capacity += (iter->second->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_) / 100;
          //total_capacity -= iter->second->use_capacity();
          total_load     += iter->second->load();
          ++alive_server_size;
        }
      }
      //total_capacity += total_block_count * average_block_size;
    }

    void LayoutManager::split_servers(const int64_t need,
        const int64_t average_load,
        const double total_capacity,
        const int64_t total_block_count,
        const int64_t average_block_size,
        std::set<ServerCollect*>& source,
        std::set<ServerCollect*>& target)
    {
      UNUSED(average_block_size);
      bool has_move = false;
      int64_t current_block_count = 0;
      int64_t should_block_count  = 0;
      double current_total_capacity = 0;
      RWLock::Lock lock(server_mutex_, READ_LOCKER);
      SERVER_MAP::const_iterator iter = servers_.begin();
      for (; iter != servers_.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
      {
        {
          RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
          has_move = ((iter->second->is_alive())
              && (!find_server_in_plan(iter->second)));

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "server: %s, alive: %d find: %d", CNetUtil::addrToString(iter->first).c_str(),
              iter->second->is_alive(), !find_server_in_plan(iter->second));
#endif
        }
        if (has_move)
        {
          current_block_count = iter->second->block_count();
          current_total_capacity = iter->second->total_capacity() * SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;
          should_block_count = static_cast<int64_t>((current_total_capacity / total_capacity) * total_block_count);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
          TBSYS_LOG(DEBUG, "server: %s, current_block_count: %"PRI64_PREFIX"d should_block_count: %"PRI64_PREFIX"d", CNetUtil::addrToString(iter->first).c_str(), current_block_count, should_block_count);
#endif
          if (current_block_count > should_block_count  + SYSPARAM_NAMESERVER.balance_max_diff_block_num_)
          {
            source.insert(iter->second);
          }
          else
          {
            if ((average_load <= 0)
                || (iter->second->load() < average_load * LOAD_BASE_MULTIPLE))
            {
              target.insert(iter->second);
            }
          }
        }
      }
    }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_balance_plan(const int64_t plan_seqno, const time_t now, int64_t& need, std::vector<uint32_t>& plans)
#else
    bool LayoutManager::build_balance_plan(const int64_t plan_seqno, const time_t now, int64_t& need)
#endif
      {
        double total_capacity  = 0;
        int64_t total_block_count = 0;
        int64_t total_load = 1;
        int64_t alive_server_size = 0;
        int64_t average_block_size = calc_average_block_size();

        statistic_all_server_info(need, average_block_size,total_capacity, total_block_count, total_load, alive_server_size);

        if (total_capacity <= 0)
        {
          TBSYS_LOG(INFO, "total_capacity: %"PRI64_PREFIX"d <= 0, we'll doesn't build moveing plan", total_capacity);
        }
        else
        {
          if (alive_server_size <= 0)
          {
            TBSYS_LOG(INFO, "alive_server_size: %"PRI64_PREFIX"d <= 0, we'll doesn't build moveing plan", alive_server_size);
          }
          else
          {
            std::set<ServerCollect*> target;
            std::set<ServerCollect*> source;
            int64_t average_load = total_load / alive_server_size;

            split_servers(need, average_load, total_capacity, total_block_count, average_block_size, source, target);

            TBSYS_LOG(INFO, "need: %"PRI64_PREFIX"d, source size: %u, target: %u", need, source.size(), target.size());

            bool has_move = false;
            uint32_t block_id = 0;
            std::vector<ServerCollect*> except;
            std::vector<ServerCollect*> servers;
            std::set<ServerCollect*>::const_iterator it = source.begin();
            for (; it != source.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0 && !target.empty(); ++it)
            {
              (*it)->rdlock();
              std::set<BlockCollect*, ServerCollect::BlockIdComp> blocks((*it)->hold_);
              (*it)->unlock();

              std::set<BlockCollect*, ServerCollect::BlockIdComp>::const_iterator cn_iter = blocks.begin();
              for (; cn_iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++cn_iter)
              {
                except.clear();
                BlockCollect* block_collect = *cn_iter;
                {
                  BlockChunkPtr ptr = get_chunk(block_collect->id());
                  RWLock::Lock r_lock(*ptr, READ_LOCKER);
                  has_move = ((block_collect != NULL)
                      && (block_collect->check_balance())
                      && (!find_server_in_plan((*it)))
                      && (!find_block_in_plan(block_collect->id())));
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
                  TBSYS_LOG(DEBUG, "block: %u check balance has_move: %s", block_collect->id(), has_move ? "true" : "false");
#endif
                  if (has_move)
                  {
                    servers = block_collect->get_hold();
                    block_id = block_collect->id();
                  }
                }
                if (has_move)
                {
                  std::vector<ServerCollect*>::iterator where = std::find(servers.begin(), servers.end(), (*it));
                  if (where == servers.end())
                  {
                    TBSYS_LOG(ERROR, "cannot elect move source server block: %u, source: %s",
                        block_id, CNetUtil::addrToString((*it)->id()).c_str());
                    continue;
                  }
                  servers.erase(where);

                  //elect dest dataserver
                  ServerCollect* target_ds = NULL;
                  bool bret = elect_move_dest_ds(target, servers, (*it), &target_ds);
                  if (!bret)
                  {
                    TBSYS_LOG(ERROR, "cannot elect move dest server block: %u, source: %s",
                        block_collect->id(), CNetUtil::addrToString((*it)->id()).c_str());
                    continue;
                  }
                  std::vector<ServerCollect*> runer;
                  runer.push_back((*it));
                  runer.push_back(target_ds);
                  MoveTaskPtr task = new MoveTask(this, PLAN_PRIORITY_NORMAL,  block_id, now , now, runer, plan_seqno);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
                  task->dump(TBSYS_LOG_LEVEL_DEBUG);
#endif

                  //push task to pending_plan_list_
                  if (!add_task(task))
                  {
                    task = 0;
                    TBSYS_LOG(ERROR, "add task(balance) fail, block: %u", block_id);
                    continue;
                  }
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
                  TBSYS_LOG(DEBUG, "add task, type: %d", task->type_);
#endif
                  --need;
                  std::set<ServerCollect*>::iterator tmp = target.find((*it));
                  if (tmp != target.end())
                  {
                    target.erase(tmp);
                  }
                  tmp = target.find(target_ds);
                  if (tmp != target.end())
                  {
                    target.erase(tmp);
                  }
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
                  plans.push_back(task->block_id_);
#endif
                  break;
                }
              }
            }
          }
        }
        return true;
      }

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
    bool LayoutManager::build_redundant_plan(const int64_t plan_seqno, const time_t now, int64_t& need, std::vector<uint32_t>& plans)
#else
    bool LayoutManager::build_redundant_plan(const int64_t plan_seqno, const time_t now, int64_t& need)
#endif
    {
      bool all_find_flag = true;
      bool has_delete = false;
      int32_t  count = 0;
      std::vector<ServerCollect*> except;
      std::vector<ServerCollect*> servers;
      for (int32_t i = 0; i < block_chunk_num_ && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++i)
      {
        RWLock::Lock lock(*block_chunk_[i], READ_LOCKER);
        const BLOCK_MAP& blocks = block_chunk_[i]->block_map_;
        BLOCK_MAP::const_iterator iter = blocks.begin();
        for (; iter != blocks.end() && !(interrupt_ & INTERRUPT_ALL) && need > 0; ++iter)
        {
          except.clear();
          count = iter->second->check_redundant();
          {
            has_delete = ((count > 0)
                && (!find_block_in_plan(iter->second->id()))
                && (!find_server_in_plan(iter->second->get_hold(), all_find_flag, except)));
            if (has_delete)
            {
              servers = iter->second->get_hold();
            }
          }
          if (has_delete)
          {
            std::vector<ServerCollect*> result;
            find_server_in_plan_helper(servers, except);
            if ((delete_excess_backup(servers, count, result) > 0)
                && (!result.empty()))
            {
              TBSYS_LOG(INFO, "we will need delete less than block: %u", iter->second->id());
              DeleteBlockTaskPtr task = new DeleteBlockTask(this, PLAN_PRIORITY_NORMAL, iter->second->id(), now, now, result, plan_seqno);
              if (!add_task(task))
              {
                task = 0;
                TBSYS_LOG(ERROR, "add task(delete) fail, block: %u", iter->second->id());
                continue;
              }
              --need;
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
              TBSYS_LOG(DEBUG, "add task, type: %d", task->type_);
#endif
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
              plans.push_back(task->block_id_);
#endif
            }
          }
        }
      }
      return true;
    }

    bool LayoutManager::add_task(const TaskPtr& task)
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      std::pair<std::set<TaskPtr, TaskCompare>::iterator, bool> res = pending_plan_list_.insert(task);
      if (!res.second)
      {
        TBSYS_LOG(ERROR, "object was found by task in plan, block: %u, type: %d", task->block_id_, task->type_);
        return true;
      }

      RWLock::Lock tlock(maping_mutex_, WRITE_LOCKER);
      std::pair<std::map<uint32_t, TaskPtr>::iterator, bool> rs =
        block_to_task_.insert(std::map<uint32_t, TaskPtr>::value_type(task->block_id_, task));
      if (!rs.second)
      {
        TBSYS_LOG(ERROR, "object was found by block: %u in block list", task->block_id_);
        pending_plan_list_.erase(res.first);
        return false;
      }
      std::pair<std::map<ServerCollect*,TaskPtr>::iterator, bool> iter;
      std::vector<ServerCollect*>::iterator index = task->runer_.begin();
      for (; index != task->runer_.end(); ++index)
      {
        iter = server_to_task_.insert(std::map<ServerCollect*,TaskPtr>::value_type((*index), task));
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
        //TBSYS_LOG(DEBUG, "server: %"PRI64_PREFIX"d, server: %"PRI64_PREFIX"d", (*index)->id(), (iter.first->first)->id());
#endif
      }
      run_plan_monitor_.notify();
#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION) || defined(TFS_NS_DEBUG)
      std::stringstream out;
      out << task->type_ << " " ;
      out << task->block_id_ << " " ;
      index = task->runer_.begin();
      for (; index != task->runer_.end(); ++index)
      {
        out << tbsys::CNetUtil::addrToString((*index)->id()) << "/";
      }
      TBSYS_LOG(ERROR, " add task : %s", out.str().c_str());
#endif
      return true;
    }

    bool LayoutManager::remove_task(const TaskPtr& task)
    {
      UNUSED(task);
      return true;
    }

    LayoutManager::TaskPtr LayoutManager::find_task(const uint32_t block_id)
    {
      std::map<uint32_t, TaskPtr>::const_iterator iter = block_to_task_.find(block_id);
      if (iter != block_to_task_.end())
      {
        return iter->second;
      }
      return 0;
    }

    bool LayoutManager::find_block_in_plan(const uint32_t block_id)
    {
      RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
      return (block_to_task_.end() != block_to_task_.find(block_id));
    }

    bool LayoutManager::find_server_in_plan(ServerCollect* server)
    {
      RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
      return (server_to_task_.end() != server_to_task_.find(server));
    }

    bool LayoutManager::find_server_in_plan(const std::vector<ServerCollect*> & servers, bool all_find, std::vector<ServerCollect*>& result)
    {
      RWLock::Lock tlock(maping_mutex_, READ_LOCKER);
      std::vector<ServerCollect*>::const_iterator iter = servers.begin();
      for (; iter != servers.end(); ++iter)
      {
        std::map<ServerCollect*, TaskPtr>::iterator it = server_to_task_.find((*iter));
        if (it != server_to_task_.end())
        {
          if (all_find)
          {
            result.push_back((*iter));
          }
          else
          {
            return true;
          }
        }
      }
      return all_find ? result.size() == servers.size() ? true : false : false;
    }

    void LayoutManager::find_server_in_plan_helper(std::vector<ServerCollect*>& servers, std::vector<ServerCollect*>& except)
    {
      std::vector<ServerCollect*>::iterator iter = except.begin();
      for (; iter != except.end(); ++iter)
      {
        std::vector<ServerCollect*>::iterator ret = std::find(servers.begin(), servers.end(), (*iter));
        if (ret != servers.end())
        {
          servers.erase(ret);
        }
      }
    }

    bool LayoutManager::expire()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(run_plan_monitor_);
      finish_plan_list_.clear();
      return true;
    }
  }
}
