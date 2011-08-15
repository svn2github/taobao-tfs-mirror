/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task.cpp 2139 2011-03-28 09:15:26Z duanfei $
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
#include <bitset>
#include "common/client_manager.h"
#include "layout_manager.h"
#include "message/compact_block_message.h"
#include "common/status_message.h"
#include "global_factory.h"
#include "message/replicate_block_message.h"
using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;
namespace tfs
{
  namespace nameserver
  {
    const int8_t LayoutManager::CompactTask::INVALID_SERVER_ID = 0;
    const int8_t LayoutManager::CompactTask::INVALID_BLOCK_ID = 0;
    LayoutManager::Task::Task(LayoutManager* manager, const PlanType type,
        const PlanPriority priority, uint32_t block_id,
        time_t begin, time_t end, const std::vector<ServerCollect*>& runer, const int64_t seqno):
      runer_(runer),
      begin_time_(begin),
      end_time_(end),
      block_id_(block_id),
      type_(type),
      status_(PLAN_STATUS_NONE),
      priority_(priority),
      manager_(manager),
      seqno_(seqno)
    {

    }

    bool LayoutManager::Task::operator < (const Task& task) const
    {
      if (priority_ < task.priority_)
        return true;
      if (priority_ > task.priority_)
        return false;
      if (type_ < task.type_)
        return true;
      if (type_ > task.type_)
        return false;
      if (block_id_ < task.block_id_)
        return true;
      if (block_id_ > task.block_id_)
        return false;
      return (begin_time_ < task.begin_time_);
    }

    void LayoutManager::Task::runTimerTask()
    {
      dump(TBSYS_LOG_LEVEL_INFO, "task expired");
      tbutil::Monitor<tbutil::Mutex>::Lock lock(manager_->run_plan_monitor_);
      TaskPtr task = TaskPtr::dynamicCast(this);
      status_ = PLAN_STATUS_TIMEOUT;
      manager_->finish_plan_list_.push_back(task);
      std::set<TaskPtr, TaskCompare>::iterator r_iter = manager_->running_plan_list_.find(task);
      if (r_iter != manager_->running_plan_list_.end())
      {
        TBSYS_LOG(INFO, "%s", "task expired erase");
        manager_->running_plan_list_.erase(r_iter);
      }
      RWLock::Lock tlock(manager_->maping_mutex_, WRITE_LOCKER);
      manager_->block_to_task_.erase(block_id_);
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter)
      {
        manager_->server_to_task_.erase((*iter));
      }
    }

    void LayoutManager::Task::dump(tbnet::DataBuffer& stream)
    {
      stream.writeInt8(type_);
      stream.writeInt8(status_);
      stream.writeInt8(priority_);
      stream.writeInt32(block_id_);
      stream.writeInt64(begin_time_);
      stream.writeInt64(end_time_);
      stream.writeInt64(seqno_);
      stream.writeInt8(runer_.size());
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter)
      {
        stream.writeInt64((*iter)->id());
      }
    }

    void LayoutManager::Task::dump(int32_t level, const char* const format)
    {
      std::string runer;
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter)
      {
        runer += CNetUtil::addrToString((*iter)->id());
        runer += "/";
      }
      if (level <= TBSYS_LOGGER._level)
      {
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__, "pointer %p, %s plan seqno: %"PRI64_PREFIX"d, type: %s ,status: %s, priority: %s , block_id: %u, begin: %"PRI64_PREFIX"d, end: %"PRI64_PREFIX"d, runer: %s",
            this,
            format == NULL ? "" : format,
            seqno_,
            type_ == PLAN_TYPE_REPLICATE ? "replicate" : type_ == PLAN_TYPE_MOVE ? "move" : type_ == PLAN_TYPE_COMPACT
            ? "compact" : type_ == PLAN_TYPE_DELETE ? "delete" : "unknow",
            status_ == PLAN_STATUS_BEGIN ? "begin" : status_ == PLAN_STATUS_TIMEOUT ? "timeout" : status_ == PLAN_STATUS_END
            ? "finish" : status_ == PLAN_STATUS_FAILURE ? "failure": "unknow",
            priority_ == PLAN_PRIORITY_NORMAL ? "normal" : priority_ == PLAN_PRIORITY_EMERGENCY ? "emergency": "unknow",
            block_id_, begin_time_, end_time_, runer.c_str());
      }
    }

    LayoutManager::CompactTask::CompactTask(LayoutManager* manager, const PlanPriority priority,
      uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer, const int64_t seqno):
      Task(manager, PLAN_TYPE_COMPACT, priority, block_id, begin, end, runer, seqno)
    {
      memset(&block_info_, 0, sizeof(block_info_));
    }

    void LayoutManager::CompactTask::runTimerTask()
    {
      status_ = PLAN_STATUS_TIMEOUT;
      TaskPtr task = TaskPtr::dynamicCast(this);
      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(manager_->run_plan_monitor_);
        std::set<TaskPtr, TaskCompare>::iterator r_iter = manager_->running_plan_list_.find(task);
        if (r_iter != manager_->running_plan_list_.end())
        {
          manager_->running_plan_list_.erase(r_iter);
        }

        RWLock::Lock tlock(manager_->maping_mutex_, WRITE_LOCKER);
        manager_->block_to_task_.erase(block_id_);
        std::vector<ServerCollect*>::iterator iter = runer_.begin();
        for (; iter != runer_.end(); ++iter)
        {
          manager_->server_to_task_.erase((*iter));
        }
      }

      std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
      for (; iter != complete_status_.end(); ++iter)
      {
        std::pair<uint64_t, PlanStatus>& status = (*iter);
        if (status.second != PLAN_STATUS_END
            || (status.second !=  PLAN_STATUS_FAILURE))
        {
          status.second = PLAN_STATUS_TIMEOUT;
        }
      }
      CompactComplete value(INVALID_SERVER_ID, INVALID_SERVER_ID, PLAN_STATUS_NONE);
      memcpy(&value.block_info_, &block_info_, sizeof(block_info_));
      VUINT64 servers;
      check_complete(value, servers);

      int32_t iret = do_complete(value, servers);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "compact task do complete fail: %d", iret);
      }

      {
        tbutil::Monitor<tbutil::Mutex>::Lock lock(manager_->run_plan_monitor_);
        manager_->finish_plan_list_.push_back(task);
      }
    }

    void LayoutManager::CompactTask::dump(tbnet::DataBuffer& stream)
    {
      Task::dump(stream);
      stream.writeInt8(complete_status_.size());
      std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
      for (; iter != complete_status_.end(); ++iter)
      {
        stream.writeInt64((*iter).first);
        stream.writeInt8((*iter).second);
      }
    }

    void LayoutManager::CompactTask::dump(int32_t level, const char* const format)
    {
      std::string runer;
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter)
      {
        runer += CNetUtil::addrToString((*iter)->id());
        runer += "/";
      }
      PlanStatus plan_status = PLAN_STATUS_NONE;
      std::string status;
      std::vector< std::pair <uint64_t, PlanStatus> >::iterator it= complete_status_.begin();
      for (; it != complete_status_.end(); ++it)
      {
        status += CNetUtil::addrToString((*it).first);
        status += ":";
        plan_status = (*it).second;
        status += plan_status == PLAN_STATUS_BEGIN ? "begin" : plan_status == PLAN_STATUS_TIMEOUT ? "timeout" : plan_status == PLAN_STATUS_END
          ? "finish" : plan_status == PLAN_STATUS_FAILURE ? "failure": "unknow",
          status += "/";
      }

      if (level <= TBSYS_LOGGER._level)
      {
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__, "pointer: %p, %s plan seqno: %"PRI64_PREFIX"d, type: %s ,status: %s, priority: %s , block_id: %u, begin: %"PRI64_PREFIX"d, end: %"PRI64_PREFIX"d, runer: %s, complete status: %s",
            this,
            format == NULL ? "" : format,
            seqno_,
            type_ == PLAN_TYPE_REPLICATE ? "replicate" : type_ == PLAN_TYPE_MOVE ? "move" : type_ == PLAN_TYPE_COMPACT
            ? "compact" : type_ == PLAN_TYPE_DELETE ? "delete" : "unknow",
            status_ == PLAN_STATUS_BEGIN ? "begin" : status_ == PLAN_STATUS_TIMEOUT ? "timeout" : status_ == PLAN_STATUS_END
            ? "finish" : status_ == PLAN_STATUS_FAILURE ? "failure" : "unknow",
            priority_ == PLAN_PRIORITY_NORMAL ? "normal" : priority_ == PLAN_PRIORITY_EMERGENCY ? "emergency": "unknow",
            block_id_, begin_time_, end_time_, runer.c_str(), status.c_str());
      }
    }

    int LayoutManager::CompactTask::handle()
    {
      int32_t iret = TFS_SUCCESS;
      int32_t index = 0;
      CompactBlockMessage msg;
      msg.set_block_id(block_id_);
      msg.set_preserve_time(SYSPARAM_NAMESERVER.run_plan_expire_interval_);
      std::pair<uint64_t, PlanStatus> res;
      std::vector<ServerCollect*>::iterator iter = runer_.begin();
      for (; iter != runer_.end(); ++iter, ++index)
      {
        res.first = (*iter)->id();
        res.second = PLAN_STATUS_BEGIN;
        msg.set_owner( index == 0 ? 1 : 0);
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        int32_t status = STATUS_MESSAGE_ERROR;
        iret = send_msg_to_server(res.first, &msg, status);
        if (TFS_SUCCESS != iret
          || STATUS_MESSAGE_OK != status)
        {
          res.second = PLAN_STATUS_TIMEOUT;
          TBSYS_LOG(INFO, "send compact message filed; block : %u owner: %d to server: %s, ret: %d",
              block_id_, index == 0 ? 1 : 0, CNetUtil::addrToString(res.first).c_str(), iret);
        }
#endif
        complete_status_.push_back(res);
      }

      status_ = PLAN_STATUS_BEGIN;
      begin_time_ = time(NULL);
      end_time_ = begin_time_ + SYSPARAM_NAMESERVER.run_plan_expire_interval_;
      return iret;
    }

    int LayoutManager::CompactTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      dump(TBSYS_LOG_LEVEL_INFO, "handle compact complete message");
      CompactBlockCompleteMessage* message = dynamic_cast<CompactBlockCompleteMessage*>(msg);
      PlanStatus status = status_transform_compact_to_plan(static_cast<CompactStatus>(message->get_success()));
      CompactComplete value(message->get_server_id(), message->get_block_id(), status);
      memcpy(&value.block_info_, &message->get_block_info(), sizeof(block_info_));
      int32_t iret = TFS_SUCCESS;
      VUINT64 servers;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.owner_role_ == NS_ROLE_MASTER)//master handle
      {
        check_complete(value, servers);
        iret = do_complete(value, servers);
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "block: %u compact, do compact complete fail: %d", value.block_id_, iret);
        }
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        iret = message->reply(new StatusMessage(iret));
#endif

        all_complete_flag = value.is_complete_;
        if (all_complete_flag)
          status_ = PLAN_STATUS_END;
      }
      else
      {
        //slave
        std::bitset < 3 > bset(message->get_flag());
        value.all_success_ = bset[0];
        value.has_success_ = bset[1];
        value.is_complete_ = bset[2];
        TBSYS_LOG(DEBUG, "check compact complete flag: %u", message->get_flag());
        servers.clear();
        servers.assign(message->get_ds_list().begin(), message->get_ds_list().end());
        iret = do_complete(value, servers);
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "block: %u compact, do compact complete fail: %d", value.block_id_, iret);
        }
      }
      return iret;
    }

    void LayoutManager::CompactTask::check_complete(CompactComplete& value, std::vector<uint64_t> & servers)
    {
      int32_t complete_count = 0;
      int32_t success_count = 0;
      std::vector< std::pair <uint64_t, PlanStatus> >::iterator iter = complete_status_.begin();
      for (; iter != complete_status_.end(); ++iter)
      {
        std::pair<uint64_t, PlanStatus>& status = (*iter);
        if (status.first == value.id_)
        {
          status.second = value.status_;
          if (value.status_ == PLAN_STATUS_END)
          {
            memcpy(&block_info_, &value.block_info_, sizeof(BlockInfo));
            value.current_complete_result_ = true;
            ++success_count;
            ++complete_count;
          }
          else
          {
            ++complete_count;
            servers.push_back(status.first);
          }
        }
        else
        {
          if (status.second == PLAN_STATUS_END)
          {
            ++complete_count;
            ++success_count;
          }
          else if (status.second != PLAN_STATUS_BEGIN)
          {
            ++complete_count;
            servers.push_back(status.first);
          }
        }
      }

      TBSYS_LOG(DEBUG, "complete_count: %d, success_count: %d, complete_status size: %u",
          complete_count, success_count, complete_status_.size());

      value.is_complete_ = complete_count == static_cast<int32_t>(complete_status_.size());
      value.has_success_ = success_count != 0;
      value.all_success_ = success_count ==  static_cast<int32_t>(complete_status_.size());
    }

    int LayoutManager::CompactTask::do_complete(CompactComplete& value, common::VUINT64& servers)
    {
      if (value.current_complete_result_)
      {
        BlockChunkPtr ptr = manager_->get_chunk(value.block_id_);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block_collect = ptr->find(value.block_id_);
        if (block_collect != NULL)
        {
          block_collect->update(block_info_);
          TBSYS_LOG(DEBUG,"check compacting complete server: %s, block: %u,copy blockinfo into metadata, block size: %d",
              CNetUtil::addrToString(value.id_).c_str(), value.block_id_, block_info_.size_);
        }
      }

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (value.is_complete_
          && value.has_success_
          && !servers.empty())
      {
        time_t now = time(NULL);
        //expire block on this failed servers.
        std::vector<uint64_t>::iterator iter = servers.begin();
        for (; iter != servers.end(); ++iter)
        {
          ServerCollect* server = NULL;
          {
            server = manager_->get_server((*iter));
          }

          BlockChunkPtr ptr = manager_->get_chunk(value.block_id_);
          RWLock::Lock lock(*ptr, WRITE_LOCKER);
          BlockCollect* block = ptr->find(value.block_id_);

          bool bret = server != NULL && block != NULL;
          if (bret)
          {
            if (!manager_->relieve_relation(block, server, now))
            {
              TBSYS_LOG(WARN, "we'll get failed when relive relation between block: %u and server: %s",
                  value.block_id_, CNetUtil::addrToString((*iter)).c_str());
            }
            if (ngi.owner_role_ == NS_ROLE_MASTER)
            {
              std::vector<stat_int_t> stat(1, 1);
              GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);
              manager_->rm_block_from_ds((*iter), value.block_id_);
            }
          }
        }
      }

      if ((value.is_complete_)
          && (value.all_success_))
      {
        // rewritable block
        BlockChunkPtr ptr = manager_->get_chunk(value.block_id_);
        RWLock::Lock lock(*ptr, WRITE_LOCKER);
        BlockCollect* block = ptr->find(value.block_id_);
        if (block != NULL)
        {
          std::vector<ServerCollect*>::iterator iter = block->get_hold().begin();
          for (; iter !=  block->get_hold().end(); ++iter)
          {
            (*iter)->add_writable(block);
          }
        }
      }

      if (((value.is_complete_)
            || (value.has_success_))
          && (ngi.owner_role_ == NS_ROLE_MASTER))
      {
        //transmit message.
        CompactBlockCompleteMessage msg;
        msg.set_block_id(value.block_id_);
        msg.set_server_id(value.id_);
        msg.set_block_info(block_info_);
        msg.set_success(status_transform_plan_to_compact(value.status_));
        msg.set_ds_list(servers);

        std::bitset < 3 > bset;
        bset[0] = value.all_success_;
        bset[1] = value.has_success_;
        bset[2] = value.is_complete_;
        msg.set_flag(bset.to_ulong());
        TBSYS_LOG(DEBUG, "check compact complete flag: %d", msg.get_flag());

        common::Stream stream(msg.length());
        int32_t iret = msg.serialize(stream);
        if (TFS_SUCCESS == iret)
        {
          if (manager_->get_oplog_sync_mgr().log(OPLOG_TYPE_COMPACT_MSG, stream.get_data(), stream.get_data_length()) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "%s", "write oplog failed");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "%s", "compact msg serialize error");
        }
      }
      return TFS_SUCCESS;
    }

    CompactStatus LayoutManager::CompactTask::status_transform_plan_to_compact(const PlanStatus status) const
    {
      return status == PLAN_STATUS_END ? COMPACT_STATUS_SUCCESS : 
             status == PLAN_STATUS_BEGIN ? COMPACT_STATUS_START : COMPACT_STATUS_FAILED;
    }

    PlanStatus LayoutManager::CompactTask::status_transform_compact_to_plan(const CompactStatus status) const
    {
      return status == COMPACT_STATUS_SUCCESS ? PLAN_STATUS_END : 
             status == COMPACT_STATUS_START ? PLAN_STATUS_BEGIN :
             status == COMPACT_STATUS_FAILED ? PLAN_STATUS_FAILURE : PLAN_STATUS_NONE; 
    }

    LayoutManager::ReplicateTask::ReplicateTask(LayoutManager* manager, const PlanPriority priority,
        const uint32_t block_id, const time_t begin,
        const time_t end, const std::vector<ServerCollect*>& runer, const int64_t seqno):
      Task(manager, PLAN_TYPE_REPLICATE, priority, block_id, begin, end, runer, seqno),
      flag_(REPLICATE_BLOCK_MOVE_FLAG_NO)
    {

    }

    int LayoutManager::ReplicateTask::handle()
    {
      int32_t iret = runer_.size() >= 0x2U ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(WARN, "task (replicate) block: %u, type: %d, priority: %d, runer size: %u is invalid", block_id_, type_, priority_, runer_.size());
      }
      else
      {
        ReplicateBlockMessage msg;
        ReplBlock block;
        memset(&block, 0, sizeof(block));
        block.block_id_ = block_id_;
        block.source_id_ = runer_[0]->id();
        block.destination_id_ = runer_[1]->id();
        block.start_time_ = time(NULL);
        block.is_move_ = flag_;
        block.server_count_ = 0;
        msg.set_repl_block(&block);
        msg.set_command(PLAN_STATUS_BEGIN);
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
        int32_t status = STATUS_MESSAGE_ERROR;
        iret = send_msg_to_server(block.source_id_, &msg, status);
        if (TFS_SUCCESS != iret
          || STATUS_MESSAGE_OK != status)
        {
          TBSYS_LOG(ERROR, "send %s command faild, block: %u, iret: %d %s===>%s",
              flag_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
              block_id_, iret, tbsys::CNetUtil::addrToString(block.source_id_).c_str(),
              tbsys::CNetUtil::addrToString(block.destination_id_).c_str());
          iret = TFS_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "send %s command successful, block: %u, : %s===>: %s",
              flag_ == REPLICATE_BLOCK_MOVE_FLAG_NO ? "replicate" : "move",
              block_id_, tbsys::CNetUtil::addrToString(block.source_id_).c_str(),
              tbsys::CNetUtil::addrToString(block.destination_id_).c_str());

          status_ = PLAN_STATUS_BEGIN;
          begin_time_ = time(NULL);
          end_time_ = begin_time_ + SYSPARAM_NAMESERVER.run_plan_expire_interval_;
        }
#endif
      }
      return iret;
    }

    int LayoutManager::ReplicateTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      time_t now = time(NULL);
      ReplicateBlockMessage* message = dynamic_cast<ReplicateBlockMessage*>(msg);
      const ReplBlock blocks = *message->get_repl_block();
      bool success = message->get_command() == PLAN_STATUS_END;
      int32_t iret = STATUS_MESSAGE_OK;
      TBSYS_LOG(INFO, "block: %u %s complete status: %s", blocks.block_id_,
          blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate",
          message->get_command() == PLAN_STATUS_END ? "end" :
          message->get_command() == PLAN_STATUS_TIMEOUT ? "timeout" :
          message->get_command() == PLAN_STATUS_BEGIN ? "begin" :
          message->get_command() == PLAN_STATUS_FAILURE ? "failure" : "unknow");
      if (success)
      {
        BlockChunkPtr ptr = manager_->get_chunk(blocks.block_id_);
        BlockCollect* block = NULL;
        {
          RWLock::Lock lock(*ptr, READ_LOCKER);
          block = ptr->find(blocks.block_id_);//find block
        }

        if (block != NULL)
        {
          ServerCollect* server = NULL;
          {
            server = manager_->get_server(blocks.destination_id_);// find destination dataserver
          }
          if (server != NULL)
          {
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            manager_->build_relation(block, server, false);//build relation between block and dest dataserver
          }

          bool has_relieve_relation = false;
          {
            RWLock::Lock lock(*ptr, READ_LOCKER);
            block = ptr->find(blocks.block_id_);
            has_relieve_relation = blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES
              && block->get_hold_size() > SYSPARAM_NAMESERVER.max_replication_;
          }
          if (has_relieve_relation)
          {
            {
              server = manager_->get_server(blocks.source_id_);
            }
            RWLock::Lock lock(*ptr, WRITE_LOCKER);
            manager_->relieve_relation(block, server, now);
            iret = STATUS_MESSAGE_REMOVE;
          }
        }

        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (ngi.owner_role_ == NS_ROLE_MASTER)
        {
          common::Stream stream(message->length());
          int32_t iret = message->serialize(stream);
          if (common::TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "%s complete msg serialize error", blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate");
          }
          else
          {
            manager_->get_oplog_sync_mgr().log(OPLOG_TYPE_REPLICATE_MSG, stream.get_data(), stream.get_data_length());
          }
#if !defined(TFS_NS_GTEST) && !defined(TFS_NS_INTEGRATION)
          message->reply(new StatusMessage(iret));
#endif
        }
      }
      else
      {
        TBSYS_LOG(INFO, "block: %u %s complete status: %s", blocks.block_id_,
            blocks.is_move_ == REPLICATE_BLOCK_MOVE_FLAG_YES ? "move" : "replicate",
            message->get_command() == PLAN_STATUS_END ? "end" :
            message->get_command() == PLAN_STATUS_TIMEOUT ? "timeout" :
            message->get_command() == PLAN_STATUS_BEGIN ? "begin" :
            message->get_command() == PLAN_STATUS_FAILURE ? "failure" : "unknow");
      }
      all_complete_flag = true;
      status_ = PLAN_STATUS_END;
      return (iret == STATUS_MESSAGE_OK || iret == STATUS_MESSAGE_REMOVE) ? TFS_SUCCESS : iret;
    }

    LayoutManager::DeleteBlockTask::DeleteBlockTask(LayoutManager* manager,const PlanPriority priority,
                const uint32_t block_id, const time_t begin, const time_t end,
                const std::vector<ServerCollect*> & runer, const int64_t seqno):
      Task(manager, PLAN_TYPE_DELETE, priority, block_id, begin, end, runer, seqno)
    {

    }

    int LayoutManager::DeleteBlockTask::handle()
    {
      time_t now = time(NULL);
      BlockChunkPtr ptr = manager_->get_chunk(block_id_);
      RWLock::Lock(*ptr, WRITE_LOCKER);
      BlockCollect* block  = ptr->find(block_id_);
      int32_t iret = NULL != block ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        std::vector<ServerCollect*>::iterator iter = runer_.begin();
        for (; iter != runer_.end(); ++iter)
        {
          iret = LayoutManager::relieve_relation(block, (*iter), now) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "remove block: %u no server: %s relieve relation failed", block_id_, tbsys::CNetUtil::addrToString((*iter)->id()).c_str());
          }
          else
          {
            iret = manager_->rm_block_from_ds((*iter)->id(), block_id_);
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "send remove block: %u command on server: %s failed", block_id_, tbsys::CNetUtil::addrToString((*iter)->id()).c_str());
            }
          }
        }

        std::vector<stat_int_t> stat(1, runer_.size());
        GFactory::get_stat_mgr().update_entry(GFactory::tfs_ns_stat_block_count_, stat, false);

        status_ = PLAN_STATUS_BEGIN;
        begin_time_ = now;
        end_time_ = begin_time_ + SYSPARAM_NAMESERVER.run_plan_expire_interval_;
      }
      return iret;
    }

    int LayoutManager::DeleteBlockTask::handle_complete(common::BasePacket* msg, bool& all_complete_flag)
    {
      all_complete_flag = true;
      status_ = PLAN_STATUS_END;
      RemoveBlockResponseMessage* message = dynamic_cast<RemoveBlockResponseMessage*>(msg);
      TBSYS_LOG(INFO, "block: %u remove complete status end", message->get_block_id());
      return TFS_SUCCESS;
    }

    LayoutManager::MoveTask::MoveTask(LayoutManager* manager, const PlanPriority priority,
      uint32_t block_id, time_t begin, time_t end, const std::vector<ServerCollect*>& runer, const int64_t seqno):
      ReplicateTask(manager, priority, block_id, begin, end, runer, seqno)
    {
      type_ = PLAN_TYPE_MOVE;
      flag_ = REPLICATE_BLOCK_MOVE_FLAG_YES;
    }

    void LayoutManager::ExpireTask::runTimerTask()
    {
      manager_->expire();
    }

    void LayoutManager::BuildPlanThreadHelper::run()
    {
      try
      {
        manager_.build_plan();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::RunPlanThreadHelper::run()
    {
      try
      {
        manager_.run_plan();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void LayoutManager::CheckDataServerThreadHelper::run()
    {
      try
      {
        manager_.check_server();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }
  }
}

