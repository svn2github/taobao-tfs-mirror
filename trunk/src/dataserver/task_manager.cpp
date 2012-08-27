/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task_manager.cpp 390 2012-08-06 10:11:49Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd@taobao.com
 *      - initial release
 *
 */

#include <Memory.hpp>
#include "common/base_packet.h"
#include "message/message_factory.h"
#include "message/dataserver_task_message.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "task_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;
    using namespace std;
    using namespace tbsys;

    TaskManager::TaskManager()
    {
      stop_ = false;
    }

    TaskManager::~TaskManager()
    {

    }

    int TaskManager::init(const uint64_t ns_id, const uint64_t ds_id)
    {
      ns_id_ = ns_id;
      ds_id_ = ds_id;
      expire_cloned_interval_ = SYSPARAM_DATASERVER.expire_cloned_block_time_;
      last_expire_cloned_block_time_ = 0;
      return TFS_SUCCESS;
    }

    int TaskManager::handle(BaseTaskMessage* packet)
    {
      int pcode = packet->getPCode();
      int ret = TFS_SUCCESS;
      switch(pcode)
      {
        case REPLICATE_BLOCK_MESSAGE:
          ret = add_replicate_task(dynamic_cast<ReplicateBlockMessage*>(packet));
          break;
        case COMPACT_BLOCK_MESSAGE:
          ret = add_compact_task(dynamic_cast<NsRequestCompactBlockMessage*>(packet));
          break;
        case DS_REPLICATE_BLOCK_MESSAGE:
          ret = add_ds_replicate_task(dynamic_cast<DsReplicateBlockMessage*>(packet));
          break;
        case DS_COMPACT_BLOCK_MESSAGE:
          ret = add_ds_compact_task(dynamic_cast<DsCompactBlockMessage*>(packet));
          break;
        case REQ_EC_MARSHALLING_MESSAGE:
          ret = add_marshalling_task(dynamic_cast<ECMarshallingMessage*>(packet));
          break;
        case REQ_EC_REINSTATE_MESSAGE:
          ret = add_reinstate_task(dynamic_cast<ECReinstateMessage*>(packet));
          break;
        case REQ_EC_DISSOLVE_MESSAGE:
          ret = add_dissolve_task(dynamic_cast<ECDissolveMessage*>(packet));
          break;
        case RESP_DS_REPLICATE_BLOCK_MESSAGE:
        case RESP_DS_COMPACT_BLOCK_MESSAGE:
          ret = handle_complete(packet);
          break;
        default:
          ret = TFS_ERROR;
          break;
      }

      if (TFS_SUCCESS == ret)
      {
        packet->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    int TaskManager::handle_complete(BaseTaskMessage* packet)
    {
      int64_t seqno = packet->get_seqno();
      // task maybe expired already, ignore response in this case
      TBSYS_LOG(DEBUG, "handle complete, running task map size %u", running_task_.size());

      running_task_mutex_.lock();
      std::map<int64_t, Task*>::iterator it = running_task_.find(seqno);
      running_task_mutex_.unlock();

      if (it != running_task_.end())
      {
        Task* task = it->second;
        task->handle_complete(packet);
        if (task->is_completed())
        {
          running_task_mutex_.lock();
          running_task_.erase(seqno);
          running_task_mutex_.unlock();
          gDelete(task);
        }
      }

      return TFS_SUCCESS;
    }

    int TaskManager::add_replicate_task(ReplicateBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      const ReplBlock* repl_info = message->get_repl_block();

      int ret = TFS_SUCCESS;
      ReplicateTask* task = new ReplicateTask(*this, seqno, ns_id_, expire_time, *repl_info);
      ret = add_task_queue(task);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_compact_task(NsRequestCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint32_t block_id = message->get_block_id();

      int ret = TFS_SUCCESS;
      CompactTask* task = new CompactTask(*this, seqno, ns_id_, expire_time, block_id);
      task->set_servers(message->get_servers());
      ret = add_task_queue(task);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_ds_replicate_task(DsReplicateBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      uint64_t source_id = message->get_source_id();
      int32_t expire_time = message->get_expire_time();
      const ReplBlock* repl_info = message->get_repl_block();

      int ret = TFS_SUCCESS;
      ReplicateTask* task = new ReplicateTask(*this, seqno, source_id, expire_time, *repl_info);
      task->set_task_from_ds();
      ret = add_task_queue(task);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_ds_compact_task(DsCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      uint64_t source_id = message->get_source_id();
      int32_t expire_time = message->get_expire_time();
      uint32_t block_id = message->get_block_id();

      int ret = TFS_SUCCESS;
      CompactTask* task = new CompactTask(*this, seqno, source_id, expire_time, block_id);
      task->set_task_from_ds();
      ret = add_task_queue(task);
      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_marshalling_task(ECMarshallingMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      int64_t family_id = message->get_family_id();
      int32_t family_aid_info = message->get_family_aid_info();
      FamilyMemberInfo* family_members = message->get_family_member_info();

      MarshallingTask* task = new MarshallingTask(*this, seqno, ns_id_, expire_time, family_id);
      int ret = task->set_family_member_info(family_members, family_aid_info);
      if (TFS_SUCCESS == ret)
      {
        ret = add_task_queue(task);
      }

      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_reinstate_task(ECReinstateMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      int64_t family_id = message->get_family_id();
      int32_t family_aid_info = message->get_family_aid_info();

      FamilyMemberInfo* family_members = message->get_family_member_info();
      ReinstateTask* task = new ReinstateTask(*this, seqno, ns_id_, expire_time, family_id);
      int ret = task->set_family_member_info(family_members, family_aid_info);
      if (TFS_SUCCESS == ret)
      {
        ret = add_task_queue(task);
      }

      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_dissolve_task(ECDissolveMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      int64_t family_id = message->get_family_id();
      int32_t family_aid_info = message->get_family_aid_info();
      FamilyMemberInfo* family_members = message->get_family_member_info();

      DissolveTask* task = new DissolveTask(*this, seqno, ns_id_, expire_time, family_id);
      int ret = task->set_family_member_info(family_members, family_aid_info);
      if (TFS_SUCCESS == ret)
      {
        ret = add_task_queue(task);
      }

      if (TFS_SUCCESS != ret)
      {
        tbsys::gDelete(task);
      }
      return ret;
    }

    int TaskManager::add_task_queue(Task* task)
    {
      int ret = TFS_SUCCESS;
      bool exist = false;
      task_monitor_.lock();
      for (uint32_t i = 0; i < task_queue_.size(); i++)
      {
        if (task_queue_[i]->get_seqno() == task->get_seqno())
        {
          exist = true;
          break;
        }
      }

      if (false == exist)
      {
        TBSYS_LOG(INFO, "%s", task->dump().c_str());
        task_queue_.push_back(task);
      }
      else
      {
        ret = TFS_ERROR;
      }
      task_monitor_.unlock();

      if (false == exist)
      {
        task_monitor_.lock();
        task_monitor_.notify();
        task_monitor_.unlock();
      }

      return ret;
    }

    int TaskManager::run_task()
    {
      while (!stop_)
      {
        task_monitor_.lock();
        while (!stop_ && task_queue_.empty())
        {
          task_monitor_.wait();
        }
        if (stop_)
        {
          task_monitor_.unlock();
          break;
        }

        // get the first task
        Task* task = task_queue_.front();
        task_queue_.pop_front();
        task_monitor_.unlock();

        TBSYS_LOG(INFO, "start task, seqno: %"PRI64_PREFIX"d, type: %d", task->get_seqno(), task->get_type());

        int ret = TFS_SUCCESS;
        int64_t start_time = Func::get_monotonic_time_us();

        // handle task here
        ret = task->handle();

        int64_t end_time = Func::get_monotonic_time_us();

        TBSYS_LOG(INFO, "finish task, seqno: %"PRI64_PREFIX"d, type: %d, cost time: %"PRI64_PREFIX"d",
          task->get_seqno(), task->get_type(), end_time - start_time);

        if (task->is_completed())
        {
          tbsys::gDelete(task);
        }
        else
        {
          TBSYS_LOG(DEBUG, "task not finished, add to map. seqno: %"PRI64_PREFIX"d", task->get_seqno());
          running_task_mutex_.lock();
          running_task_.insert(make_pair<int64_t>(task->get_seqno(), task));
          running_task_mutex_.unlock();
        }
      }

      // when stoped, clear the left tasks
      task_monitor_.lock();
      while (!task_queue_.empty())
      {
        Task* task = task_queue_.front();
        task_queue_.pop_front();
        tbsys::gDelete(task);
      }
      task_monitor_.unlock();
      return TFS_SUCCESS;
    }

    int TaskManager::expire_task()
    {
      running_task_mutex_.lock();
      map<int64_t, Task*>::iterator iter = running_task_.begin();
      uint32_t old_size = running_task_.size();
      for ( ; iter != running_task_.end(); )
      {
        if (iter->second->is_expired())
        {
          TBSYS_LOG(DEBUG, "task expired, seqno: %"PRI64_PREFIX"d", iter->second->get_seqno());
          iter->second->report_to_ns(PLAN_STATUS_TIMEOUT);
          tbsys::gDelete(iter->second);
          running_task_.erase(iter++);
        }
        else
        {
          iter++;
        }
      }
      uint32_t new_size = running_task_.size();
      running_task_mutex_.unlock();

      TBSYS_LOG(DEBUG, "task manager expire task, old: %u, new: %u", old_size, new_size);

      return TFS_SUCCESS;
    }

    void TaskManager::stop()
    {
      stop_ = true;
      clear_cloned_block_map();

      task_monitor_.lock();
      task_monitor_.notifyAll();
      task_monitor_.unlock();
    }

    int TaskManager::add_cloned_block_map(const uint32_t block_id)
    {
      ClonedBlock* cloned_block = new ClonedBlock();

      cloned_block->blockid_ = block_id;
      cloned_block->start_time_ = Func::get_monotonic_time();
      cloned_block_mutex_.lock();
      cloned_block_map_.insert(ClonedBlockMap::value_type(block_id, cloned_block));
      cloned_block_mutex_.unlock();

      return TFS_SUCCESS;
    }

    int TaskManager::del_cloned_block_map(const uint32_t block_id)
    {
      TBSYS_LOG(DEBUG, "del cloned block map blockid :%u", block_id);
      cloned_block_mutex_.lock();
      ClonedBlockMapIter mit = cloned_block_map_.find(block_id);
      if (mit != cloned_block_map_.end())
      {
        tbsys::gDelete(mit->second);
        cloned_block_map_.erase(mit);
      }
      cloned_block_mutex_.unlock();

      return TFS_SUCCESS;
    }

    int TaskManager::expire_cloned_block_map()
    {
      int ret = TFS_SUCCESS;
      int32_t current_time = Func::get_monotonic_time();
      int32_t now_time = current_time - expire_cloned_interval_;
      if (last_expire_cloned_block_time_ < now_time)
      {
        cloned_block_mutex_.lock();
        int old_cloned_block_size = cloned_block_map_.size();
        for (ClonedBlockMapIter mit = cloned_block_map_.begin(); mit != cloned_block_map_.end();)
        {
          if (now_time < 0)
            break;
          if (mit->second->start_time_ < now_time)
          {
            ret = BlockFileManager::get_instance()->del_block(mit->first);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "in check thread: del blockid: %u error. ret: %d", mit->first, ret);
            }

            tbsys::gDelete(mit->second);
            cloned_block_map_.erase(mit++);
          }
          else
          {
            ++mit;
          }
        }

        int32_t new_cloned_block_size = cloned_block_map_.size();
        last_expire_cloned_block_time_ = current_time;
        cloned_block_mutex_.unlock();
        TBSYS_LOG(INFO, "cloned block map: old: %d, new: %d", old_cloned_block_size, new_cloned_block_size);
      }
      return TFS_SUCCESS;
    }

    void TaskManager::clear_cloned_block_map()
    {
      int ret = TFS_SUCCESS;
      cloned_block_mutex_.lock();
      for (ClonedBlockMapIter mit = cloned_block_map_.begin(); mit != cloned_block_map_.end(); ++mit)
      {
        ret = BlockFileManager::get_instance()->del_block(mit->first);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "in check thread: del blockid: %u error. ret: %d", mit->first, ret);
        }

        tbsys::gDelete(mit->second);
      }
      cloned_block_map_.clear();
      cloned_block_mutex_.unlock();
    }
  }
}
