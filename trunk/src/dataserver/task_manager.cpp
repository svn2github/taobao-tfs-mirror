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
#include "dataservice.h"
#include "erasure_code.h"
#include "task_manager.h"
#include "block_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;
    using namespace std;
    using namespace tbsys;

    TaskManager::TaskManager(DataService& service):
      service_(service)
    {
    }

    TaskManager::~TaskManager()
    {
      // when stoped, clear tasks in queue
      task_monitor_.lock();
      while (!task_queue_.empty())
      {
        Task* task = task_queue_.front();
        task_queue_.pop_front();
        tbsys::gDelete(task);
      }
      task_monitor_.unlock();

      // clear running tasks
      running_task_mutex_.lock();
      map<int64_t, Task*>::iterator iter = running_task_.begin();
      for ( ; iter != running_task_.end(); )
      {
        tbsys::gDelete(iter->second);
        running_task_.erase(iter++);
      }
      running_task_mutex_.unlock();
    }

    BlockManager& TaskManager::get_block_manager()
    {
      return service_.get_block_manager();
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
        case NS_REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
          ret = add_resolve_conflict_task(dynamic_cast<NsReqResolveBlockVersionConflictMessage*>(packet));
          return ret;
        case RESP_DS_REPLICATE_BLOCK_MESSAGE:
        case RESP_DS_COMPACT_BLOCK_MESSAGE:
          ret = handle_complete(packet);
          break;
        default:
          ret = TFS_ERROR;
          TBSYS_LOG(WARN, "unknown pcode : %d",  pcode);
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
      TBSYS_LOG(DEBUG, "handle complete, running task map size %zd", running_task_.size());

      running_task_mutex_.lock();
      std::map<int64_t, Task*>::iterator it = running_task_.find(seqno);
      running_task_mutex_.unlock();

      if (it != running_task_.end())
      {
        Task* task = it->second;
        if (!task->is_completed())
        {
          task->handle_complete(packet);
          if (task->is_completed())
          {
            running_task_mutex_.lock();
            running_task_.erase(seqno);
            running_task_mutex_.unlock();
            get_block_manager().get_gc_manager().add(task);
          }
        }
      }

      return TFS_SUCCESS;
    }

    int TaskManager::add_replicate_task(ReplicateBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      const ReplBlock* repl_info = message->get_repl_block();
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      int ret = TFS_SUCCESS;
      if ((NULL == repl_info) ||
          (seqno < 0) || (expire_time <= 0) ||
          (INVALID_BLOCK_ID == repl_info->block_id_) ||
          (INVALID_SERVER_ID == repl_info->destination_id_))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        ret = check_source(repl_info->source_id_, repl_info->source_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        ReplicateTask* task = new (std::nothrow) ReplicateTask(service_, seqno,
            ds_info.ns_vip_port_, expire_time, *repl_info);
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          get_block_manager().get_gc_manager().add(task);
        }
      }
      return ret;
    }

    int TaskManager::add_compact_task(NsRequestCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint64_t block_id = message->get_block_id();
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      int ret = ((seqno < 0) || (expire_time <= 0) || (INVALID_BLOCK_ID == block_id) ||
          IS_VERFIFY_BLOCK(block_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        CompactTask* task = new (std::nothrow) CompactTask(service_, seqno,
            ds_info.ns_vip_port_, expire_time, block_id);
        task->set_servers(message->get_servers());
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          get_block_manager().get_gc_manager().add(task);
        }
      }
      return ret;
    }

    int TaskManager::add_ds_replicate_task(DsReplicateBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      const uint64_t source_id = message->get_source_id();
      const ReplBlock* repl_info = message->get_repl_block();
      int ret = TFS_SUCCESS;
      if ((NULL == repl_info) ||
          (seqno < 0) || (expire_time <= 0) ||
          (INVALID_BLOCK_ID == repl_info->block_id_) ||
          (INVALID_SERVER_ID == repl_info->destination_id_) ||
          (INVALID_SERVER_ID == source_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        ret = check_source(repl_info->source_id_, repl_info->source_num_);
      }

      if (TFS_SUCCESS == ret)
      {
        ReplicateTask* task = new (std::nothrow) ReplicateTask(service_,
            seqno, source_id, expire_time, *repl_info);
        task->set_task_from_ds();
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          get_block_manager().get_gc_manager().add(task);
        }
      }
      return ret;
    }

    int TaskManager::add_ds_compact_task(DsCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint64_t source_id = message->get_source_id();
      uint64_t block_id = message->get_block_id();

      int ret = ((seqno < 0) || (expire_time <= 0) || (INVALID_BLOCK_ID == block_id) ||
          (INVALID_SERVER_ID == source_id) || IS_VERFIFY_BLOCK(block_id)) ?
          EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        CompactTask* task = new (std::nothrow) CompactTask(service_, seqno, source_id, expire_time, block_id);
        task->set_task_from_ds();
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          get_block_manager().get_gc_manager().add(task);
        }
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
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = check_marshalling(family_id, family_aid_info, family_members);
        if (TFS_SUCCESS == ret)
        {
          MarshallingTask* task = new (std::nothrow) MarshallingTask(service_, seqno,
              ds_info.ns_vip_port_, expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            get_block_manager().get_gc_manager().add(task);
          }
        }
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
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int erased[MAX_MARSHALLING_NUM];
        ret = check_reinstate(family_id, family_aid_info, family_members, erased);
        if (TFS_SUCCESS == ret)
        {
          ReinstateTask* task = new (std::nothrow) ReinstateTask(service_, seqno,
              ds_info.ns_vip_port_, expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info, erased);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            get_block_manager().get_gc_manager().add(task);
          }
        }
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
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = check_dissolve(family_id, family_aid_info, family_members);
        if (TFS_SUCCESS == ret)
        {
          DissolveTask* task = new (std::nothrow) DissolveTask(service_, seqno,
              ds_info.ns_vip_port_, expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            get_block_manager().get_gc_manager().add(task);
          }
        }
      }
      return ret;
    }

    int TaskManager::add_resolve_conflict_task(NsReqResolveBlockVersionConflictMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint64_t block_id = message->get_block();
      uint64_t* servers= message->get_members();
      int32_t size = message->get_size();
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      int ret = ((seqno < 0) || (expire_time <= 0) || INVALID_BLOCK_ID == block_id ||
          (NULL == servers) || (size <= 1)) ?EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ResolveVersionConflictTask* task = new (std::nothrow) ResolveVersionConflictTask(service_,
            seqno, ds_info.ns_vip_port_, expire_time, block_id);
        assert(NULL != task);
        ret = task->set_servers(servers, size);
        if (TFS_SUCCESS == ret)
        {
          ret = add_task_queue(task);
        }
        if (TFS_SUCCESS != ret)
        {
          get_block_manager().get_gc_manager().add(task);
        }
      }

      return ret;
    }

    int TaskManager::add_task_queue(Task* task)
    {
      TBSYS_LOG(DEBUG, "Add task %s", task->dump().c_str());
      task_monitor_.lock();
      task_queue_.push_back(task);
      task_monitor_.notify();
      task_monitor_.unlock();
      return TFS_SUCCESS;
    }

    void TaskManager::run_task()
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        task_monitor_.lock();
        while (!ds_info.is_destroyed() && task_queue_.empty())
        {
          task_monitor_.wait();
        }

        if (ds_info.is_destroyed())
        {
          task_monitor_.unlock();
          break;
        }

        // get the first task
        Task* task = task_queue_.front();
        task_queue_.pop_front();
        task_monitor_.unlock();

        TBSYS_LOG(INFO, "start task, seqno: %"PRI64_PREFIX"d, type: %s, %s",
            task->get_seqno(), task->get_type(), task->dump().c_str());

        int ret = TFS_SUCCESS;
        int64_t start_time = Func::get_monotonic_time_us();

        // handle task here
        ret = task->handle();

        int64_t end_time = Func::get_monotonic_time_us();

        TBSYS_LOG(INFO, "finish task, seqno: %"PRI64_PREFIX"d, type: %s, cost time: %"PRI64_PREFIX"d, ret: %d",
          task->get_seqno(), task->get_type(), end_time - start_time, ret);

        if (task->is_completed())
        {
          get_block_manager().get_gc_manager().add(task);
        }
        else
        {
          TBSYS_LOG(DEBUG, "task not finished, add to map. seqno: %"PRI64_PREFIX"d", task->get_seqno());
          running_task_mutex_.lock();
          running_task_.insert(make_pair<int64_t>(task->get_seqno(), task));
          running_task_mutex_.unlock();
        }
      }
    }

    void TaskManager::stop_task()
    {
      task_monitor_.lock();
      task_monitor_.notifyAll();
      task_monitor_.unlock();
    }

    void TaskManager::expire_task()
    {
      list<Task*> expire_tasks;

      // add all expired task to list
      running_task_mutex_.lock();
      map<int64_t, Task*>::iterator iter = running_task_.begin();
      uint32_t old_size = running_task_.size();
      int64_t now = Func::get_monotonic_time();
      for ( ; iter != running_task_.end(); )
      {
        if (iter->second->is_expired(now))
        {
          TBSYS_LOG(DEBUG, "task expired, seqno: %"PRI64_PREFIX"d", iter->second->get_seqno());
          expire_tasks.push_back(iter->second);
          running_task_.erase(iter++);
        }
        else
        {
          iter++;
        }
      }
      uint32_t new_size = running_task_.size();
      running_task_mutex_.unlock();

     // do real expire work for task in list, report status to nameserver
     list<Task*>::iterator it = expire_tasks.begin();
     for ( ; it != expire_tasks.end(); it++)
     {
       (*it)->report_to_ns(PLAN_STATUS_TIMEOUT);
       get_block_manager().get_gc_manager().add(*it);
     }

     TBSYS_LOG(DEBUG, "task manager expire task, old: %u, new: %u", old_size, new_size);
    }

    int TaskManager::check_source(const uint64_t* servers, const int32_t source_num)
    {
      int ret = ((NULL != servers) && (source_num > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        for (int32_t i = 0; i < source_num; i++)
        {
          if (INVALID_SERVER_ID == servers[i])
          {
            ret = EXIT_PARAMETER_ERROR;
            break;
          }
        }
      }
      return ret;
    }

    int TaskManager::check_family(const int64_t family_id, const int32_t family_aid_info)
    {
      int ret = (INVALID_FAMILY_ID != family_id)? TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info);
        if (!CHECK_MEMBER_NUM_V2(data_num, check_num))
        {
          ret = EXIT_PARAMETER_ERROR;
        }
      }
      return ret;
    }

    int TaskManager::check_marshalling(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members)
    {
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = check_family(family_id, family_aid_info);
      }

      if (TFS_SUCCESS == ret)
      {
        // check if all data ok
        int alive = 0;
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        for (int32_t i = 0; i < data_num; i++)
        {
          if (INVALID_BLOCK_ID != family_members[i].block_ &&
              INVALID_SERVER_ID != family_members[i].server_ &&
              FAMILY_MEMBER_STATUS_NORMAL == family_members[i].status_)
          {
            alive++;
          }
        }

        if (alive < data_num)
        {
          TBSYS_LOG(ERROR, "no enough node to marshalling, alive: %d", alive);
          ret = EXIT_NO_ENOUGH_DATA;
        }
      }

      return ret;
    }

    int TaskManager::check_reinstate(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members, int* erased)
    {
      assert(NULL != erased);
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = check_family(family_id, family_aid_info);
      }

      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info);
        const int32_t member_num = data_num + check_num;

        int alive = 0;
        bool need_recovery = false;
        for (int32_t i = 0; i < member_num; i++)
        {
          // just need data_num nodes to recovery
          if (INVALID_BLOCK_ID != family_members[i].block_ &&
              INVALID_SERVER_ID != family_members[i].server_ &&
              FAMILY_MEMBER_STATUS_NORMAL == family_members[i].status_)
          {
            erased[i] = ErasureCode::NODE_ALIVE;
            alive++;
          }
          else
          {
            erased[i] = ErasureCode::NODE_DEAD;
            need_recovery = true;
          }
        }

        // we need exact data_num nodes to do reinstate
        if (alive < data_num)
        {
          TBSYS_LOG(WARN, "no enough alive node to reinstate, alive: %d", alive);
          ret = EXIT_NO_ENOUGH_DATA;
        }
        else if ((alive > data_num) && need_recovery)
        {
          // random set alive-data_num nodes to UNUSED status
          for (int32_t i = 0; i < alive - data_num; i++)
          {
            int32_t unused = rand() % member_num;
            if (ErasureCode::NODE_ALIVE != erased[unused])
            {
              while (ErasureCode::NODE_ALIVE != erased[unused])
              {
                unused = (unused + 1) % member_num;
              }
            }
            // got here, erased[unused]  mustbe NODE_ALIVE
            erased[unused] = ErasureCode::NODE_UNUSED;
          }
        }

        // all node ok, no need to recovery, just return
        if (TFS_SUCCESS == ret && !need_recovery)
        {
          TBSYS_LOG(INFO, "all nodes are alive, no need do recovery");
          ret = EXIT_NO_NEED_REINSTATE;
        }
      }

      return ret;
    }

    int TaskManager::check_dissolve(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members)
    {
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (INVALID_FAMILY_ID == family_id)
        {
          ret = EXIT_PARAMETER_ERROR;
        }
        else
        {
          const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info) / 2;
          const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info) / 2;
          if (!CHECK_MEMBER_NUM_V2(data_num, check_num))
          {
            ret = EXIT_PARAMETER_ERROR;
          }
        }
      }
      return ret;
    }
  }
}
