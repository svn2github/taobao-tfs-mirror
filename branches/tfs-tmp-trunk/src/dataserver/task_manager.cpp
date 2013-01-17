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
      service_(service), stop_(false)
    {
    }

    TaskManager::~TaskManager()
    {

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
      TBSYS_LOG(DEBUG, "handle complete, running task map size %zd", running_task_.size());

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
      if ((NULL == repl_info) ||
          (seqno < 0) || (expire_time <= 0) ||
          (INVALID_BLOCK_ID == repl_info->block_id_) ||
          (INVALID_SERVER_ID == repl_info->source_id_) ||
          (INVALID_SERVER_ID == repl_info->destination_id_))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        ReplicateTask* task = new ReplicateTask(service_, seqno,
            service_.get_ns_ipport(), expire_time, *repl_info);
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(task);
        }
      }
      return ret;
    }

    int TaskManager::add_compact_task(NsRequestCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint32_t block_id = message->get_block_id();
      int ret = ((seqno < 0) || (expire_time <= 0) || (INVALID_BLOCK_ID == block_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        CompactTask* task = new CompactTask(service_, seqno,
            service_.get_ns_ipport(), expire_time, block_id);
        task->set_servers(message->get_servers());
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(task);
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
          (INVALID_SERVER_ID == repl_info->source_id_) ||
          (INVALID_SERVER_ID == repl_info->destination_id_) ||
          (INVALID_SERVER_ID == source_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }
      else
      {
        ReplicateTask* task = new ReplicateTask(service_, seqno, source_id, expire_time, *repl_info);
        task->set_task_from_ds();
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(task);
        }
      }
      return ret;
    }

    int TaskManager::add_ds_compact_task(DsCompactBlockMessage* message)
    {
      int64_t seqno = message->get_seqno();
      int32_t expire_time = message->get_expire_time();
      uint64_t source_id = message->get_source_id();
      uint32_t block_id = message->get_block_id();

      int ret = ((seqno < 0) || (expire_time <= 0) || (INVALID_BLOCK_ID == block_id) ||
          (source_id == INVALID_SERVER_ID)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        CompactTask* task = new CompactTask(service_, seqno, source_id, expire_time, block_id);
        task->set_task_from_ds();
        ret = add_task_queue(task);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(task);
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

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = check_marshalling(family_id, family_aid_info, family_members);
        if (TFS_SUCCESS == ret)
        {
          MarshallingTask* task = new MarshallingTask(service_, seqno,
              service_.get_ns_ipport(), expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            tbsys::gDelete(task);
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
      int erased[MAX_MARSHALLING_NUM];

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = check_reinstate(family_id, family_aid_info, family_members, erased);
        if (TFS_SUCCESS == ret)
        {
          ReinstateTask* task = new ReinstateTask(service_, seqno,
              service_.get_ns_ipport(), expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info, erased);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            tbsys::gDelete(task);
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

      int ret = ((seqno < 0) || (expire_time <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = check_dissolve(family_id, family_aid_info, family_members);
        if (TFS_SUCCESS == ret)
        {
          DissolveTask* task = new DissolveTask(service_, seqno,
              service_.get_ns_ipport(), expire_time, family_id);
          ret = task->set_family_info(family_members, family_aid_info);
          if (TFS_SUCCESS == ret)
          {
            ret = add_task_queue(task);
          }

          if (TFS_SUCCESS != ret)
          {
            tbsys::gDelete(task);
          }
        }
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

      task_monitor_.lock();
      task_monitor_.notifyAll();
      task_monitor_.unlock();
    }

    int TaskManager::check_family(const int64_t family_id, const int32_t family_aid_info)
    {
      int ret = (INVALID_FAMILY_ID != family_id)? TFS_SUCCESS: EXIT_INVALID_ARGU_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info);
        if (!CHECK_MEMBER_NUM_V2(data_num, check_num))
        {
          ret = EXIT_INVALID_ARGU_ERROR;
        }
      }
      return ret;
    }

    int TaskManager::check_marshalling(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members)
    {
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_INVALID_ARGU_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = check_family(family_id, family_aid_info);
      }

      if (TFS_SUCCESS == ret)
      {
        // check if all data ok
        int normal_count = 0;
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        for (int32_t i = 0; i < data_num; i++)
        {
          if (INVALID_BLOCK_ID != family_members[i].block_ &&
              INVALID_SERVER_ID != family_members[i].server_ &&
              FAMILY_MEMBER_STATUS_NORMAL == family_members[i].status_)
          {
            normal_count++;
          }
        }

        if (normal_count < data_num)
        {
          TBSYS_LOG(ERROR, "no enough node to marshalling, normal count: %d", normal_count);
          ret = EXIT_NO_ENOUGH_DATA;
        }
      }

      return ret;
    }

    int TaskManager::check_reinstate(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members, int* erased)
    {
      assert(NULL != erased);
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_INVALID_ARGU_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = check_family(family_id, family_aid_info);
      }

      if (TFS_SUCCESS == ret)
      {
        const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info);
        const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info);
        const int32_t member_num = data_num + check_num;

        bool need_recovery = false;
        int normal_count = 0;
        for (int32_t i = 0; i < member_num; i++)
        {
          // just need data_num nodes to recovery
          if (INVALID_BLOCK_ID != family_members[i].block_ &&
              INVALID_SERVER_ID != family_members[i].server_ &&
              FAMILY_MEMBER_STATUS_NORMAL == family_members[i].status_)
          {
            if (normal_count < data_num)
            {
              erased[i] = ErasureCode::NODE_ALIVE;
              normal_count++;
            }
            else
            {
              erased[i] = ErasureCode::NODE_UNUSED;
            }
          }
          else
          {
            erased[i] = ErasureCode::NODE_DEAD;
            need_recovery = true;
          }
        }

        if (normal_count < data_num)
        {
          TBSYS_LOG(ERROR, "no enough normal node to reinstate, normal count: %d", normal_count);
          ret = EXIT_NO_ENOUGH_DATA;
        }

        // all node ok, no need to recovery, just return
        if (TFS_SUCCESS == ret && !need_recovery)
        {
          TBSYS_LOG(INFO, "all nodes are normal, no need do recovery");
          ret = EXIT_NO_NEED_REINSTATE;
        }
      }

      return ret;
    }

    int TaskManager::check_dissolve(const int64_t family_id, const int32_t family_aid_info,
        common::FamilyMemberInfo* family_members)
    {
      int ret = (NULL != family_members)? TFS_SUCCESS: EXIT_INVALID_ARGU_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (INVALID_FAMILY_ID == family_id)
        {
          ret = EXIT_INVALID_ARGU_ERROR;
        }
        else
        {
          const int32_t data_num = GET_DATA_MEMBER_NUM(family_aid_info) / 2;
          const int32_t check_num = GET_CHECK_MEMBER_NUM(family_aid_info) / 2;
          if (!CHECK_MEMBER_NUM_V2(data_num, check_num))
          {
            ret = EXIT_INVALID_ARGU_ERROR;
          }
        }
      }
      return ret;
    }
  }
}
