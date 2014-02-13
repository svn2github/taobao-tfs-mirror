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

#include "task_manager.h"
#include "server_collect.h"
#include "global_factory.h"
#include "layout_manager.h"
#include "common/client_manager.h"
#include "message/block_info_message.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    int TaskManager::Machine::add(const uint64_t server, Task* task, const bool target)
    {
      std::pair<SERVER_TO_TASK_ITER, bool> res =
        target ? target_server_to_task_.insert(SERVER_TO_TASK::value_type(server, task))
               : source_server_to_task_.insert(SERVER_TO_TASK::value_type(server, task));
      return !res.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
    }

    int TaskManager::Machine::remove(const uint64_t server, const bool target)
    {
      int32_t ret = EXIT_TASK_NO_EXIST_ERROR;
      SERVER_TO_TASK_ITER iter;
      if (target)
      {
        iter = target_server_to_task_.find(server);
        ret = target_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
          target_server_to_task_.erase(iter);
      }
      else
      {
        iter = source_server_to_task_.find(server);
        ret = source_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
          source_server_to_task_.erase(iter);
      }
      return ret;
    }

    Task* TaskManager::Machine::get(const uint64_t server) const
    {
      Task* task = NULL;
      SERVER_TO_TASK_CONST_ITER iter = source_server_to_task_.find(server);
      int32_t ret = source_server_to_task_.end() == iter ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        iter = target_server_to_task_.find(server);
        ret = (target_server_to_task_.end() == iter) ? EXIT_TASK_NO_EXIST_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
        task = iter->second;
      return task;
    }

    int64_t TaskManager::Machine::size(int32_t& source_size, int32_t& target_size) const
    {
      source_size = source_server_to_task_.size();
      target_size = target_server_to_task_.size();
      return source_size + target_size;
    }

    int64_t TaskManager::Machine::size() const
    {
      return source_server_to_task_.size() + target_server_to_task_.size();
    }

    TaskManager::TaskManager(LayoutManager& manager):
      manager_(manager),
      seqno_(0)
    {

    }

    TaskManager::~TaskManager()
    {
      TASKS_ITER iter = tasks_.begin();
      for (; iter != tasks_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
    }

    int TaskManager::add(const uint64_t id, const ArrayHelper<uint64_t>& servers,
            const PlanType type, const time_t now)
    {
      int32_t ret = manager_.get_block_manager().has_valid_lease(id,now) ? EXIT_BLOCK_WRITING_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
        std::pair<BLOCK_TO_TASK_ITER, bool> res;
        res.first = block_to_tasks_.find(id);
        ret = block_to_tasks_.end() != res.first ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          Task* task = generation_(id, servers, type);
          assert(NULL != task);
          res = block_to_tasks_.insert(BLOCK_TO_TASK::value_type(id, task));
          std::pair<PENDING_TASK_ITER, bool> rs = pending_queue_.insert(task);
          ret = !rs.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            int64_t index = 0;
            uint64_t server = INVALID_SERVER_ID;
            std::pair<uint64_t, bool> success[MAX_REPLICATION_NUM];
            ArrayHelper<std::pair<uint64_t, bool> > helper(MAX_REPLICATION_NUM, success);
            for (index = 0; index < servers.get_array_index() && TFS_SUCCESS == ret; ++index)
            {
              server = *servers.at(index);
              if (task->type_ != PLAN_TYPE_COMPACT && index < 2)
              {
                std::pair<MACHINE_TO_TASK_ITER, bool> mrs;
                uint64_t machine_ip = (server & 0xFFFFFFFF);
                MACHINE_TO_TASK_ITER mit = machine_to_tasks_.find(machine_ip);
                if (machine_to_tasks_.end() == mit)
                {
                  mrs = machine_to_tasks_.insert(MACHINE_TO_TASK::value_type(machine_ip, Machine()));
                  mit = mrs.first;
                }
                ret = mit->second.add(server, task, is_target(index));
                if (TFS_SUCCESS == ret)
                  helper.push_back(std::make_pair(server, is_target(index)));
              }
            }

            //rollback
            if (TFS_SUCCESS != ret)
            {
              pending_queue_.erase(task);
              for (index = 0; index < helper.get_array_index(); ++index)
              {
                std::pair<uint64_t, bool>* item = helper.at(index);
                if (task->type_ != PLAN_TYPE_COMPACT)
                  remove_(item->first, item->second);
              }
            }
          }

          TBSYS_LOG(DEBUG, "add task result: seqno: %ld, %d", task->seqno_, ret);

          if (TFS_SUCCESS != ret)
          {
            task->dump(TBSYS_LOG_LEVEL(WARN), "add task failed, rollback, ret: %d", ret);
            block_to_tasks_.erase(res.first);
            tbsys::gDelete(task);
          }

          if (TFS_SUCCESS == ret)
          {
            tasks_.insert(TASKS::value_type(task->seqno_, task));
          }
        }
      }
      return ret;
    }

    int TaskManager::add(const int64_t family_id, const int32_t family_aid_info, const PlanType type,
        const int32_t member_num, const FamilyMemberInfo* members, const time_t now)
    {
      UNUSED(now);
      bool flag = (member_num > 0 && NULL != members);
      const int32_t DATA_MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info);
      const int32_t MEMBER_NUM = DATA_MEMBER_NUM + GET_CHECK_MEMBER_NUM(family_aid_info);
      int32_t ret = (flag && INVALID_FAMILY_ID != family_id && MEMBER_NUM == member_num && CHECK_MEMBER_NUM(MEMBER_NUM))? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        Task* task = generation_(family_id, family_aid_info, type, member_num, members);
        assert(NULL != task);

        task->dump(TBSYS_LOG_LEVEL(INFO), "add new task");

        RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
        std::pair<PENDING_TASK_ITER, bool> rs = pending_queue_.insert(task);
        ret = !rs.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          std::pair<FAMILY_TO_TASK_ITER, bool> fres =
              family_to_tasks_.insert(FAMILY_TO_TASK::value_type(family_id, task));
          ret = !fres.second ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            //目前的流控做得简单点，将主控机的流量记1个，后面慢慢优化
            int64_t index = 0;
            std::pair<BLOCK_TO_TASK_ITER, bool> res;
            std::pair<FamilyMemberInfo, bool> success[MEMBER_NUM];
            ArrayHelper<std::pair<FamilyMemberInfo, bool> > helper(MEMBER_NUM, success);
            for (index = 0; index < member_num && TFS_SUCCESS == ret; ++index)
            {
              if (INVALID_SERVER_ID != members[index].server_ && INVALID_BLOCK_ID != members[index].block_)
              {
                bool target = is_target(members[index], index, type, family_aid_info);
                bool insert_block = is_insert_block_(type, MEMBER_NUM, index);
                bool insert_server= is_insert_server_(type, DATA_MEMBER_NUM, index);
                ret = insert_(helper, task, members[index], target, insert_block, insert_server);
                if (TFS_SUCCESS != ret)
                  TBSYS_LOG(INFO, "family_id : %"PRI64_PREFIX"d, block: %"PRI64_PREFIX"u, server: %"PRI64_PREFIX"u", family_id, members[index].block_, members[index].server_);
              }
            }

            //we'll rollback
            if (TFS_SUCCESS != ret)
            {
              for (index = 0; index < helper.get_array_index(); ++index)
              {
                std::pair<FamilyMemberInfo, bool>* item = helper.at(index);
                remove_(item->first.server_,item->second);
                block_to_tasks_.erase(item->first.block_);
              }
            }
          }
          if (TFS_SUCCESS != ret)
          {
            family_to_tasks_.erase(family_id);
          }
        }

        if (TFS_SUCCESS != ret)
        {
          task->dump(TBSYS_LOG_LEVEL(INFO), "add task failed, rollback, ret: %d", ret);
          pending_queue_.erase(task);
          tbsys::gDelete(task);
        }
        if (TFS_SUCCESS == ret)
        {
          tasks_.insert(TASKS::value_type(task->seqno_, task));
        }
      }
      return ret;
    }

    int TaskManager::remove(Task* task)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(task);
    }

    void TaskManager::clear()
    {
      int32_t count = 0;
      bool complete = false;
      const int32_t MAX_QUERY_TASK = 32;
      Task* tasks[MAX_QUERY_TASK];
      ArrayHelper<Task*> helper(MAX_QUERY_TASK, tasks);
      TASKS_ITER iter;
      while (!complete)
      {
        count = 0;
        rwmutex_.wrlock();
        complete = (tasks_.end() == (iter = tasks_.begin()));
        while ((++count < MAX_QUERY_TASK)&& (iter != tasks_.end()))
        {
          helper.push_back((iter->second));
          tasks_.erase(iter++);
        }
        rwmutex_.unlock();
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          Task* task = *helper.at(index);
          manager_.get_gc_manager().insert(task, Func::get_monotonic_time());
        }
      }
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      tasks_.clear();
      machine_to_tasks_.clear();
      block_to_tasks_.clear();
      pending_queue_.clear();
   }

    void TaskManager::dump(tbnet::DataBuffer& stream) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      stream.writeInt32(tasks_.size());
      TASKS_CONST_ITER it = tasks_.begin();
      for (; it != tasks_.end(); ++it)
        it->second->dump(stream);
    }

    void TaskManager::dump(const int32_t level, const char* format) const
    {
      if (level <= TBSYS_LOGGER._level)
      {
        RWLock::Lock lock(rwmutex_, READ_LOCKER);
        TBSYS_LOGGER.logMessage(level, __FILE__, __LINE__, __FUNCTION__,pthread_self(), "%s, tasks size: %zd, block_to_task size: %zd, machine_to_task size: %"PRI64_PREFIX"d, pening_queue_size: %zd",
          format == NULL ? "" : format, tasks_.size(), block_to_tasks_.size(), size_(), pending_queue_.size());
        TASKS_CONST_ITER it = tasks_.begin();
        for (; it != tasks_.end(); ++it)
          it->second->dump(TBSYS_LOG_NUM_LEVEL(level), format);
        BLOCK_TO_TASK_CONST_ITER iter = block_to_tasks_.begin();
        for (; iter != block_to_tasks_.end(); ++iter)
          iter->second->dump(TBSYS_LOG_NUM_LEVEL(level), format);
      }
    }

    Task* TaskManager::get_by_id(const uint64_t id) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      TASKS_CONST_ITER iter = tasks_.find(id);
      return (tasks_.end() == iter) ? NULL : iter->second;
    }

    int64_t TaskManager::get_running_server_size() const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return size_();
    }

    bool TaskManager::exist_block(const uint64_t block) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return (NULL != get_task_by_block_id_(block));
    }

    bool TaskManager::exist_server(const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return (NULL != get_task_by_server_id_(server));
    }

    bool TaskManager::exist_family(const int64_t family_id) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      return (NULL != get_task_by_family_id_(family_id));
    }

    bool TaskManager::exist(const ArrayHelper<uint64_t>& servers) const
    {
      bool ret = !servers.empty();
      if (ret)
      {
        uint64_t server = INVALID_SERVER_ID;
        for (int64_t index = 0; index < servers.get_array_index() && ret; ++index)
        {
          server = *servers.at(index);
          ret = exist_server(server);
        }
      }
      return ret;
    }

    bool TaskManager::get_exist_servers(ArrayHelper<ServerCollect*>& result,
        const ArrayHelper<ServerCollect*>& servers) const
    {
      bool ret = !servers.empty();
      if (ret)
      {
        ServerCollect* server = NULL;
        for (int32_t i = 0; i < servers.get_array_index(); i++)
        {
          server = *servers.at(i);
          assert(NULL != server);
          ret = exist_server(server->id());
          if (ret)
            result.push_back(server);
        }
      }
      return (ret && result.get_array_index() == servers.get_array_index());
    }

    bool TaskManager::has_space_do_task_in_machine(const uint64_t server, const bool target) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t source_size = 0, target_size = 0, total = 0;
      total = get_task_size_in_machine_(source_size, target_size, server);
      TBSYS_LOG(DEBUG, "total = %d target: %d, source:%d, max_task_in_machine_nums_: %d", total ,target_size, source_size, SYSPARAM_NAMESERVER.max_task_in_machine_nums_);
      return target ? target_size < ((SYSPARAM_NAMESERVER.max_task_in_machine_nums_ / 2) + 1)
                    : source_size < ((SYSPARAM_NAMESERVER.max_task_in_machine_nums_ / 2) + 1);
    }

    bool TaskManager::has_space_do_task_in_machine(const uint64_t server) const
    {
      RWLock::Lock lock(rwmutex_, READ_LOCKER);
      int32_t source_size = 0, target_size = 0, total = 0;
      total = get_task_size_in_machine_(source_size, target_size, server);
      //TBSYS_LOG(DEBUG, "total = %d , max_task_in_machine_nums_: %d", total , SYSPARAM_NAMESERVER.max_task_in_machine_nums_);
      return total < SYSPARAM_NAMESERVER.max_task_in_machine_nums_;
    }

    int TaskManager::remove_block_from_dataserver(const uint64_t server, const uint64_t block, const time_t now)
    {
      int32_t ret = remove_block_from_dataserver_(server, block, now);
      TBSYS_LOG(INFO, "send remove block: %"PRI64_PREFIX"u command on server : %s %s",
        block, tbsys::CNetUtil::addrToString(server).c_str(), TFS_SUCCESS == ret ? "successful" : "failed");
      return ret;
    }

    /*
     * expire blocks on dataserver only post expire message to ds, dont care result.
     * @param [in] server dataserver id the one who post to.
     * @param [in] block, the one need expired.
     * @return TFS_SUCCESS success.
     */
    int TaskManager::remove_block_from_dataserver_(const uint64_t server, const uint64_t block, const time_t now)
    {
      RemoveBlockMessageV2 rbmsg;
      rbmsg.set_block_id(block);
      BlockInfoV2 info;
      info.block_id_ = block;
      uint64_t servers[1];
      common::ArrayHelper<uint64_t> helper(1,servers);
      helper.push_back(server);
      manager_.block_oplog_write_helper(OPLOG_RELIEVE_RELATION, info, helper, now);
      std::vector<uint64_t> targets;
      targets.push_back(server);
      int32_t ret = post_msg_to_server(server, &rbmsg, ns_async_callback);
      return ret;
    }

    int64_t TaskManager::size_() const
    {
      int64_t total = 0;
      MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.begin();
      for (;iter != machine_to_tasks_.end(); ++iter)
      {
        total += iter->second.size();
      }
      return total;
    }

    int TaskManager::run()
    {
      time_t now = 0;
      Task* task = NULL;
      int32_t ret = TFS_SUCCESS;
      PENDING_TASK_ITER iter;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();
        if (ngi.in_safe_mode_time(now))
          Func::sleep(SYSPARAM_NAMESERVER.safe_mode_time_, ngi.destroy_flag_);

        task = NULL;
        rwmutex_.wrlock();
        if (!pending_queue_.empty())
        {
          iter = pending_queue_.begin();
          task = *iter;
          pending_queue_.erase(iter);
        }
        rwmutex_.unlock();

        if (NULL != task)
        {
          ret = task->handle();
          if (TFS_SUCCESS != ret)
          {
            task->dump(TBSYS_LOG_LEVEL(WARN), "task handle failed, ret: %d, ", ret);
            remove(task);
          }
        }
        usleep(500);
      }
      return TFS_SUCCESS;
    }

    int TaskManager::handle(BasePacket* msg, const int64_t seqno)
    {
      int32_t ret = (NULL != msg) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        Task* task = NULL;
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        bool master = ngi.is_master();
        if (master)
        {
          task = get_by_id(seqno);
        }
        else
        {
          const uint64_t block  = 0xfffffffa;
          const int32_t MAX_NUM = 2;
          int64_t family_id = 0xfffffffffffffffa;
          int32_t family_aid_info;
          SET_DATA_MEMBER_NUM(family_aid_info, 1);
          SET_CHECK_MEMBER_NUM(family_aid_info, 1);
          SET_MASTER_INDEX(family_aid_info,1);
          uint64_t server[MAX_NUM];
          FamilyMemberInfo info[MAX_NUM];
          if (msg->getPCode() == BLOCK_COMPACT_COMPLETE_MESSAGE)
          {
            task= new (std::nothrow)CompactTask(*this, block, MAX_NUM, server);
          }
          else if (msg->getPCode() == REPLICATE_BLOCK_MESSAGE)
          {
            PlanType type = dynamic_cast<ReplicateBlockMessage*>(msg)->get_move_flag() == REPLICATE_BLOCK_MOVE_FLAG_NO ?
              PLAN_TYPE_REPLICATE : PLAN_TYPE_MOVE;
            task = type == PLAN_TYPE_MOVE ? new (std::nothrow)MoveTask(*this, block, MAX_NUM, server) :
                 new (std::nothrow)ReplicateTask(*this, block, MAX_NUM, server, type);
          }
          else if (msg->getPCode() == REQ_EC_MARSHALLING_COMMIT_MESSAGE)
          {
            task = new (std::nothrow)ECMarshallingTask(*this, family_id, family_aid_info, MAX_NUM, info, PLAN_TYPE_EC_MARSHALLING);
          }
          else if (msg->getPCode() == REQ_EC_REINSTATE_COMMIT_MESSAGE)
          {
            task = new (std::nothrow)ECReinstateTask(*this, family_id, family_aid_info, MAX_NUM, info);
          }
          else if (msg->getPCode() == REQ_EC_DISSOLVE_COMMIT_MESSAGE)
          {
            task = new (std::nothrow)ECDissolveTask(*this, family_id, family_aid_info, MAX_NUM, info);
          }
          assert(NULL != task);
        }
        ret = (NULL != task) ? TFS_SUCCESS : EXIT_TASK_NO_EXIST_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = task->handle_complete(msg);
          task->dump(TBSYS_LOG_LEVEL(INFO), "handle message complete, show result: %d,", ret);
          if (master)
          {
            remove(task);
          }
          else
          {
            tbsys::gDelete(task);
          }
        }
      }
      return ret;
    }

    int TaskManager::timeout(const time_t now)
    {
      int32_t index = 0;
      Task* task = NULL;
      const int32_t MAX_COUNT = 2048;
      Task* expire_tasks[MAX_COUNT];
      ArrayHelper<Task*> helper(MAX_COUNT, expire_tasks);
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      TASKS_ITER iter = tasks_.begin();
      while (iter != tasks_.end() && index < MAX_COUNT)
      {
        ++index;
        task = iter->second;
        if (task->expire(now))
        {
          helper.push_back(task);
          task->runTimerTask();
          tasks_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
      remove_(helper);
      return helper.get_array_index();
    }

    int TaskManager::remove_(Task* task)
    {
      int32_t ret = (NULL != task)? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tasks_.erase(task->seqno_);
        const PlanType type = task->type_;
        if ((PLAN_TYPE_REPLICATE == type)
            || (PLAN_TYPE_MOVE == type)
            || (PLAN_TYPE_COMPACT == type))
        {
          ReplicateTask* ptask = dynamic_cast<ReplicateTask*>(task);
          block_to_tasks_.erase(ptask->block_);
          for (int8_t index = 0; index < ptask->server_num_ && index < 2; ++index)
          {
            if (PLAN_TYPE_COMPACT != type)
              remove_(ptask->servers_[index], is_target(index));
          }
        }

        if ((PLAN_TYPE_EC_MARSHALLING == type)
            || (PLAN_TYPE_EC_REINSTATE == type)
            || (PLAN_TYPE_EC_DISSOLVE == type))
        {
          ECMarshallingTask* ptask = dynamic_cast<ECMarshallingTask*>(task);
          const int32_t MEMBER_NUM = GET_DATA_MEMBER_NUM(ptask->family_aid_info_)
                + GET_CHECK_MEMBER_NUM(ptask->family_aid_info_);
          family_to_tasks_.erase(ptask->family_id_);
          for (int64_t index = 0; index < MEMBER_NUM; ++index)
          {
            if (INVALID_SERVER_ID != ptask->family_members_[index].server_)
            {
              bool target = is_target(ptask->family_members_[index], index, ptask->type_, ptask->family_aid_info_);
              remove_(ptask->family_members_[index].server_, target);
            }
            if (INVALID_BLOCK_ID != ptask->family_members_[index].block_)
              block_to_tasks_.erase(ptask->family_members_[index].block_);
          }
        }
        manager_.get_gc_manager().insert(task, Func::get_monotonic_time());
      }
      return ret;
    }

    int TaskManager::remove_(const uint64_t server, const bool target)
    {
      uint64_t machine_ip = server & 0xFFFFFFFF;
      MACHINE_TO_TASK_ITER iter = machine_to_tasks_.find(machine_ip);
      if (machine_to_tasks_.end() != iter)
      {
        iter->second.remove(server, target);
        if (iter->second.size() <= 0)
        {
          machine_to_tasks_.erase(iter);
        }
      }
      return TFS_SUCCESS;
    }

    bool TaskManager::remove(const ArrayHelper<Task*>& tasks)
    {
      RWLock::Lock lock(rwmutex_, WRITE_LOCKER);
      return remove_(tasks);
    }

    bool TaskManager::remove_(const ArrayHelper<Task*>& tasks)
    {
      bool all_complete = true;
      Task* task = NULL;
      for (int32_t i = 0; i < tasks.get_array_index(); i++)
      {
        task = *tasks.at(i);
        assert(NULL != task);
        int32_t ret = remove_(task);
        if (all_complete)
          all_complete = ret == TFS_SUCCESS;
      }
      return all_complete;
    }

    Task* TaskManager::get_task_by_block_id_(const uint64_t block) const
    {
      BLOCK_TO_TASK_CONST_ITER iter = block_to_tasks_.find(block);
      return (block_to_tasks_.end() == iter) ? NULL : iter->second;
    }

    Task* TaskManager::get_task_by_block_(const BlockCollect* block) const
    {
      return (NULL != block) ? get_task_by_block_id_(block->id()) : NULL;
    }

    Task* TaskManager::get_task_by_server_id_(const uint64_t server) const
    {
      Task* task = NULL;
      uint64_t machine_ip = server & 0xFFFFFFFF;
      MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.find(machine_ip);
      if (machine_to_tasks_.end() != iter)
        task = iter->second.get(server);
      return task;
    }

    Task* TaskManager::get_task_by_server_(const ServerCollect* server) const
    {
      return (NULL != server) ? get_task_by_server_id_(server->id()) : NULL;
    }

    Task* TaskManager::get_task_by_family_id_(const int64_t family_id) const
    {
      Task* task = NULL;
      FAMILY_TO_TASK_CONST_ITER iter = family_to_tasks_.find(family_id);
      if (family_to_tasks_.end() != iter)
        task = iter->second;
      return task;
    }

    int64_t TaskManager::get_task_size_in_machine_(int32_t& source_size, int32_t& target_size, const uint64_t server) const
    {
      uint64_t machine_ip = server & 0xFFFFFFFF;
      MACHINE_TO_TASK_CONST_ITER iter = machine_to_tasks_.find(machine_ip);
      return machine_to_tasks_.end() == iter ? 0 : iter->second.size(source_size, target_size);
    }

    Task* TaskManager::generation_(const uint64_t id, const ArrayHelper<uint64_t>& servers, const PlanType type)
    {
      Task* result = NULL;
      if (type == PLAN_TYPE_REPLICATE)
        result = new (std::nothrow)ReplicateTask(*this, id, servers.get_array_index(), servers.get_base_address(), type);
      else if (type == PLAN_TYPE_MOVE)
        result = new (std::nothrow)MoveTask(*this, id, servers.get_array_index(), servers.get_base_address());
      else if (type == PLAN_TYPE_COMPACT)
        result = new (std::nothrow)CompactTask(*this, id, servers.get_array_index(), servers.get_base_address());
      else if (type == PLAN_TYPE_RESOLVE_VERSION_CONFLICT)
        result = new (std::nothrow)ResolveBlockVersionConflictTask(*this, id, servers.get_array_index(), servers.get_base_address());
      assert(NULL != result);
      result->seqno_ = ++seqno_;
      return result;
    }

    Task* TaskManager::generation_(const int64_t family_id, const int32_t family_aid_info, const PlanType type,
          const int32_t member_num, const FamilyMemberInfo* members)
    {
      Task* result = NULL;
      if (PLAN_TYPE_EC_REINSTATE == type)
        result = new (std::nothrow)ECReinstateTask(*this, family_id, family_aid_info,member_num, members);
      else if (PLAN_TYPE_EC_DISSOLVE == type)
        result = new (std::nothrow)ECDissolveTask(*this, family_id, family_aid_info,member_num, members);
      else if (PLAN_TYPE_EC_MARSHALLING == type)
        result = new (std::nothrow)ECMarshallingTask(*this, family_id, family_aid_info, member_num, members,
            PLAN_TYPE_EC_MARSHALLING);
      assert(NULL != result);
      result->seqno_ = ++seqno_;
      return result;
    }

    bool TaskManager::is_target(const int32_t index)
    {
      return index % 2 == 1;
    }

    bool TaskManager::is_target(const common::FamilyMemberInfo& info, const int32_t index,
         const PlanType type, const int32_t family_aid_info)
    {
      const int32_t DATA_MEMBER_NUM = GET_DATA_MEMBER_NUM(family_aid_info);
      const int32_t MEMBER_NUM = DATA_MEMBER_NUM +  GET_CHECK_MEMBER_NUM(family_aid_info);
      bool target = false;
      if (PLAN_TYPE_EC_MARSHALLING == type)
        target = ((index == GET_MASTER_INDEX(family_aid_info)) || (index >= DATA_MEMBER_NUM));
      else if (PLAN_TYPE_EC_DISSOLVE == type)
        target = (index >= (MEMBER_NUM / 2 ));
      else if (PLAN_TYPE_EC_REINSTATE == type)
        target = (FAMILY_MEMBER_STATUS_ABNORMAL == info.status_);
      return target;
    }

    bool TaskManager::is_insert_block_(const int32_t type, const int32_t member_num, const int32_t index) const
    {
      return (PLAN_TYPE_EC_DISSOLVE == type) ? (index >= member_num / 2) : true;
    }

    bool TaskManager::is_insert_server_(const int32_t type, const int32_t data_member_num, const int32_t index) const
    {
      bool insert = (PLAN_TYPE_EC_DISSOLVE != type);
      if (!insert)
      {
        const int32_t DATA_MEMBER_NUM = data_member_num / 2;
        insert = index % 2 < DATA_MEMBER_NUM;
      }
      return insert;
    }

    int TaskManager::insert_(ArrayHelper<std::pair<FamilyMemberInfo, bool> >& success, Task* task,
          const FamilyMemberInfo& info, const bool target, const bool insert_block, const bool insert_server)
    {
      int32_t ret = (NULL != task && success.get_array_size() > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (insert_block && INVALID_BLOCK_ID != info.block_)
        {
          std::pair<BLOCK_TO_TASK_ITER, bool> res;
          res.first = block_to_tasks_.find(info.block_);
          ret = block_to_tasks_.end() != res.first ? EXIT_TASK_EXIST_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
            res = block_to_tasks_.insert(BLOCK_TO_TASK::value_type(info.block_, task));
        }
        if (insert_server && TFS_SUCCESS == ret && INVALID_SERVER_ID != info.server_)
        {
          std::pair<MACHINE_TO_TASK_ITER, bool> mrs;
          uint64_t machine_ip = (info.server_ & 0xFFFFFFFF);
          MACHINE_TO_TASK_ITER mit = machine_to_tasks_.find(machine_ip);
          if (machine_to_tasks_.end() == mit)
          {
            mrs = machine_to_tasks_.insert(MACHINE_TO_TASK::value_type(machine_ip, Machine()));
            mit = mrs.first;
          }
          ret = mit->second.add(info.server_, task, target);
        }
        success.push_back(std::make_pair(info, target));
      }
      return ret;
    }
  }/** nameserver **/
}/** tfs **/
