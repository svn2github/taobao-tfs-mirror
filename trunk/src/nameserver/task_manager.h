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

#ifndef TFS_NAMESERVER_TASK_MANAGER_H_
#define TFS_NAMESERVER_TASK_MANAGER_H_

#include "common/internal.h"
#include "common/error_msg.h"
#include "common/array_helper.h"
#include "task.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class TaskManager
    {
      #ifdef TFS_GTEST
      friend class TaskManagerTest;
      friend class LayoutManagerTest;
      FRIEND_TEST(TaskManagerTest, add);
      FRIEND_TEST(TaskManagerTest, add2);
      FRIEND_TEST(TaskManagerTest, remove_get_exist);
      FRIEND_TEST(TaskManagerTest, has_space_do_task_in_machine);
      FRIEND_TEST(LayoutManagerTest, update_relation);
      FRIEND_TEST(LayoutManagerTest, scan_illegal_block_);
      FRIEND_TEST(LayoutManagerTest, scan_replicate_queue_);
      FRIEND_TEST(LayoutManagerTest, build_replicate_task_);
      FRIEND_TEST(LayoutManagerTest, build_compact_task_);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      FRIEND_TEST(LayoutManagerTest, build_redundant_);
      #endif
      typedef std::map<uint64_t, Task*> SERVER_TO_TASK;
      typedef SERVER_TO_TASK::iterator SERVER_TO_TASK_ITER;
      typedef SERVER_TO_TASK::const_iterator SERVER_TO_TASK_CONST_ITER;
      typedef std::map<uint32_t, Task*> BLOCK_TO_TASK;
      typedef BLOCK_TO_TASK::iterator BLOCK_TO_TASK_ITER;
      typedef BLOCK_TO_TASK::const_iterator BLOCK_TO_TASK_CONST_ITER;
      typedef std::set<Task*, TaskCompare> PENDING_TASK;
      typedef PENDING_TASK::iterator PENDING_TASK_ITER;
      typedef PENDING_TASK::const_iterator PENDING_TASK_CONST_ITER;
      struct Machine
      {
        int add(const uint64_t server, Task* task, const bool target = false);
        int remove(const uint64_t server, const bool target = false);
        Task* get(const uint64_t server) const;
        int64_t size(int32_t& source_size, int32_t& target_size) const;
        int64_t size() const;
        SERVER_TO_TASK source_server_to_task_;
        SERVER_TO_TASK target_server_to_task_;
      };
      typedef std::map<uint64_t, Machine> MACHINE_TO_TASK;
      typedef MACHINE_TO_TASK::iterator MACHINE_TO_TASK_ITER;
      typedef MACHINE_TO_TASK::const_iterator MACHINE_TO_TASK_CONST_ITER;
      typedef std::map<uint64_t, Task*> TASKS;
      typedef TASKS::iterator TASKS_ITER;
      typedef TASKS::const_iterator TASKS_CONST_ITER;
      public:
      explicit TaskManager(LayoutManager& manager);
      virtual ~TaskManager();
      int add(const uint32_t id, const common::ArrayHelper<ServerCollect*>& servers,
          const common::PlanType type, const time_t now);
      int add(const int64_t family_id, const int32_t family_aid_info, const common::PlanType type,
          const int32_t member_num, const common::FamilyMemberInfo* members, const time_t now);
      bool remove(const common::ArrayHelper<Task*>& tasks);
      int remove(Task* task);
      void clear();
      void dump(tbnet::DataBuffer& stream) const;
      void dump(const int32_t level, const char* format = NULL) const;
      int64_t get_running_server_size() const;
      Task* get_by_id(const uint64_t id) const;
      bool exist(const uint32_t block) const;
      bool exist(const uint64_t server) const;
      bool exist(const common::ArrayHelper<ServerCollect*>& servers) const;
      bool get_exist_servers(common::ArrayHelper<ServerCollect*>& result, const common::ArrayHelper<ServerCollect*>& servers) const;
      bool has_space_do_task_in_machine(const uint64_t server, const bool target) const;
      bool has_space_do_task_in_machine(const uint64_t server) const;
      int remove_block_from_dataserver(const uint64_t server, const uint32_t block, const int64_t seqno, const time_t now);
      int run();
      bool handle(common::BasePacket* msg, const int64_t seqno);
      int timeout(const time_t now);
      inline LayoutManager& get_manager() { return manager_;}
      private:
      int remove_(const uint64_t server, const bool target = false);
      int remove_(Task* task);
      bool remove_(const common::ArrayHelper<Task*>& tasks);
      Task* get_(const uint32_t block) const;
      Task* get_(const BlockCollect* block) const;
      Task* get_(const uint64_t server) const;
      Task* get_(const ServerCollect* server) const;
      int64_t get_task_size_in_machine_(int32_t& source_size, int32_t& target_size, const uint64_t server) const;
      int64_t size_() const;
      bool is_target(const int32_t index);
      bool is_target(const common::FamilyMemberInfo& info, const int32_t index,
        const common::PlanType type, const int32_t family_aid_info);
      Task* generation_(const uint32_t id, const common::ArrayHelper<ServerCollect*>& servers,
          const common::PlanType type);
      Task* generation_(const int64_t family_id, const int32_t family_aid_info, const common::PlanType type,
          const int32_t member_num, const common::FamilyMemberInfo* members);
      int insert_(common::ArrayHelper<std::pair<common::FamilyMemberInfo, bool> >& success, Task* task,
          const common::FamilyMemberInfo& info, const bool target, const bool insert);
      int remove_block_from_dataserver_(const uint64_t server, const uint32_t block, const int64_t seqno, const time_t now);
      private:
      LayoutManager& manager_;
      TASKS tasks_;
      MACHINE_TO_TASK machine_to_tasks_;
      BLOCK_TO_TASK block_to_tasks_;
      PENDING_TASK pending_queue_;
      mutable common::RWLock rwmutex_;
      uint64_t seqno_;
    };
  }/** nameserver **/
}/** tfs **/

#endif
