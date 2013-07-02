/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id:
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMESERVER_TASK_H_
#define TFS_NAMESERVER_TASK_H_

#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include <Timer.h>
#include "gc.h"
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class TaskManager;
    class Task : public GCObject
    {
        friend class TaskManager;
      public:
        Task(TaskManager& manager, const common::PlanType type);
        virtual ~Task(){}
        virtual int handle() = 0;
        virtual int handle_complete(common::BasePacket* msg) = 0;
        virtual void dump(tbnet::DataBuffer& stream) = 0;
        virtual void dump(std::stringstream& stream) = 0;
        virtual void dump(const int32_t level, const char* file, const int32_t line,
                          const char* function, pthread_t thid, const char* format, ...) = 0;
        virtual void runTimerTask();
        bool operator < (const Task& task) const;
        bool timeout(const time_t now) const;
        int send_msg_to_server(const uint64_t server, common::BasePacket* msg);
        const char* transform_type_to_str() const;
        const char* transform_status_to_str(const int8_t status) const;
      public:
        TaskManager& manager_;
        int64_t seqno_;
        common::PlanType type_;
        common::PlanStatus status_;
    };

    struct TaskCompare
    {
      bool operator()(const Task* lhs, const Task* rhs) const
      {
        return (*lhs) < (*rhs);
      }
    };

    class ReplicateTask : public Task
    {
      friend class TaskManager;
      #ifdef TFS_GTEST
      friend class TaskTest;
      friend class TaskManagerTest;
      friend class LayoutManagerTest;
      FRIEND_TEST(TaskTest, dump);
      FRIEND_TEST(TaskManagerTest, add);
      FRIEND_TEST(LayoutManagerTest, scan_replicate_queue_);
      FRIEND_TEST(LayoutManagerTest, scan_illegal_block_);
      FRIEND_TEST(LayoutManagerTest, build_replicate_task_);
      FRIEND_TEST(LayoutManagerTest, build_compact_task_);
      FRIEND_TEST(LayoutManagerTest, build_balance_task_);
      FRIEND_TEST(LayoutManagerTest, build_redundant_);
      #endif
      public:
        ReplicateTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers,
          const common::PlanType type);
        virtual ~ReplicateTask();
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg);
        virtual void dump(tbnet::DataBuffer& stream);
        virtual void dump(std::stringstream& stream);
        virtual void dump(const int32_t level, const char* file, const int32_t line,
                          const char* function, pthread_t thid, const char* format, ...);
      protected:
        DISALLOW_COPY_AND_ASSIGN(ReplicateTask);
        uint64_t* servers_;
        uint64_t  block_;
        int8_t    server_num_;
    };

    class MoveTask : public ReplicateTask
    {
      public:
        MoveTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers);
        virtual ~MoveTask(){}
      private:
        DISALLOW_COPY_AND_ASSIGN(MoveTask);
    };

    class CompactTask: public ReplicateTask
    {
        friend class TaskManager;
      public:
        CompactTask(TaskManager& manager, const uint64_t block, const int8_t server_num, const uint64_t* servers);
        virtual ~CompactTask(){};
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg);
      private:
        DISALLOW_COPY_AND_ASSIGN(CompactTask);
    };

    class ECMarshallingTask : public Task
    {
        friend class TaskManager;
        #ifdef TFS_GTEST
        friend class TaskTest;
        FRIEND_TEST(TaskTest, marshalling_handle_complete);
        #endif
      public:
        ECMarshallingTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
            const int32_t member_num, const common::FamilyMemberInfo* members, const common::PlanType type);
        virtual ~ECMarshallingTask();
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg);
        virtual void dump(tbnet::DataBuffer& stream);
        virtual void dump(std::stringstream& stream);
        virtual void dump(const int32_t level, const char* file, const int32_t line,
                          const char* function, pthread_t thid, const char* format, ...);
      private:
        DISALLOW_COPY_AND_ASSIGN(ECMarshallingTask);
      protected:
        common::FamilyMemberInfo* family_members_;
        int64_t family_id_;
        int32_t family_aid_info_;
    };

    class ECReinstateTask : public ECMarshallingTask
    {
        friend class TaskManager;
      public:
        ECReinstateTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
            const int32_t all_member_num , const common::FamilyMemberInfo* members);
        virtual ~ECReinstateTask();
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg);
      protected:
        DISALLOW_COPY_AND_ASSIGN(ECReinstateTask);
    };

    class ECDissolveTask : public ECMarshallingTask
    {
      public:
        ECDissolveTask(TaskManager& manager, const int64_t family_id, const int32_t family_aid_info,
            const int32_t all_member_num , const common::FamilyMemberInfo* members);
        virtual ~ECDissolveTask();
        virtual int handle();
        virtual int handle_complete(common::BasePacket* msg);
      private:
        DISALLOW_COPY_AND_ASSIGN(ECDissolveTask);
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/
#endif /* TASK_H_ */
