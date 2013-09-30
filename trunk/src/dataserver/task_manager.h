/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: task_manager.h 390 2012-08-06 10:11:49Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd@taobao.com
 *      - initial release
 *
 */

#ifndef TFS_DATASERVER_TASKMANAGER_H_
#define TFS_DATASERVER_TASKMANAGER_H_

#include <Mutex.h>
#include <Monitor.h>
#include "ds_define.h"
#include "task.h"
#include "common/base_packet.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace message;

    class BlockMnager;
    class DataService;
    class TaskManager
    {
      public:
        TaskManager(DataService& service);
        ~TaskManager();

        int handle(BaseTaskMessage* packet);
        int handle_complete(BaseTaskMessage* packet);

        void run_task();
        void stop_task();
        void expire_task();

        BlockManager& get_block_manager();

        int add_resolve_conflict_task(const uint64_t blockid, common::VUINT64& servers);

      private:
        DISALLOW_COPY_AND_ASSIGN(TaskManager);

        int add_task_queue(Task* task);
        int add_replicate_task(ReplicateBlockMessage* message);
        int add_compact_task(NsRequestCompactBlockMessage* message);
        int add_ds_replicate_task(DsReplicateBlockMessage* message);
        int add_ds_compact_task(DsCompactBlockMessage* message);
        int add_marshalling_task(ECMarshallingMessage* message);
        int add_reinstate_task(ECReinstateMessage* message);
        int add_dissolve_task(ECDissolveMessage* message);
        int add_resolve_conflict_task(NsReqResolveBlockVersionConflictMessage* message);

        int check_source(const uint64_t* servers, const int32_t source_num);
        int check_family(const int64_t family_id, const int32_t family_aid_info);
        int check_marshalling(const int64_t family_id, const int32_t family_aid_info,
            common::FamilyMemberInfo* family_members);
        int check_reinstate(const int64_t family_id, const int32_t family_aid_info,
            common::FamilyMemberInfo* family_members, int* erased);
        int check_dissolve(const int64_t family_id, const int32_t family_aid_info,
            common::FamilyMemberInfo* family_members);

      private:
        DataService& service_;
        std::deque<Task*> task_queue_;
        tbutil::Monitor<tbutil::Mutex> task_monitor_;

        std::map<int64_t, Task*> running_task_;
        tbutil::Mutex running_task_mutex_;
    };
  }
}
#endif //TFS_DATASERVER_TASKMANAGER_H_
