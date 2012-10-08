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
#include "logic_block.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
#include "task.h"
#include "common/base_packet.h"
#include "message/message_factory.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace message;

    class Task;

    class TaskManager
    {
      public:
        TaskManager();
        ~TaskManager();

        int init(const uint64_t ns_id, const uint64_t ds_id);

        uint64_t get_ns_id() { return ns_id_; }
        void set_ns_id(const uint64_t ns_id) { ns_id_ = ns_id; }

        uint64_t get_ds_id() { return ds_id_; }
        void set_ds_id(const uint64_t ds_id) { ds_id_ = ds_id; }

        int handle(BaseTaskMessage* packet);
        int handle_complete(BaseTaskMessage* packet);

        int run_task();
        int expire_task();
        void stop();

        void clear_compact_block_map();
        int expire_compact_block_map();

        int add_cloned_block_map(const uint32_t block_id);
        int del_cloned_block_map(const uint32_t block_id);
        int expire_cloned_block_map();
        void clear_cloned_block_map();

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

      private:
        uint64_t ns_id_;
        uint64_t ds_id_;

        bool stop_;
        std::deque<Task*> task_queue_;
        tbutil::Monitor<tbutil::Mutex> task_monitor_;

        std::map<int64_t, Task*> running_task_;
        tbutil::Mutex running_task_mutex_;

        ClonedBlockMap cloned_block_map_;
        tbutil::Mutex cloned_block_mutex_;

        int32_t expire_cloned_interval_;
        int32_t last_expire_cloned_block_time_;
    };
  }
}
#endif //TFS_DATASERVER_TASKMANAGER_H_
