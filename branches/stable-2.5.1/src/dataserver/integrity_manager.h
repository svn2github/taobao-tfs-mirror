/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_DATASERVER_INTEGRITYMANAGER_H_
#define TFS_DATASERVER_INTEGRITYMANAGER_H_

#include <Mutex.h>
#include "common/internal.h"
#include "message/message_factory.h"
#include "common/error_msg.h"
#include "ds_define.h"

namespace tfs
{
  namespace dataserver
  {
    class BlockManager;
    class DataService;

    class IntegrityManager
    {
      public:
        IntegrityManager(DataService& service);
        ~IntegrityManager();
        inline BlockManager& get_block_manager();
        inline TaskManager& get_task_manager();
        inline DataHelper& get_data_helper();
        void run_check();

     private:
        bool should_check_now();
        void check_for_crash(const int32_t crash_time);
        bool check_one(const uint64_t block_id, const bool force = false);
        int get_dirty_expire_time();

     private:
        DISALLOW_COPY_AND_ASSIGN(IntegrityManager);
        DataService& service_;
    };
  }
}
#endif
