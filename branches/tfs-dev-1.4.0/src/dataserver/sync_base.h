/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_SYNCBASE_H_
#define TFS_DATASERVER_SYNCBASE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <queue>
#include <errno.h>
#include <Monitor.h>
#include <Mutex.h>

#include "sync_backup.h"
#include "common/file_queue.h"
#include "common/file_queue_thread.h"

namespace tfs
{
  namespace dataserver
  {

    class SyncBase
    {
      public:
        explicit SyncBase(const int32_t type);
        ~SyncBase();
        void stop();

        int write_sync_log(const int32_t cmd, const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id = 0);
        int reset_log();
        int disable_log();
        void set_pause(const int32_t v);
        int reload_slave_ip();

        static void* do_sync_mirror(void* args);
        static int do_second_sync(const void* data, const int64_t len, const int32_t thread_index, void* arg);

      private:
        SyncBase();
        DISALLOW_COPY_AND_ASSIGN(SyncBase);

        static const int32_t SYNC_WORK_DIR = 256;
        int32_t stop_;
        int32_t pause_;
        int32_t need_sync_;
        int32_t need_sleep_;
        tbutil::Monitor<tbutil::Mutex> sync_mirror_monitor_;
        common::FileQueue* file_queue_;
        common::FileQueue* second_file_queue_;
        common::FileQueueThread* second_file_queue_thread_;
        SyncBackup* backup_;

      private:
        int run_sync_mirror();
        int do_sync(const char* data, const int32_t len, const bool second = false);
    };

  }
}
#endif //TFS_DATASERVER_SYNCBASE_H_
