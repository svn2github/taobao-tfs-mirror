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
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_COMPACTBLOCK_H_
#define TFS_DATASERVER_COMPACTBLOCK_H_

#include "logic_block.h"
#include "blockfile_manager.h"
#include "dataserver_define.h"
#include "common/interval.h"
#include "common/config.h"
#include "message/message_factory.h"
#include "message/client.h"
#include "message/client_pool.h"
#include <Mutex.h>
#include <Monitor.h>

namespace tfs
{
  namespace dataserver
  {
    
    struct CompactBlkInfo
    {
      uint32_t block_id_;
      int32_t preserve_time_;
      int32_t owner_;
    };

    class CompactBlock
    {
      public:
        CompactBlock();
        CompactBlock(tbutil::Mutex* mutex, message::Client* client, const uint64_t dataserver_id);
        ~CompactBlock();

        // stop compact tasks
        void stop();
        static void* do_compact_block(void* args);

        // add logic block compact task
        int add_cpt_task(CompactBlkInfo* cpt_blk);
        int real_compact(const uint32_t block_id);
        int real_compact(LogicBlock* src, LogicBlock *dest);
        // delete expired compact block files
        int expire_compact_block_map();

      private:
        void init();
        int run_compact_block();

        int clear_compact_block_map();
        int write_big_file(LogicBlock* src, LogicBlock* dest, const common::FileInfo& src_info,
            const common::FileInfo& dest_info, int32_t woffset);
        int req_block_compact_complete(const uint32_t block_id, const int32_t success);

      private:
        DISALLOW_COPY_AND_ASSIGN(CompactBlock);

        std::deque<CompactBlkInfo*> compact_block_queue_;
        tbutil::Monitor<tbutil::Mutex> compact_block_monitor_;
        tbutil::Mutex* client_mutex_;
        message::Client* client_;

        int32_t stop_;
        int32_t expire_compact_interval_;
        int32_t last_expire_compact_block_time_;
        uint64_t dataserver_id_;
    };
  }
}
#endif //TFS_DATASERVER_COMPACTBLOCK_H_
