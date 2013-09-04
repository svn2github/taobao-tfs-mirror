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
#ifndef TFS_DATASERVER_CHECKMANAGER_H_
#define TFS_DATASERVER_CHECKMANAGER_H_

#include <Mutex.h>
#include "common/internal.h"
#include "message/message_factory.h"
#include "common/error_msg.h"
#include "ds_define.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    class SyncBase;
    class DataHelper;
    class BlockManager;
    class DataService;
    class CheckManager
    {
      #ifdef TFS_GTEST
      friend class TestCheckManager;
      FRIEND_TEST(TestCheckManager, test_compare_block_fileinfos);
      #endif

      public:
        CheckManager(DataService& service);
        ~CheckManager();
        BlockManager& get_block_manager();
        DataHelper& get_data_helper();
        std::vector<SyncBase*>& get_sync_mirror();

        void run_check();
        int handle(tbnet::Packet* packet);

      private:
        int get_check_blocks(message::CheckBlockRequestMessage* message);
        int add_check_blocks(message::ReportCheckBlockMessage* message);
        void add_check_blocks(const int64_t seqno, const uint64_t check_server_id, const common::VUINT64& blocks);
        int report_check_blocks();
        int check_block(const uint64_t block_id);
        int check_single_block(const uint64_t block_id,
            std::vector<common::FileInfoV2>& finfos, const std::string& dest_addr);
        int process_more_files(const uint64_t dest_ns_addr,
            const uint64_t block_id, const std::vector<common::FileInfoV2>& more);
        int process_diff_files(const uint64_t dest_ns_addr,
            const uint64_t block_id, const std::vector<common::FileInfoV2>& diff);
        int process_less_files(const uint64_t dest_ns_addr,
            const uint64_t block_id, const std::vector<common::FileInfoV2>& less);
        void compare_block_fileinfos(const std::vector<common::FileInfoV2>& left,
            const std::vector<common::FileInfoV2>& right,
            std::vector<common::FileInfoV2>& more,
            std::vector<common::FileInfoV2>& diff,
            std::vector<common::FileInfoV2>& less);

     private:
        DataService& service_;
        std::deque<uint64_t> pending_blocks_;
        std::vector<uint64_t> success_blocks_;
        tbutil::Mutex mutex_;
        int64_t seqno_;
        int64_t check_server_id_;
    };
  }
}
#endif
