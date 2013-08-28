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
#ifndef TFS_DATASERVER_WRITABLE_BLOCK_MANAGER_H_
#define TFS_DATASERVER_WRITABLE_BLOCK_MANAGER_H_

#include <Mutex.h>
#include "common/atomic.h"
#include "common/internal.h"
#include "common/lock.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"
#include "message/ds_lease_message.h"
#include "data_file.h"
#include "ds_define.h"
#include "writable_block.h"

namespace tfs
{
  namespace dataserver
  {
    struct BlockIdCompare
    {
      bool operator ()(const WritableBlock* lhs, const WritableBlock* rhs) const
      {
        assert(NULL != lhs);
        assert(NULL != rhs);
        return lhs->get_block_id() < rhs->get_block_id();
      }
    };

    class DataService;
    class LeaseManager;
    class BlockManager;
    class WritableBlockManager
    {
      #ifdef TFS_GTEST
      friend class TestWritableBlockManager;
      FRIEND_TEST(TestWritableBlockManager, general);
      FRIEND_TEST(TestWritableBlockManager, apply_callback);
      FRIEND_TEST(TestWritableBlockManager, giveup_callback);
      #endif

      typedef common::TfsSortedVector<WritableBlock*, BlockIdCompare> BLOCK_TABLE;
      typedef BLOCK_TABLE::iterator BLOCK_TABLE_ITER;

      public:
        explicit WritableBlockManager(DataService& service);
        ~WritableBlockManager();

        BlockManager& get_block_manager();

        int32_t size(const BlockType type);

        // expire all blocks, move to expired list
        void expire_one_block(const uint64_t block_id);
        void expire_update_blocks();
        void expire_all_blocks();

        // apply and giveup callback
        int callback(common::NewClient* client);

        WritableBlock* insert(const uint64_t block_id,
            const common::ArrayHelper<uint64_t>& servers, const BlockType type);
        WritableBlock* remove(const uint64_t block_id);
        WritableBlock* get(const uint64_t block_id);

        // alloc a writable block
        int alloc_writable_block(WritableBlock*& block);
        void free_writable_block(const uint64_t block_id);

        // alloc a block for update
        // if ds doesn't have lease, apply for this block
        int alloc_update_block(const uint64_t block_id, WritableBlock*& block);

        // used when renew lease & expire block
        void get_blocks(common::ArrayHelper<common::BlockInfoV2> blocks, const BlockType type);

        // apply block helper function
        int apply_writable_block(const int32_t count);
        int apply_update_block(const int64_t block_id);
        int giveup_writable_block();

      private:
        int32_t& select_size(const BlockType type);
        WritableBlock* insert_(const uint64_t block_id,
            const common::ArrayHelper<uint64_t>& servers, const BlockType type);
        WritableBlock* remove_(const uint64_t block_id);
        WritableBlock* get_(const uint64_t block_id);
        void apply_block_callback(message::DsApplyBlockResponseMessage* response);
        void giveup_block_callback(message::DsGiveupBlockResponseMessage* response);
        int process_apply_update_block(message::DsApplyBlockForUpdateResponseMessage* response);

      private:
        DataService& service_;
        BLOCK_TABLE writable_;
        int32_t writable_size_;
        int32_t update_size_;
        int32_t expired_size_;
        int32_t write_index_;
        mutable common::RWLock rwmutex_;

      private:
        DISALLOW_COPY_AND_ASSIGN(WritableBlockManager);
    };
  }
}

#endif
