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

    enum BlockType
    {
      BLOCK_WRITABLE,
      BLOCK_UPDATE,
      BLOCK_EXPIRED
    };

    class DataService;
    class LeaseManager;
    class BlockManager;
    class WritableBlockManager
    {
      typedef common::TfsSortedVector<WritableBlock*, BlockIdCompare> BLOCK_TABLE;
      typedef BLOCK_TABLE::iterator BLOCK_TABLE_ITER;

      public:
        explicit WritableBlockManager(DataService& service);
        ~WritableBlockManager();

        BlockManager& get_block_manager();
        LeaseManager& get_lease_manager();

        bool empty(const BlockType type);
        int size(const BlockType type);

        // apply write block
        void run_apply_and_giveup();

        // expire all blocks, move to expired list
        int expire_all_blocks();

        // apply and giveup callback
        int callback(common::NewClient* client);

        WritableBlock* insert(const uint64_t block_id,
            const common::ArrayHelper<uint64_t>& servers, const BlockType type);
        WritableBlock* remove(const uint64_t block_id, const BlockType type);
        WritableBlock* get(const uint64_t block_id, const BlockType type);

        // alloc a writable block
        int alloc_writable_block(uint64_t& block_id);
        void free_writable_block(const uint64_t block_id);

        // alloc a block for update
        // if ds doesn't have lease, apply for this block
        int alloc_update_block(const uint64_t block_id);
        void free_update_block(const uint64_t block_id);

      private:
        BLOCK_TABLE* select(const BlockType type);
        int get_blocks(common::ArrayHelper<common::BlockInfoV2> blocks, const BlockType type);
        WritableBlock* insert_(const uint64_t block_id,
            const common::ArrayHelper<uint64_t>& servers, const BlockType type);
        WritableBlock* remove_(const uint64_t block_id, const BlockType type);
        WritableBlock* get_(const uint64_t block_id, const BlockType type);
        int apply_writable_block(const int32_t count);
        int apply_update_block(const int64_t block_id);
        int giveup_writable_block();
        void apply_block_callback(DsApplyBlockResponseMessage* response);
        void giveup_block_callback(DsGiveupBlockResponseMessage* response);

      private:
        DataService& service_;
        BLOCK_TABLE writable_;
        BLOCK_TABLE update_;
        BLOCK_TABLE expired_;
        int32_t write_index_;
        mutable common::RWLock rwmutex_;

      private:
        DISALLOW_COPY_AND_ASSIGN(WritableBlockManager);
    };
  }
}

#endif
