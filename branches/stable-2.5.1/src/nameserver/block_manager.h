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

#ifndef TFS_NAMESERVER_BLOCK_MANAGER_H_
#define TFS_NAMESERVER_BLOCK_MANAGER_H_

#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"
#include "block_collect.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
   class BlockManager
    {
        struct BlockIdCompare
        {
          bool operator ()(const BlockCollect* lhs, const BlockCollect* rhs) const
          {
            return lhs->id() < rhs->id();
          }
        };
        #ifdef TFS_GTEST
        friend class BlockManagerTest;
        friend class LayoutManagerTest;
        FRIEND_TEST(BlockManagerTest, insert_remove_get_exist);
        FRIEND_TEST(BlockManagerTest, get_servers);
        FRIEND_TEST(BlockManagerTest, scan);
        FRIEND_TEST(BlockManagerTest, update_relation);
        FRIEND_TEST(BlockManagerTest, build_relation);
        FRIEND_TEST(BlockManagerTest, update_block_info);
        FRIEND_TEST(BlockManagerTest, scan_ext);
        void clear_();
        #endif
        typedef common::TfsSortedVector<BlockCollect*, BlockIdCompare> BLOCK_MAP;
        typedef common::TfsSortedVector<BlockCollect*, BlockIdCompare>::iterator BLOCK_MAP_ITER;
        friend void BlockCollect::callback(void* args, LayoutManager& manager);
      public:
        explicit BlockManager(LayoutManager& manager);
        virtual ~BlockManager();
        BlockCollect* insert(const uint64_t block, const time_t now, const bool set = false);
        bool remove(BlockCollect*& object, const uint64_t block);

        bool push_to_delete_queue(const uint64_t block, const uint64_t server);
        bool pop_from_delete_queue(std::pair<uint64_t, uint64_t>& output);
        bool delete_queue_empty() const;
        void clear_delete_queue();

        //emergency replicate method only call by build thread,no lock
        bool push_to_emergency_replicate_queue(BlockCollect* block);
        BlockCollect* pop_from_emergency_replicate_queue();
        bool has_emergency_replicate_in_queue() const;
        int64_t get_emergency_replicate_queue_size() const;

        BlockCollect* get(const uint64_t block) const;
        bool exist(const uint64_t block) const;
        bool scan(common::ArrayHelper<BlockCollect*>& result, uint64_t& begin, const int32_t count) const;
        int scan(common::SSMScanParameter& param, int32_t& next, bool& all_over,
            bool& cutover, const int32_t should) const;
        int get_servers(common::ArrayHelper<uint64_t>& server, const uint64_t block) const;
        int get_servers(common::ArrayHelper<uint64_t>& server, const BlockCollect* block) const;
        int get_servers_size(const uint64_t block) const;
        int get_servers_size(const BlockCollect* const pblock) const;
        uint64_t get_server(const uint64_t block, const int8_t index = 0) const;
        bool exist(const BlockCollect* block, const ServerCollect* server) const;

        int update_relation(std::vector<uint64_t>& cleanup_family_id_array, ServerCollect* server,
            const common::ArrayHelper<common::BlockInfoV2>& blocks, const time_t now);
        int build_relation(BlockCollect* block, bool& writable, bool& master, const uint64_t server,
            const time_t now, const bool set =false);
        int relieve_relation(BlockCollect* block, const uint64_t server, const time_t now);
        int relieve_relation(const uint64_t block, const uint64_t server, const time_t now);
        int update_block_info(BlockCollect*& output, bool& isnew, bool& writable, bool& master,
            const common::BlockInfoV2& info, const ServerCollect* server, const time_t now, const bool addnew);
        int update_block_info(const common::BlockInfoV2& info, BlockCollect* block);

        int set_family_id(const uint64_t block, const uint64_t family_id);

        bool need_replicate(const BlockCollect* block) const;
        bool need_replicate(const BlockCollect* block, const time_t now) const;
        bool need_replicate(common::ArrayHelper<uint64_t>& servers, common::PlanPriority& priority,
             const BlockCollect* block, const time_t now) const;
        bool need_compact(const BlockCollect* block, const time_t now, const bool check_in_family = true) const;
        bool need_compact(common::ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now, const bool check_in_family = true) const;
        bool need_balance(common::ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now) const;
        bool need_marshalling(const uint64_t block, const time_t now);
        bool need_marshalling(const BlockCollect* block, const time_t now);
        bool need_marshalling(common::ArrayHelper<uint64_t>& servers, const BlockCollect* block, const time_t now) const;
        bool need_reinstate(const BlockCollect* block, const time_t now) const;
        bool check_version_conflict(const BlockCollect* block, const time_t now) const;
        bool check_version_conflict(const BlockCollect* block, const time_t now, common::ArrayHelper<uint64_t>& servers) const;
        bool need_adjust_copies_location(common::ArrayHelper<uint64_t>& adjust_copies, const BlockCollect* block, const time_t now) const;

        int expand_ratio(int32_t& index, const float expand_ratio = 0.1);

        int update_block_last_wirte_time(uint64_t& id, const uint64_t block, const time_t now);
        bool has_valid_lease(const uint64_t block, const time_t now) const;
        bool has_valid_lease(const BlockCollect* pblock, const time_t now) const;
        int apply_lease(const uint64_t server, const time_t now, const int32_t step, const bool update, const uint64_t block);
        int apply_lease(const uint64_t server, const time_t now, const int32_t step, const bool update, BlockCollect* pblock);
        int renew_lease(const uint64_t server, const time_t now, const int32_t step, const bool update, const common::BlockInfoV2& info, const uint64_t block);
        int renew_lease(const uint64_t server, const time_t now, const int32_t step, const bool update, const common::BlockInfoV2& info, BlockCollect* pblock);
        int giveup_lease(const uint64_t block, const uint64_t server, const time_t now, const common::BlockInfoV2* info);
        void timeout(const time_t now);
      private:
        DISALLOW_COPY_AND_ASSIGN(BlockManager);
        common::RWLock& get_mutex_(const uint64_t block) const;

        BlockCollect* get_(const uint64_t block) const;

        BlockCollect* insert_(const uint64_t block, const time_t now, const bool set = false);
        BlockCollect* remove_(const uint64_t block);

        int build_relation_(BlockCollect* block, bool& writable, bool& master,
            const uint64_t server, const time_t now, const bool set = false);

        bool pop_from_delete_queue_(std::pair<uint64_t, uint64_t>& output);

        int32_t get_chunk_(const uint64_t block) const;

        int get_servers_(common::ArrayHelper<uint64_t>& server, const BlockCollect* block) const;

        int update_relation_(const common::ArrayHelper<common::BlockInfoV2*>& blocks, const time_t now,
              const bool change, std::vector<uint64_t>& cleanup_family_id_array, ServerCollect* server);
        int relieve_relation_(ServerCollect* server, const common::ArrayHelper<common::BlockInfoV2>& blocks, const time_t now);

      private:
        LayoutManager& manager_;
        uint64_t last_traverse_block_;
        mutable common::RWLock rwmutex_[MAX_BLOCK_CHUNK_NUMS];
        BLOCK_MAP * blocks_[MAX_BLOCK_CHUNK_NUMS];

        tbutil::Mutex delete_queue_mutex_;
        std::deque<std::pair<uint64_t, uint64_t> > delete_queue_;

        tbutil::Mutex emergency_replicate_queue_mutex_;
        std::deque<uint64_t> emergency_replicate_queue_;
    };
  }/** nameserver **/
}/** tfs **/

#endif /* BLOCKCHUNK_H_ */
