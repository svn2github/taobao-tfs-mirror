/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
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

#ifndef TFS_NAMESERVER_FAMILY_MANAGER_H_
#define TFS_NAMESERVER_FAMILY_MANAGER_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include "ns_define.h"
#include "common/internal.h"
#include "common/array_helper.h"
#include "common/tfs_vector.h"

#include "family_collect.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    struct FamilyIdCompare
    {
      bool operator ()(const FamilyCollect* lhs, const FamilyCollect* rhs) const
      {
        return lhs->get_family_id() < rhs->get_family_id();
      }
    };
    class LayoutManager;
    class FamilyManager
    {
      #ifdef TFS_GTEST
      friend class FamilyManagerTest;
      FRIEND_TEST(FamilyManagerTest, MarshallingItem_get);
      FRIEND_TEST(FamilyManagerTest, MarshallingItem_choose_item_random);
      FRIEND_TEST(FamilyManagerTest, insert_remove_get);
      FRIEND_TEST(FamilyManagerTest, scan);
      FRIEND_TEST(FamilyManagerTest, create_family_choose_data_members);
      #endif
      struct MarshallingItem
      {
        uint32_t rack_;
        int32_t  slot_num_;
        int64_t  last_update_time_;
        int insert(const uint64_t server, const uint32_t block);
        int choose_item_random(std::pair<uint64_t, uint32_t>& pair);
        std::pair<uint64_t, uint32_t> slot_[MAX_SINGLE_RACK_SERVER_NUM];
        void dump(const int32_t level, const char* format = NULL) const;
        #ifdef TFS_GTEST
        bool get(std::pair<uint64_t, uint32_t>& pair, const uint64_t server, const uint32_t block) const;
        #endif
      };
      struct MarshallingItemCompare
      {
        bool operator () (const MarshallingItem* lhs, const MarshallingItem* rhs) const
        {
          return lhs->rack_ < rhs->rack_;
        }
      };
      typedef common::TfsSortedVector<FamilyCollect*, FamilyIdCompare> FAMILY_MAP;
      typedef FAMILY_MAP::iterator FAMILY_MAP_ITER;
      typedef common::TfsSortedVector<MarshallingItem*, MarshallingItemCompare> MARSHALLING_MAP;
      typedef MARSHALLING_MAP::iterator MARSHALLING_MAP_ITER;
    public:
      explicit FamilyManager(LayoutManager& manager);
      virtual ~FamilyManager();
      int insert(const int64_t family_id, const int32_t family_aid_info,
          const common::ArrayHelper<std::pair<uint32_t, int32_t> >& member, const time_t now);
      int update(const int64_t family_id, const uint32_t block, const int32_t version);
      bool exist(int32_t& version, const int64_t family_id, const uint32_t block, const int32_t new_version);
      int remove(GCObject*& object, const int64_t family_id);
      FamilyCollect* get(const int64_t family_id) const;
      bool scan(common::ArrayHelper<FamilyCollect*>& result, int64_t& begin, const int32_t count) const;
      int scan(common::SSMScanParameter& param, int32_t& next, bool& all_over,
          bool& cutover, const int32_t should) const;
      int get_members(common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const int64_t family_id) const;
      int get_members(std::vector<std::pair<uint32_t, uint64_t> >& members, int32_t& family_aid_info, const int64_t family_id) const;
      int get_members(common::ArrayHelper<common::FamilyMemberInfo>& members,
          const common::ArrayHelper<common::FamilyMemberInfo>& abnormal_members, const int64_t family_id) const;
      bool push_to_reinstate_or_dissolve_queue(FamilyCollect* family, const int32_t type);
      FamilyCollect* pop_from_reinstate_or_dissolve_queue();
      bool reinstate_or_dissolve_queue_empty() const;
      int64_t get_reinstate_or_dissolve_queue_size() const;
      bool has_marshalling(int32_t& current_version, const int64_t family_id, const uint32_t block, const int32_t version) const;
      bool push_block_to_marshalling_queues(const BlockCollect* block, const time_t now);
      int push_block_to_marshalling_queues(const uint32_t rack, const uint64_t server, const uint32_t block);
      int marshalling_queue_timeout(const time_t now);
      int64_t get_marshalling_queue_size() const;
      int create_family_choose_data_members(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& members,
          const int32_t data_member_num);
      int create_family_choose_check_members(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& members,
          common::ArrayHelper<uint64_t>& already_exist, const int32_t check_member_num);
      int reinstate_family_choose_members(common::ArrayHelper<uint64_t>& results,
          const int64_t family_id, const int32_t family_aid_info, const int32_t data_member_num);
      int dissolve_family_choose_member_targets_server(common::ArrayHelper<std::pair<uint64_t, uint32_t> >& results,
          const int64_t family_id, const int32_t family_aid_info);
      bool check_need_reinstate(common::ArrayHelper<common::FamilyMemberInfo>& reinstate_members,
          const FamilyCollect* family, const time_t now) const;
      bool check_need_dissolve(const FamilyCollect* family,
          const common::ArrayHelper<common::FamilyMemberInfo>& need_reinstate_members) const;
      bool check_need_compact() const;//TODO
      void dump_marshalling_queue(const int32_t level, const char* format = NULL) const;
    private:
      DISALLOW_COPY_AND_ASSIGN(FamilyManager);
      FamilyCollect* get_(const int64_t family_id) const;
      int insert_(const int64_t family_id, const int32_t family_aid_info,
          const common::ArrayHelper<std::pair<uint32_t, int32_t> >& member, const time_t now);
      int get_members_(common::ArrayHelper<std::pair<uint32_t, int32_t> >& members, const int64_t family_id) const;
      int get_members_(common::ArrayHelper<BlockCollect*>& members, const int64_t family_id) const;
      int get_members_(common::ArrayHelper<std::pair<BlockCollect*, ServerCollect*> >& members, const int64_t family_id) const;
      common::RWLock& get_mutex_(const int64_t family_id) const;
      int32_t get_chunk_(const int64_t family_id) const;
      bool has_marshalling_(int32_t& current_version, const int64_t family_id, const uint32_t block, const int32_t version) const;
    private:
      LayoutManager& manager_;
      MARSHALLING_MAP marshalling_queue_;
      mutable common::RWLock marshallin_queue_mutex_;
      FAMILY_MAP* families_[MAX_FAMILY_CHUNK_NUM];
      mutable common::RWLock mutexs_[MAX_FAMILY_CHUNK_NUM];

      std::deque<int64_t> reinstate_or_dissolve_queue_;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* BLOCKCOLLECT_H_ */
