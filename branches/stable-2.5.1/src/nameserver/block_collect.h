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

#ifndef TFS_NAMESERVER_BLOCK_COLLECT_H_
#define TFS_NAMESERVER_BLOCK_COLLECT_H_

#include <stdint.h>
#include <time.h>
#include <vector>
#include "ns_define.h"
#include "common/internal.h"
#include "common/base_object.h"
#include "common/parameter.h"
#include "common/array_helper.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class ServerCollect;
    class LayoutManager;
    class BlockCollect : public common::BaseObject<LayoutManager>
    {
      #ifdef TFS_GTEST
      friend class BlockCollectTest;
      FRIEND_TEST(BlockCollectTest, add);
      FRIEND_TEST(BlockCollectTest, remove);
      FRIEND_TEST(BlockCollectTest, exist);
      FRIEND_TEST(BlockCollectTest, check_replicate);
      FRIEND_TEST(BlockCollectTest, check_compact);
      #endif
      public:
      typedef std::pair<uint64_t, int32_t> SERVER_ITEM;
      typedef std::vector<SERVER_ITEM> SERVER_MAP;
      typedef SERVER_MAP::iterator SERVER_ITER;
      typedef SERVER_MAP::const_iterator CONST_SERVER_ITER;
      public:
      explicit BlockCollect(const uint64_t block_id);
      BlockCollect(const uint64_t block_id, const time_t now);
      virtual ~BlockCollect();

      int add(bool& writable, bool& master, const uint64_t server, const common::BlockInfoV2* info, const time_t now);
      int remove(const uint64_t server, const time_t now);
      bool exist(const uint64_t server) const;
      bool is_master(const uint64_t server) const;
      inline bool is_writable() const { return ((!is_full()) && check_copies_complete() && is_equal_group(id()));}
      inline bool is_creating() const { return BLOCK_CREATE_FLAG_YES == create_flag_;}
      inline bool in_replicate_queue() const { return BLOCK_IN_REPLICATE_QUEUE_YES == in_replicate_queue_;}
      inline bool has_lease() const   { return BLOCK_HAS_LEASE_FLAG_YES == has_lease_;}
      inline bool has_valid_lease(const time_t now) const { return (has_lease() && !expire(now));}
      common::PlanPriority check_replicate(const time_t now) const;
      bool check_compact(const time_t now, const bool check_in_family) const;
      bool check_balance(const time_t now) const;
      bool check_reinstate(const time_t now) const;
      bool check_marshalling(const time_t now) const;
      bool check_need_adjust_copies_location(common::ArrayHelper<std::pair<uint64_t, int32_t> >& adjust_copies, const time_t now) const;
      void callback(void * args, LayoutManager& manger);
      void cleanup(common::ArrayHelper<uint64_t>& expires);
      int scan(common::SSMScanParameter& param) const;
      void get_servers(common::ArrayHelper<uint64_t>& servers) const;
      uint64_t get_server(const int8_t index = 0) const;
      void resolve_version_conflict(common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper);
      int apply_lease(const uint64_t server, const time_t now, const int32_t step, const bool update,
        common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper);
      int renew_lease(const uint64_t server, const time_t now, const int32_t step, const bool update,
        const common::BlockInfoV2& info,common::ArrayHelper<std::pair<uint64_t, int32_t> >& helper);
      int giveup_lease(const uint64_t server, const time_t now, const common::BlockInfoV2* info);
      void update_version(const common::ArrayHelper<uint64_t>& helper, const int32_t step);
      void dump(int32_t level, const char* file = __FILE__,
          const int32_t line = __LINE__, const char* function = __FUNCTION__, const pthread_t thid = pthread_self()) const;
      inline int32_t size() const { return info_.size_;}
      inline void update(const common::BlockInfoV2& info) { if (info.version_ > info_.version_) {info_ = info;}}
      inline uint64_t id() const { return info_.block_id_;}
      inline int32_t version() const { return info_.version_;}
      inline void set_create_flag(const int8_t flag) { create_flag_ = flag;}
      inline void set_in_replicate_queue(const int8_t flag) {in_replicate_queue_ = flag;}
      inline void set_has_lease(const int8_t flag) { has_lease_ = flag;}
      inline int8_t get_servers_size() const { return servers_.size();}
      inline int64_t get_family_id() const { return info_.family_id_;};
      inline void set_family_id(const int64_t family_id) { info_.family_id_ = family_id;}
      inline bool is_in_family() const { return common::INVALID_FAMILY_ID != info_.family_id_;}
      inline bool is_full() const { return ((info_.size_ + common::BLOCK_RESERVER_LENGTH) >= common::SYSPARAM_NAMESERVER.max_block_size_
                                            || info_.file_count_ >= common::MAX_SINGLE_BLOCK_FILE_COUNT); }
      inline bool check_copies_complete() const
      {
        uint32_t copies = is_in_family() ? 1 : common::SYSPARAM_NAMESERVER.max_replication_;
        return servers_.size() == copies;
      }
      inline const common::BlockInfoV2& get_block_info() const { return info_;}
      SERVER_ITEM* get_server_item(const uint64_t server);
    private:
      inline int32_t get_delete_file_num_ratio_() const
      {
        return info_.file_count_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.del_file_count_)
              / static_cast<float>(info_.file_count_))) : 0;
      }
      inline int32_t get_delete_file_size_ratio_() const
      {
        return info_.size_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.del_size_)
                / static_cast<float>(info_.size_))) : 0;
      }
      inline int32_t get_update_file_num_ratio_() const
      {
        return info_.file_count_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.update_file_count_)
              / static_cast<float>(info_.file_count_))) : 0;
      }
      inline int32_t get_update_file_size_ratio_() const
      {
        return info_.size_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.update_size_)
                / static_cast<float>(info_.size_))) : 0;
      }
      SERVER_ITEM* get_(const uint64_t server);
      void update_all_version_(const int32_t version);
      inline bool leave_timeout_(const int64_t now) const
      {
        return (last_leave_time_ + common::SYSPARAM_NAMESERVER.replicate_wait_time_) < now;
      }
      inline bool has_master_() const { return BLOCK_CHOOSE_MASTER_COMPLETE_FLAG_YES == choose_master_;}
      private:
      DISALLOW_COPY_AND_ASSIGN(BlockCollect);//56 + 8 * (max_replication + 1) + 4
      SERVER_MAP servers_;
      common::BlockInfoV2 info_; //56
      int64_t last_leave_time_;
      uint8_t reserve_[3];
      uint8_t reserve_bit_:4;
      uint8_t choose_master_:1;
      uint8_t create_flag_:1;
      uint8_t in_replicate_queue_:1;
      uint8_t has_lease_:1;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* BLOCKCOLLECT_H_ */
