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
      FRIEND_TEST(BlockCollectTest, check_version);
      FRIEND_TEST(BlockCollectTest, check_replicate);
      FRIEND_TEST(BlockCollectTest, check_compact);
      #endif
      public:
      explicit BlockCollect(const uint64_t block_id);
      BlockCollect(const uint64_t block_id, const time_t now);
      virtual ~BlockCollect();

      int add(bool& writable, bool& master, const uint64_t server, const time_t now);
      int remove(const uint64_t server, const time_t now);
      bool exist(const uint64_t server) const;
      inline bool is_master(const uint64_t server) const { return (NULL != servers_ && common::INVALID_SERVER_ID != server && server_size_ > 0) ? server == servers_[0] : false;}
      inline bool is_writable() const { return ((!is_full()) && check_copies_complete() && is_equal_group(id()) && !has_version_conflict());}
      inline bool is_creating() const { return BLOCK_CREATE_FLAG_YES == create_flag_;}
      inline bool in_replicate_queue() const { return BLOCK_IN_REPLICATE_QUEUE_YES == in_replicate_queue_;}
      inline bool has_lease() const   { return BLOCK_HAS_LEASE_FLAG_YES == has_lease_;}
      inline bool has_valid_lease(const time_t now) const { return (has_lease() && !expire(now));}
      inline bool has_version_conflict() const { return BLOCK_HAS_VERSION_CONFLICT_FLAG_YES == has_version_conflict_;}
      int check_version(LayoutManager& manager, common::ArrayHelper<uint64_t>& expires,
            const uint64_t server, const bool isnew, const common::BlockInfoV2& info, const time_t now);
      int check_version(common::ArrayHelper<uint64_t>& expires, const common::BlockInfoV2& info, const uint64_t server);
      common::PlanPriority check_replicate(const time_t now) const;
      bool check_compact(const time_t now, const bool check_in_family) const;
      bool check_balance(const time_t now) const;
      bool check_reinstate(const time_t now) const;
      bool check_marshalling(const time_t now) const;
      bool check_version_conflict(const time_t now) const;
      bool check_need_adjust_copies_location(common::ArrayHelper<uint64_t>& adjust_copies, const time_t now) const;
      void callback(void * args, LayoutManager& manger);
      void cleanup(common::ArrayHelper<uint64_t>& expires);
      int scan(common::SSMScanParameter& param) const;
      void get_servers(common::ArrayHelper<uint64_t>& servers) const;
      uint64_t get_server(const int8_t index = 0) const;
      void dump(int32_t level, const char* file = __FILE__,
          const int32_t line = __LINE__, const char* function = __FUNCTION__, const pthread_t thid = pthread_self()) const;
      inline int32_t size() const { return info_.size_;}
      inline void update(const common::BlockInfoV2& info) { info_ = info;}
      inline uint64_t id() const { return info_.block_id_;}
      inline int32_t version() const { return info_.version_;}
      inline void update_version(const int32_t step) { info_.version_ += step;}
      inline void set_create_flag(const int8_t flag) { create_flag_ = flag;}
      inline void set_in_replicate_queue(const int8_t flag) {in_replicate_queue_ = flag;}
      inline void set_has_lease(const int8_t flag) { has_lease_ = flag;}
      inline void set_has_version_conflict(const int8_t flag) { has_version_conflict_ = flag;}
      inline int8_t get_servers_size() const { return server_size_;}
      inline int64_t get_family_id() const { return info_.family_id_;};
      inline void set_family_id(const int64_t family_id) { info_.family_id_ = family_id;}
      inline bool is_in_family() const { return common::INVALID_FAMILY_ID != info_.family_id_;}
      inline bool is_full() const { return ((info_.size_ + common::BLOCK_RESERVER_LENGTH) >= common::SYSPARAM_NAMESERVER.max_block_size_
                                            || info_.file_count_ >= common::MAX_SINGLE_BLOCK_FILE_COUNT); }
      inline bool check_copies_complete() const
      {
        int32_t copies = is_in_family() ? 1 : common::SYSPARAM_NAMESERVER.max_replication_;
        return server_size_ == copies;
      }
      inline int32_t get_delete_file_num_ratio() const
      {
        return info_.file_count_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.del_file_count_)
              / static_cast<float>(info_.file_count_))) : 0;
      }
      inline int32_t get_delete_file_size_ratio() const
      {
        return info_.size_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.del_size_)
                / static_cast<float>(info_.size_))) : 0;
      }
      inline int32_t get_update_file_num_ratio() const
      {
        return info_.file_count_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.update_file_count_)
              / static_cast<float>(info_.file_count_))) : 0;
      }
      inline int32_t get_update_file_size_ratio() const
      {
        return info_.size_ > 0 ? (static_cast<int32_t>(100 * static_cast<float>(info_.update_size_)
                / static_cast<float>(info_.size_))) : 0;
      }
      private:
      DISALLOW_COPY_AND_ASSIGN(BlockCollect);//56 + 8 * (max_replication + 1) + 4
      uint64_t* servers_;
      common::BlockInfoV2 info_; //56
      uint8_t  reserve_[3];
      uint8_t  server_size_:4;
      uint8_t create_flag_:1;
      uint8_t in_replicate_queue_:1;
      uint8_t has_lease_:1;
      uint8_t has_version_conflict_:1;
    };
  }/** end namespace nameserver **/
}/** end namespace tfs **/

#endif /* BLOCKCOLLECT_H_ */
