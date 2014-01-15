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

#ifndef TFS_NAMESERVER_SERVER_COLLECT_H_
#define TFS_NAMESERVER_SERVER_COLLECT_H_

#include <stdint.h>
#include <tbnet.h>
#include "ns_define.h"
#include "common/lock.h"
#include "common/parameter.h"
#include "common/tfs_vector.h"
#include "block_manager.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace nameserver
  {
    class BlockCollect;
    class LayoutManager;
    class ServerCollect : public common::BaseObject<LayoutManager>
    {
      struct BlockIdCompareExt
      {
        bool operator ()(const uint64_t lhs, const uint64_t rhs) const
        {
          return lhs < rhs;
        }
      };

      friend class BlockCollect;
      friend class LayoutManager;
      typedef common::TfsSortedVector<uint64_t, BlockIdCompareExt> BLOCK_TABLE;
      typedef BLOCK_TABLE::iterator BLOCK_TABLE_ITER;
      #ifdef TFS_GTEST
      friend class ServerCollectTest;
      friend class LayoutManagerTest;
      FRIEND_TEST(ServerCollectTest, add);
      FRIEND_TEST(ServerCollectTest, remove);
      FRIEND_TEST(ServerCollectTest, get_range_blocks_);
      FRIEND_TEST(ServerCollectTest, touch);
      FRIEND_TEST(LayoutManagerTest, update_relation);
      #endif
      public:
      ServerCollect(const uint64_t id);
      ServerCollect(const common::DataServerStatInfo& info, const int64_t now);
      virtual ~ServerCollect();

      int add(const uint64_t block, const bool master, const bool writable);
      int remove(const uint64_t block);
      int remove(const common::ArrayHelper<uint64_t>& blocks);
      void statistics(NsGlobalStatisticsInfo& stat) const;
      int add_writable(const uint64_t block, const bool isfull);
      bool calc_regular_create_block_count(const double average_used_capacity,LayoutManager& manager, bool& promote, int32_t& count);
      bool get_range_blocks(const uint64_t begin, BLOCK_TABLE& table, common::ArrayHelper<uint64_t>& blocks) const;
      int scan(common::SSMScanParameter& param, const int8_t scan_flag) const;
      virtual void callback(void* args, LayoutManager& manager);

      int apply_block_for_update(LayoutManager& manager, common::ArrayHelper<common::BlockLease>& result);
      int apply_block(LayoutManager& manager, common::ArrayHelper<common::BlockLease>& result);
      int renew_block(const common::ArrayHelper<common::BlockInfoV2>& input, LayoutManager& manager, common::ArrayHelper<common::BlockLease>& output);
      int giveup_block(const common::ArrayHelper<common::BlockInfoV2>& input,LayoutManager& manager, common::ArrayHelper<common::BlockLease>& result);
      bool has_valid_lease(const int64_t now) const;
      bool renew(const common::DataServerStatInfo& info, const int64_t now, const int32_t times);
      int reset(const common::DataServerStatInfo& info, const int64_t now, LayoutManager& manager);

      int diff(const common::ArrayHelper<common::BlockInfoV2>& input, common::ArrayHelper<uint64_t>& left,
              common::ArrayHelper<common::BlockInfoV2*>& right, common::ArrayHelper<common::BlockInfoV2*>& same) const;
      inline uint64_t id() const { return id_;}
      inline int8_t get_status() const { return status_;}
      inline void set_status(const int8_t status) { status_ = status;}
      inline int8_t get_disk_type() const { return disk_type_;}
      inline bool is_alive() const { return common::SERVICE_STATUS_ONLINE == status_;}
      inline int8_t get_wait_free_phase() const { return wait_free_phase_;}
      inline void set_wait_free_phase(const int8_t wait_free_phase) { wait_free_phase_ = wait_free_phase;}
      inline int64_t use_capacity() const { return use_capacity_;}
      inline int64_t total_capacity() const { return total_capacity_;}
      inline bool is_full() const { return use_capacity_ >= total_capacity_ * common::SYSPARAM_NAMESERVER.max_use_capacity_ratio_ / 100;}
      inline int32_t block_count() const { return block_count_;}
      inline void set_report_block_status(const int8_t status) { rb_status_ = status;}
      inline int8_t get_report_block_status() const { return rb_status_;}
      inline bool is_report_block(const time_t now) const
      {
        return now >= next_report_block_time_ ? true : is_report_block_complete()
                    ? false : is_report_block_expired(now);
      }

      inline bool is_report_block_expired(const time_t now) const
      {
        return (now > rb_expired_time_ && rb_status_ == REPORT_BLOCK_STATUS_REPORTING);
      }
      inline bool is_report_block_complete(void) const { return rb_status_ == REPORT_BLOCK_STATUS_COMPLETE;}
      inline void set_report_block_expire_time(const time_t now)
      {
        rb_expired_time_ = now + common::SYSPARAM_NAMESERVER.report_block_expired_time_;
      }
      void set_next_report_block_time(const time_t now, const int64_t time_seed, const int32_t flag);
      int choose_move_block_random(uint64_t& result) const;
      int expand_ratio(const float expand_ratio = 0.1);

      static const int8_t AVERAGE_USED_CAPACITY_MULTIPLE;
      private:
      DISALLOW_COPY_AND_ASSIGN(ServerCollect);
      int clear_(const int32_t args, LayoutManager& manager);

      bool get_range_blocks_(const uint64_t begin, const BLOCK_TABLE& table, common::ArrayHelper<uint64_t>& blocks) const;
      int remove_(const uint64_t block, BLOCK_TABLE& table);
      int for_each_(const uint64_t begin, const BLOCK_TABLE& table, common::ArrayHelper<uint64_t>& blocks) const;
      bool exist_(const uint64_t block, const BLOCK_TABLE& table) const;
      bool cleanup_invalid_block_(BlockCollect* block);

      private:
      mutable common::RWLock mutex_;
      BLOCK_TABLE* hold_;
      BLOCK_TABLE* writable_;
      BLOCK_TABLE* issued_leases_;
      uint64_t id_;/** lease id or server ip_port **/
      int64_t write_bytes_[common::MAX_RW_STAT_PAIR_NUM];
      int64_t read_bytes_[common::MAX_RW_STAT_PAIR_NUM];
      int64_t write_file_count_[common::MAX_RW_STAT_PAIR_NUM];
      int64_t read_file_count_[common::MAX_RW_STAT_PAIR_NUM];
      int64_t unlink_file_count_[common::MAX_RW_STAT_PAIR_NUM];
      int64_t use_capacity_;
      int64_t total_capacity_;
      int64_t startup_time_;
      int64_t rb_expired_time_;
      int64_t next_report_block_time_;
      uint64_t scan_writable_block_id_;
      int32_t current_load_;
      int32_t block_count_;
      int32_t total_network_bandwith_;
      mutable int32_t write_index_;
      int8_t  reserve_[3];
      int8_t  status_:1;
      int8_t  disk_type_:1;
      int8_t  wait_free_phase_:3;
      int8_t  rb_status_:3;//report block complete status
   };
  }/** nameserver **/
}/** tfs **/

#endif /* SERVERCOLLECT_H_ */
