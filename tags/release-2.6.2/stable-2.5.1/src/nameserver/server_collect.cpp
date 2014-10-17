/* * (C) 2007-2010 Alibaba Group Holding Limited.
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
#include "server_collect.h"
#include "block_collect.h"
#include "global_factory.h"
#include <tbsys.h>
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t ServerCollect::AVERAGE_USED_CAPACITY_MULTIPLE = 2;
    static void calculate_server_init_block_parameter(int32_t& block_nums, int32_t& min_expand_size,
        float& expand_ratio, const int64_t capacity)
    {
      static const int32_t init_block_nums[] = {2048, 4096, 8192, 16384, 16384};
      static const int32_t init_min_expand_size[] = {256, 512,1024,1024,1024};
      static const float   init_expand_ratio[] = {0.1, 0.1, 0.1,0.1,0.1};
      static const int64_t GB = 1 * 1024 * 1024 * 1024;
      int32_t index = 0;
      int64_t capacity_gb = capacity / GB;
      if (capacity_gb <= 300)//300GB
        index = 0;
      else if (capacity_gb > 300 && capacity_gb <= 600)//600GB
        index = 1;
      else if (capacity_gb > 600 && capacity_gb <= 1024)//1024GB
        index = 2;
      else if (capacity_gb > 1024 && capacity_gb <= 2048)//2048GB
        index = 3;
      else if (capacity_gb > 2048) //> 2048GB
        index = 4;
      block_nums = init_block_nums[index];
      min_expand_size = init_min_expand_size[index];
      expand_ratio = init_expand_ratio[index];
    }

   ServerCollect::ServerCollect(const uint64_t id):
      BaseObject<LayoutManager>(0),
      hold_(NULL),
      writable_(NULL),
      issued_leases_(NULL),
      id_(id)
    {
      //for query
    }

    ServerCollect::ServerCollect(const DataServerStatInfo& info, const int64_t now):
      BaseObject<LayoutManager>(now),
      id_(info.id_),
      use_capacity_(info.use_capacity_),
      total_capacity_(info.total_capacity_),
      startup_time_(info.startup_time_),
      rb_expired_time_(0),
      next_report_block_time_(0),
      scan_writable_block_id_(0),
      current_load_(info.current_load_ <= 0 ? 1 : info.current_load_),
      block_count_(info.block_count_),
      total_network_bandwith_(128),//MB
      write_index_(0),
      status_(SERVICE_STATUS_ONLINE),
      disk_type_(info.type_),
      wait_free_phase_(OBJECT_WAIT_FREE_PHASE_NONE),
      rb_status_(REPORT_BLOCK_FLAG_NO)
    {
        float   expand_ratio = 0.0;
        int32_t block_nums = 0, min_expand_size = 0;
        for (int8_t index = 0; index < MAX_RW_STAT_PAIR_NUM; ++index)
        {
          write_bytes_[index] = 0;
          read_bytes_[index] = 0;
          write_file_count_[index] = 0;
          read_file_count_[index] = 0;
          unlink_file_count_[index] = 0;
        }
        calculate_server_init_block_parameter(block_nums, min_expand_size, expand_ratio, total_capacity_);
        hold_     = new (std::nothrow)TfsSortedVector<uint64_t, BlockIdCompareExt>(block_nums, min_expand_size, expand_ratio);
        writable_ = new (std::nothrow)TfsSortedVector<uint64_t, BlockIdCompareExt>(block_nums / 4, min_expand_size / 4, expand_ratio);
        issued_leases_= new (std::nothrow)TfsSortedVector<uint64_t, BlockIdCompareExt>(MAX_WRITE_FILE_COUNT, 16, 0.1);
        assert(NULL != hold_);
        assert(NULL != writable_);
        assert(NULL != issued_leases_);
    }

    ServerCollect::~ServerCollect()
    {
      tbsys::gDelete(hold_);
      tbsys::gDelete(writable_);
      tbsys::gDelete(issued_leases_);
    }

    int ServerCollect::add(const uint64_t block, const bool master, const bool writable)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        uint64_t result;
        ret = hold_->insert_unique(result, block);
        if (((TFS_SUCCESS == ret)
             ||(EXIT_ELEMENT_EXIST == ret))
            && (writable)
            && (master)
            && (is_equal_group(block))
            && !IS_VERFIFY_BLOCK(block))
        {
          ret = writable_->insert_unique(result, block);
        }
      }
      return ret;
    }

    int ServerCollect::remove(const uint64_t block)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      remove_(block, *hold_);
      remove_(block, *writable_);
      remove_(block, *issued_leases_);
      return TFS_SUCCESS;
    }

    int ServerCollect::remove(const ArrayHelper<uint64_t>& blocks)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      for (int64_t index = 0; index < blocks.get_array_index(); ++index)
      {
        uint64_t block = *blocks.at(index);
        remove_(block, *hold_);
        remove_(block, *writable_);
        remove_(block, *issued_leases_);
      }
      return TFS_SUCCESS;
    }

    void ServerCollect::statistics(NsGlobalStatisticsInfo& stat) const
    {
      stat.use_capacity_ += use_capacity_;
      stat.total_capacity_ += total_capacity_;
      stat.total_load_ += current_load_;
      stat.total_block_count_ += block_count_;
    }

    int ServerCollect::add_writable(const uint64_t block, const bool isfull)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (!isfull && is_equal_group(block) && !IS_VERFIFY_BLOCK(block)) ? TFS_SUCCESS : EXIT_BLOCK_FULL;
        if (TFS_SUCCESS == ret)
        {
          RWLock::Lock lock(mutex_, WRITE_LOCKER);
          uint64_t result = INVALID_SERVER_ID;
          ret = writable_->insert_unique(result, block);
        }
      }
      return ret;
    }

    bool ServerCollect::calc_regular_create_block_count(const double average_used_capacity,LayoutManager& manager, int32_t& count)
    {
      bool ret = (count > 0 && total_capacity_ > 0 && use_capacity_ >= 0
                  && DATASERVER_DISK_TYPE_FULL == get_disk_type() && rb_status_ != REPORT_BLOCK_FLAG_YES);
      if (!ret)
      {
        count = 0;
      }
      else
      {
        mutex_.rdlock();
        int32_t current = issued_leases_->size();
        mutex_.unlock();
        TBSYS_LOG(DEBUG, "%s calc regular create block count, current : %d", CNetUtil::addrToString(id()).c_str(), current);
        count = current >= SYSPARAM_NAMESERVER.max_write_file_count_ ? 0 :
                  std::min(count, (SYSPARAM_NAMESERVER.max_write_file_count_ - current));
        if (count > 0)
        {
          int64_t index  = 0, actual = 0;
          const int32_t MAX_QUERY_NUM = 128;
          uint64_t blocks[MAX_QUERY_NUM];
          uint64_t invalid_blocks[MAX_QUERY_NUM];
          ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
          ArrayHelper<uint64_t> invalid(MAX_QUERY_NUM,invalid_blocks);
          bool complete = get_range_blocks(scan_writable_block_id_, *writable_, helper);

          BlockCollect* block = NULL;
          double use_capacity_ratio = static_cast<double>(use_capacity_) / static_cast<double>(total_capacity_);
          double max_average_use_capacity_ratio = average_used_capacity * AVERAGE_USED_CAPACITY_MULTIPLE;
          for (index = 0; index < helper.get_array_index() && actual < count; ++index)
          {
            int64_t now = Func::get_monotonic_time();
            scan_writable_block_id_ = *helper.at(index);
            assert(INVALID_BLOCK_ID != scan_writable_block_id_);
            block = manager.get_block_manager().get(scan_writable_block_id_);
            if (NULL == block)  // dissove maybe happened here
              continue;
            if (cleanup_invalid_block_(block, now, false))
              continue;
            RWLock::Lock lock(mutex_, READ_LOCKER);
            if (!exist_(scan_writable_block_id_, *issued_leases_))
            {
              ++actual;
            }
          }
          if (complete)
            scan_writable_block_id_ = 0;
          if (!is_full() && ((use_capacity_ratio < max_average_use_capacity_ratio)
                || max_average_use_capacity_ratio <= PERCENTAGE_MIN))
            count -= actual;
          else
            count = 0;
        }
      }
      return ret;
    }

    int ServerCollect::scan(SSMScanParameter& param, const int8_t scan_flag) const
    {
      if (scan_flag & SSM_CHILD_SERVER_TYPE_INFO)
      {
        param.data_.writeInt64(id_);
        param.data_.writeInt64(use_capacity_);
        param.data_.writeInt64(total_capacity_);
        param.data_.writeInt32(current_load_);
        param.data_.writeInt32(block_count_);
        param.data_.writeInt64(this->get());
        param.data_.writeInt64(startup_time_);
        for (int8_t index = 0; index < MAX_RW_STAT_PAIR_NUM; ++index)
        {
          param.data_.writeInt64(write_bytes_[index]);
          param.data_.writeInt64(read_bytes_[index]);
          param.data_.writeInt64(write_file_count_[index]);
          param.data_.writeInt64(read_file_count_[index]);
          param.data_.writeInt64(unlink_file_count_[index]);
        }
        param.data_.writeInt64(time(NULL));
        param.data_.writeInt32(status_);
        param.data_.writeInt64(rb_expired_time_);
        param.data_.writeInt64(next_report_block_time_);
        param.data_.writeInt8(disk_type_);
        param.data_.writeInt8(rb_status_);
      }

      RWLock::Lock lock(mutex_, READ_LOCKER);
      if (scan_flag & SSM_CHILD_SERVER_TYPE_HOLD)
      {
        param.data_.writeInt32(hold_->size());
        for (BLOCK_TABLE_ITER iter = hold_->begin(); iter != hold_->end(); iter++)
        {
          param.data_.writeInt64((*iter));
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_WRITABLE)
      {
        param.data_.writeInt32(writable_->size());
        for (BLOCK_TABLE_ITER iter = writable_->begin(); iter != writable_->end(); iter++)
        {
          param.data_.writeInt64((*iter));
        }
      }

      if (scan_flag & SSM_CHILD_SERVER_TYPE_MASTER)
      {
        param.data_.writeInt32(issued_leases_->size());
        for (BLOCK_TABLE_ITER iter = issued_leases_->begin(); iter != issued_leases_->end(); iter++)
        {
          param.data_.writeInt64((*iter));
        }
      }
      return TFS_SUCCESS;
    }

    bool ServerCollect::get_range_blocks(const uint64_t begin, BLOCK_TABLE& table, ArrayHelper<uint64_t>& blocks) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return get_range_blocks_(begin, table, blocks);
    }

    void ServerCollect::callback(void* args, LayoutManager& manager)
    {
      assert(NULL != args);
      int32_t* flag = reinterpret_cast<int32_t*>(args);
      TBSYS_LOG(DEBUG, "server %s callback, args: %d", CNetUtil::addrToString(id()).c_str(), *flag);
      clear_(*flag, manager);
    }

    int ServerCollect::apply_block_for_update(LayoutManager& manager, ArrayHelper<BlockLease>& result)
    {
      ServerItem arrays[MAX_REPLICATION_NUM];
      common::ArrayHelper<ServerItem> helper(MAX_REPLICATION_NUM, arrays);
      ServerItem family_arrays[MAX_REPLICATION_NUM];
      common::ArrayHelper<ServerItem> clean_family_helper(MAX_REPLICATION_NUM, family_arrays);

      int64_t now = Func::get_monotonic_time();
      BlockManager& block_manager = manager.get_block_manager();
      for (int64_t index = 0; index < result.get_array_index(); ++index)
      {
        helper.clear();
        clean_family_helper.clear();
        BlockLease* entry = result.at(index);
        assert(NULL != entry);
        BlockCollect* pblock = NULL;
        int32_t& ret = entry->result_;
        ret = is_equal_group(entry->block_id_) ? TFS_SUCCESS : EXIT_BLOCK_NOT_IN_CURRENT_GROUP;
        if (TFS_SUCCESS == ret)
        {
          pblock = block_manager.get(entry->block_id_);
          ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = manager.get_task_manager().exist_block(entry->block_id_) ? EXIT_BLOCK_BUSY : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.apply_lease(id(), now, get() - now, true, pblock, helper, clean_family_helper);
          invalid_block_copies_(manager, helper, clean_family_helper, entry->block_id_);
        }
        if (TFS_SUCCESS == ret)
        {
          uint64_t rt = INVALID_BLOCK_ID;
          RWLock::Lock lock(mutex_, WRITE_LOCKER);
          ret = issued_leases_->insert_unique(rt , entry->block_id_);
          assert((TFS_SUCCESS == ret || EXIT_ELEMENT_EXIST == ret));
          if (EXIT_ELEMENT_EXIST == ret)
            ret = TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
          ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, entry->servers_);
          block_manager.get_servers(servers, pblock);
          entry->size_ = servers.get_array_index();
          if (!(SYSPARAM_NAMESERVER.global_switch_ & ENABLE_INCOMPLETE_UPDATE))
          {
            int32_t expect_size = pblock->is_in_family() ? 1 : common::SYSPARAM_NAMESERVER.max_replication_;
            ret = entry->size_ != expect_size ? EXIT_BLOCK_COPIES_INCOMPLETE : TFS_SUCCESS;
          }
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "%s apply update block %"PRI64_PREFIX"u fail, ret %d",
              CNetUtil::addrToString(id()).c_str(), entry->block_id_, ret);
        }
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::apply_block(LayoutManager& manager, ArrayHelper<BlockLease>& result)
    {
      result.clear();
      uint64_t start = 0, now = 0;
      bool all_over = false;
      int32_t ret = TFS_SUCCESS;
      const int32_t MAX_QUERY_NUM = 2048;
      uint64_t blocks[MAX_QUERY_NUM];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
      BlockManager& block_manager = manager.get_block_manager();
      while (!all_over && result.get_array_index() < result.get_array_size())
      {
        helper.clear();
        now = Func::get_monotonic_time();
        all_over = get_range_blocks(start, *writable_, helper);
        ServerItem arrays[MAX_REPLICATION_NUM];
        common::ArrayHelper<ServerItem> invalid_helper(MAX_REPLICATION_NUM, arrays);
        ServerItem family_arrays[MAX_REPLICATION_NUM];
        common::ArrayHelper<ServerItem> clean_family_helper(MAX_REPLICATION_NUM, family_arrays);
        for (int64_t index = 0; index < helper.get_array_index() && result.get_array_index() < result.get_array_size(); ++index)
        {
          invalid_helper.clear();
          start = *helper.at(index);
          ret = (exist_(start, *issued_leases_)) ? EXIT_LEASE_EXISTED : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ret = manager.get_task_manager().exist_block(start) ? EXIT_BLOCK_BUSY : TFS_SUCCESS;
          }
          if (TFS_SUCCESS == ret)
          {
            ret = block_manager.apply_lease(id(), now, get() - now, false, start, invalid_helper, clean_family_helper);
            invalid_block_copies_(manager, invalid_helper, clean_family_helper, start);
          }
          if (TFS_SUCCESS == ret)
          {
            uint64_t rt = INVALID_BLOCK_ID;
            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            ret = issued_leases_->insert_unique(rt , start);
            if (EXIT_ELEMENT_EXIST == ret)
              ret = TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
            {
              block_manager.giveup_lease(id(), now, NULL, start);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            result.push_back(BlockLease());
            BlockLease* entry = result.at(result.get_array_index() - 1);
            assert(NULL != entry);
            entry->block_id_ = start;
            entry->result_   = TFS_SUCCESS;
            ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, entry->servers_);
            block_manager.get_servers(servers, start);
            entry->size_ = servers.get_array_index();
          }
        }
      }
      std::stringstream rts;
      print_lease(result, rts);
      TBSYS_LOG(DEBUG, "%s apply block: %s", tbsys::CNetUtil::addrToString(id()).c_str(), rts.str().c_str());
      return TFS_SUCCESS;
    }

    int ServerCollect::renew_block(const ArrayHelper<BlockInfoV2>& input,LayoutManager& manager, ArrayHelper<BlockLease>& output)
    {
      ServerItem arrays[MAX_REPLICATION_NUM];
      common::ArrayHelper<ServerItem> invalid_helper(MAX_REPLICATION_NUM, arrays);
      ServerItem family_arrays[MAX_REPLICATION_NUM];
      common::ArrayHelper<ServerItem> clean_family_helper(MAX_REPLICATION_NUM, family_arrays);
      BlockManager& block_manager = manager.get_block_manager();
      for (int64_t index = 0; index < input.get_array_index(); ++index)
      {
        invalid_helper.clear();
        int64_t now = Func::get_monotonic_time();
        output.push_back(BlockLease());
        BlockLease* result = output.at(index);
        BlockInfoV2* entry = input.at(index);
        result->block_id_ = entry->block_id_;
        int32_t& ret = result->result_;

        BlockCollect* pblock = block_manager.get(entry->block_id_);
        ret = (pblock != NULL) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          ret = manager.get_task_manager().exist_block(entry->block_id_) ? EXIT_BLOCK_BUSY : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.renew_lease(id(), now, get() - now, false, *entry, pblock, invalid_helper, clean_family_helper);
          invalid_block_copies_(manager, invalid_helper, clean_family_helper, entry->block_id_);
        }
        if (TFS_SUCCESS == ret)
        {
          RWLock::Lock lock(mutex_, READ_LOCKER);
          ret = exist_(pblock->id(), *issued_leases_) ? TFS_SUCCESS : EXIT_BLOCK_WRITING_ERROR;
        }
        if (TFS_SUCCESS == ret)
        {
          ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, result->servers_);
          block_manager.get_servers(servers, pblock);
          result->size_ = servers.get_array_index();
          write_block_oplog_(manager, OPLOG_UPDATE, *entry, now);
        }
        else
        {
          cleanup_invalid_block_(pblock, now);
        }
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::giveup_block(const ArrayHelper<BlockInfoV2>& input,LayoutManager& manager, ArrayHelper<BlockLease>& output)
    {
      BlockCollect* pblock = NULL;
      BlockManager& block_manager = manager.get_block_manager();
      for (int64_t index = 0; index < input.get_array_index(); ++index)
      {
        output.push_back(BlockLease());
        BlockLease* result = output.at(index);
        BlockInfoV2* entry = input.at(index);
        int64_t now = Func::get_monotonic_time();
        result->block_id_ = entry->block_id_;
        int32_t& ret = result->result_;
        pblock = block_manager.get(entry->block_id_);
        ret = (pblock != NULL) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          uint64_t server = pblock->get_master();
          ret = (id() == server) ? TFS_SUCCESS : EXIT_CANNOT_GIVEUP_LEASE;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.giveup_lease(id(), now, entry, entry->block_id_);
        }
        if (TFS_SUCCESS == ret)
        {
          cleanup_invalid_block_(pblock, now);
          write_block_oplog_(manager, OPLOG_UPDATE, *entry, now);
        }
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::giveup_block(const uint64_t block, LayoutManager& manager)
    {
      BlockManager& block_manager = manager.get_block_manager();
      int64_t now = Func::get_monotonic_time();
      BlockCollect* pblock = block_manager.get(block);
      int32_t ret = (pblock != NULL) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        uint64_t server = pblock->get_master();
        ret = (id() == server) ? TFS_SUCCESS : EXIT_CANNOT_GIVEUP_LEASE;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager.giveup_lease(id(), now, NULL, pblock);
      }
      if (TFS_SUCCESS == ret)
      {
        cleanup_invalid_block_(pblock, now);
      }
      return ret;
    }

    bool ServerCollect::has_valid_lease(const int64_t now) const
    {
      return now < this->get();
    }

    bool ServerCollect::renew(const DataServerStatInfo& info, const int64_t now, const int32_t times)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      bool result = has_valid_lease(now);
      if (result)
      {
        this->set(now, times);
        use_capacity_ = info.use_capacity_;
        total_capacity_ = info.total_capacity_;
        total_network_bandwith_ = info.total_network_bandwith_;
        current_load_ = info.current_load_;
        block_count_ = info.block_count_;
        for (int8_t index = 0; index < MAX_RW_STAT_PAIR_NUM; ++index)
        {
          write_bytes_[index] = info.write_bytes_[index];
          read_bytes_[index] = info.read_bytes_[index];
          write_file_count_[index] = info.write_file_count_[index];
          read_file_count_[index] = info.read_file_count_[index];
          unlink_file_count_[index] = info.unlink_file_count_[index];
        }
      }
      return  result;
    }

    int ServerCollect::reset(const DataServerStatInfo& info, const int64_t now, LayoutManager& manager)
    {
      UNUSED(now);
      int32_t args = CALL_BACK_FLAG_CLEAR;
      callback(reinterpret_cast<void*>(&args), manager);
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      rb_expired_time_ = 0;
      next_report_block_time_ = 0;
      scan_writable_block_id_ = 0;
      rb_status_ = REPORT_BLOCK_FLAG_NO;
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      total_network_bandwith_ = info.total_network_bandwith_;
      current_load_ = info.current_load_;
      block_count_ = info.block_count_;
      startup_time_ = info.startup_time_;
      disk_type_   = info.type_;
      wait_free_phase_ = OBJECT_WAIT_FREE_PHASE_NONE;
      for (int8_t index = 0; index < MAX_RW_STAT_PAIR_NUM; ++index)
      {
        write_bytes_[index] = info.write_bytes_[index];
        read_bytes_[index] = info.read_bytes_[index];
        write_file_count_[index] = info.write_file_count_[index];
        read_file_count_[index] = info.read_file_count_[index];
        unlink_file_count_[index] = info.unlink_file_count_[index];
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::diff(const ArrayHelper<BlockInfoV2>& input, ArrayHelper<uint64_t>& left,
              ArrayHelper<BlockInfoV2*>& right, ArrayHelper<BlockInfoV2*>& same) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      uint64_t left_element = INVALID_BLOCK_ID;
      BlockInfoV2* right_element = NULL;
      int64_t left_index = 0, right_index = 0;
      while (left_index < hold_->size() && right_index < input.get_array_index())
      {
        left_element = hold_->at(left_index);
        right_element= input.at(right_index);
        assert(NULL != right_element);
        if (left_element == right_element->block_id_)
        {
          ++left_index;
          ++right_index;
          same.push_back(right_element);
        }
        if (left_element < right_element->block_id_)
        {
          ++left_index;
          left.push_back(left_element);
        }
        if (left_element > right_element->block_id_)
        {
          ++right_index;
          right.push_back(right_element);
        }
      }
      for (;left_index < hold_->size(); ++left_index)
      {
        left.push_back(hold_->at(left_index));
      }

      for (; right_index < input.get_array_index(); ++right_index)
      {
        right_element = input.at(right_index);
        assert(NULL != right_element);
        right.push_back(right_element);
      }
      return left.get_array_index() + right.get_array_index();
    }

    void ServerCollect::set_next_report_block_time(const time_t now, const int64_t time_seed, const bool force)
    {
      time_t current = time(NULL), next = time(NULL);
      int32_t SAFE_MODE_TIME = next_report_block_time_ > 0 ? SYSPARAM_NAMESERVER.safe_mode_time_ :
          SYSPARAM_NAMESERVER.safe_mode_time_ - (now - GFactory::get_runtime_info().startup_time_);
      SAFE_MODE_TIME = std::max(SAFE_MODE_TIME, 120);
      if (common::SYSPARAM_NAMESERVER.report_block_time_interval_ > 0 && force)
      {
        next += (SYSPARAM_NAMESERVER.report_block_time_interval_ * 24 * 3600);
        struct tm lt;
        localtime_r(&next, &lt);
        lt.tm_hour = SYSPARAM_NAMESERVER.report_block_time_lower_;
        lt.tm_min  = time_seed % ((SAFE_MODE_TIME / 60) - 1);
        lt.tm_sec  = time_seed % 60;
        next = mktime(&lt);
      }
      else
      {
        next += (time_seed % SAFE_MODE_TIME);
      }
      time_t diff_sec = next - current;
      next_report_block_time_ = now + diff_sec;
      TBSYS_LOG(INFO, "%s next: %"PRI64_PREFIX"d, diff: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d",
        tbsys::CNetUtil::addrToString(id()).c_str(), next_report_block_time_, diff_sec, now);
    }

    int ServerCollect::choose_move_block_random(uint64_t& result) const
    {
      result = INVALID_BLOCK_ID;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      int32_t ret = !hold_->empty() ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = random() % hold_->size();
        result = hold_->at(index);
      }
      return INVALID_BLOCK_ID == result ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
    }

    int ServerCollect::expand_ratio(const float expand_ratio)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      if (hold_->need_expand(expand_ratio))
        hold_->expand_ratio(expand_ratio);
      if (writable_->need_expand(expand_ratio))
        writable_->expand_ratio(expand_ratio);
      TBSYS_LOG(DEBUG, "%s expand, hold: %d, writable: %d, master: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), hold_->size(), writable_->size(), issued_leases_->size());
      return TFS_SUCCESS;
    }

    void ServerCollect::copy_block(ServerCollect* server)
    {
      bool all_over = false;
      uint64_t start = 0;
      const int32_t MAX_QUERY_NUM = 2048;
      uint64_t blocks[MAX_QUERY_NUM];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
      while (!all_over)
      {
        helper.clear();
        all_over = server->get_range_blocks(start, *server->hold_, helper);
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          uint64_t result;
          start = *helper.at(index);
          hold_->insert_unique(result, start);
        }
      }
    }

    int ServerCollect::clear_(const int32_t args, LayoutManager& manager)
    {
      bool all_over = false;
      BlockCollect*  pblock  = NULL;
      ServerCollect* pserver = NULL;
      const int32_t MAX_QUERY_NUM = 2048;
      uint64_t start = 0, now = Func::get_monotonic_time();
      uint64_t blocks[MAX_QUERY_NUM];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
      BlockManager& block_manager = manager.get_block_manager();
      ServerManager& server_manager = manager.get_server_manager();
      FamilyManager& family_manager = manager.get_family_manager();
      while (!all_over)
      {
        helper.clear();
        all_over = get_range_blocks(start, *hold_, helper);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          start = *helper.at(index);
          now   = Func::get_monotonic_time();
          pblock = block_manager.get(start);
          if ((SERVICE_STATUS_OFFLINE == get_status())
              && (NULL != pblock))
          {
            pserver = server_manager.get(id());
            if (pblock->exist(id())
                && (NULL == pserver))
            {
              block_manager.relieve_relation(pblock, id(), now);
            }
            if ((CALL_BACK_FLAG_PUSH & args)
               && (NULL == pserver))
            {
              if (block_manager.need_replicate(pblock))
              {
                block_manager.push_to_emergency_replicate_queue(pblock);
              }
              else if (block_manager.need_reinstate(pblock))
              {
                FamilyCollect* family = family_manager.get(pblock->get_family_id());
                if (NULL != family)
                {
                  family_manager.push_to_reinstate_or_dissolve_queue(family, PLAN_TYPE_EC_REINSTATE);
                }
              }
            }
          }
        }
      }

      start = 0, all_over = false;
      while (!all_over)
      {
        helper.clear();
        all_over = get_range_blocks(start, *issued_leases_, helper);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          start = *helper.at(index);
          now = Func::get_monotonic_time();
          block_manager.giveup_lease(id(), now, NULL, start);
        }
      }

      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      if (CALL_BACK_FLAG_CLEAR & args)
        hold_->clear();
      writable_->clear();
      issued_leases_->clear();
      return TFS_SUCCESS;
    }

    bool ServerCollect::get_range_blocks_(const uint64_t begin, const BLOCK_TABLE& table, ArrayHelper<uint64_t>& blocks) const
    {
      return for_each_(begin, table, blocks);
    }

    int ServerCollect::for_each_(const uint64_t begin, const BLOCK_TABLE& table, ArrayHelper<uint64_t>& blocks) const
    {
      BLOCK_TABLE_ITER iter = 0 == begin ? table.begin() : table.upper_bound(begin);
      for (; iter != table.end() && blocks.get_array_index() < blocks.get_array_size(); ++iter)
      {
        blocks.push_back((*iter));
      }
      return blocks.get_array_index() < blocks.get_array_size();
    }

    bool ServerCollect::exist_(const uint64_t block, const BLOCK_TABLE& table) const
    {
      BLOCK_TABLE_ITER iter = table.find(block);
      return table.end() != iter;
    }

    int ServerCollect::remove_(const uint64_t block, BLOCK_TABLE& table)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_BLOCK_ID_INVALID_ERROR;
      if (TFS_SUCCESS == ret)
      {
        table.erase(block);
      }
      return ret;
    }

    bool ServerCollect::cleanup_invalid_block_(BlockCollect* block, const int64_t now, const bool master, const bool writable)
    {
      bool ret = (NULL != block);
      if (ret)
      {
        ret = false;
        if (master)
        {
          if (!block->is_writable()
              || !block->is_master(id())
              || !is_equal_group(block->id())
              || !block->has_valid_lease(now)
              || IS_VERFIFY_BLOCK(block->id()))
          {
            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            remove_(block->id(), *issued_leases_);
          }
        }

        if (writable)
        {
          if (block->is_full()
              || !block->is_master(id())
              || !is_equal_group(block->id())
              || IS_VERFIFY_BLOCK(block->id()))
          {
            ret = true;
            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            remove_(block->id(), *writable_);
          }
        }
      }
      return ret;
    }

    void ServerCollect::invalid_block_copies_(LayoutManager& manager, const common::ArrayHelper<ServerItem>& helper,
    const common::ArrayHelper<ServerItem>& clean_family_helper, const uint64_t block)
    {
      for (int64_t index = 0; index < helper.get_array_index(); ++index)
      {
        ServerItem* item = helper.at(index);
        assert(NULL != item);
        ServerCollect* pserver = manager.get_server_manager().get(item->server_);
        manager.get_server_manager().relieve_relation(pserver, block);
        manager.get_block_manager().push_to_delete_queue(block, *item, GFactory::get_runtime_info().is_master());
      }

      for (int64_t k = 0; k < clean_family_helper.get_array_index(); ++k)
      {
        ServerItem* item = clean_family_helper.at(k);
        assert(NULL != item);
        manager.get_block_manager().push_to_clean_familyinfo_queue(block, *item, GFactory::get_runtime_info().is_master());
      }
    }

    void ServerCollect::write_block_oplog_(LayoutManager& manager, const int32_t cmd, const common::BlockInfoV2& info, const int64_t now)
    {
      uint64_t servers[1];
      ArrayHelper<uint64_t> helper(1, servers);
      helper.push_back(id());
      manager.block_oplog_write_helper(cmd, info, helper, now);
    }
  } /** nameserver **/
}/** tfs **/
