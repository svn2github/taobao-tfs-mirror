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
      rb_status_(REPORT_BLOCK_STATUS_REPORTING)
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
            && (is_equal_group(block)))
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
        ret = (!isfull && is_equal_group(block)) ? TFS_SUCCESS : EXIT_BLOCK_FULL;
        if (TFS_SUCCESS == ret)
        {
          RWLock::Lock lock(mutex_, WRITE_LOCKER);
          uint64_t result = INVALID_SERVER_ID;
          ret = writable_->insert_unique(result, block);
        }
      }
      return ret;
    }

    bool ServerCollect::calc_regular_create_block_count(const double average_used_capacity,LayoutManager& manager, bool& promote, int32_t& count)
    {
      bool ret = ((is_report_block_complete() || promote) /*&& !is_full()*/
                    && count > 0 && total_capacity_ > 0 && use_capacity_ >= 0);
      if (!ret)
      {
        count = 0;
      }
      else
      {
        mutex_.rdlock();
        int32_t current = writable_->size();
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
            scan_writable_block_id_ = *helper.at(index);
            assert(INVALID_BLOCK_ID != scan_writable_block_id_);
            block = manager.get_block_manager().get(scan_writable_block_id_);
            assert(NULL != block);
            if (cleanup_invalid_block_(block))
              continue;
            RWLock::Lock lock(mutex_, READ_LOCKER);
            if (!exist_(scan_writable_block_id_, *issued_leases_))
            {
              ++actual;//TODO
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
      clear_(*flag, manager);
    }

    int ServerCollect::apply_block_for_update(LayoutManager& manager, ArrayHelper<BlockLease>& result)
    {
      int64_t now = Func::get_monotonic_time();
      BlockManager& block_manager = manager.get_block_manager();
      for (int64_t index = 0; index < result.get_array_index(); ++index)
      {
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
          ret = (exist_(entry->block_id_, *issued_leases_) && block_manager.has_valid_lease(pblock, now)) ? EXIT_LEASE_EXISTED : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)
        {
            ret = pblock->check_copies_complete() && is_equal_group(entry->block_id_) && pblock->is_master(id()) ? TFS_SUCCESS : EXIT_BLOCK_NOT_IN_CURRENT_GROUP;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.apply_lease(id(), now, get() - now, true, pblock);
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
        }
        else
        {
          TBSYS_LOG(WARN, "%s apply update block %"PRI64_PREFIX"u ret %d",
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
        for (int64_t index = 0; index < helper.get_array_index() && result.get_array_index() < result.get_array_size(); ++index)
        {
          start = *helper.at(index);
          BlockCollect* pblock = NULL;
          ret = (exist_(start, *issued_leases_)) ? EXIT_LEASE_EXISTED : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            pblock = block_manager.get(start);
            ret = (NULL != pblock) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
          }
          if (TFS_SUCCESS == ret)
          {
            ret = pblock->is_writable() && pblock->is_master(id()) ? TFS_SUCCESS : EXIT_BLOCK_NO_WRITABLE;
            if (TFS_SUCCESS != ret)
            {
              if (pblock->has_version_conflict() || pblock->is_full() || !is_equal_group(start) || !pblock->is_master(id()))
              {
                RWLock::Lock lock(mutex_, WRITE_LOCKER);
                remove_(start, *writable_);
              }
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = block_manager.has_valid_lease(pblock, now) ? EXIT_LEASE_EXISTED : TFS_SUCCESS;
          }
          if (TFS_SUCCESS == ret)
          {
            ret = block_manager.apply_lease(id(), now, get() - now, false, pblock);
          }
          if (TFS_SUCCESS == ret)
          {
            uint64_t rt = INVALID_BLOCK_ID;
            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            ret = issued_leases_->insert_unique(rt , start);
            if (TFS_SUCCESS != ret && EXIT_ELEMENT_EXIST != ret)
            {
              block_manager.giveup_lease(start, id(), now, NULL);
            }
          }
          if (TFS_SUCCESS == ret || EXIT_ELEMENT_EXIST == ret)
          {
            result.push_back(BlockLease());
            BlockLease* entry = result.at(result.get_array_index() - 1);
            assert(NULL != entry);
            entry->block_id_ = start;
            entry->result_   = TFS_SUCCESS;
            ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, entry->servers_);
            block_manager.get_servers(servers, pblock);
            entry->size_ = servers.get_array_index();
          }
        }
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::renew_block(const ArrayHelper<BlockInfoV2>& input,LayoutManager& manager, ArrayHelper<BlockLease>& output)
    {
      BlockManager& block_manager = manager.get_block_manager();
      for (int64_t index = 0; index < input.get_array_index(); ++index)
      {
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
          uint64_t server = pblock->get_server(0);
          ret = (id() == server) ? TFS_SUCCESS : EXIT_CANNOT_RENEW_LEASE;
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.renew_lease(id(), now, get() - now, false, *entry, pblock);
        }
        if (TFS_SUCCESS == ret)
        {
          ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, result->servers_);
          block_manager.get_servers(servers, pblock);
          result->size_ = servers.get_array_index();
        }
        else
        {
          cleanup_invalid_block_(pblock);
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
          uint64_t server = pblock->get_server(0);
          ret = (id() == server) ? TFS_SUCCESS : EXIT_CANNOT_GIVEUP_LEASE;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "%s cannot giveup lease, %s has lease, real %s ,block: %"PRI64_PREFIX"u",
                CNetUtil::addrToString(id()).c_str(), CNetUtil::addrToString(id()).c_str(),
                CNetUtil::addrToString(server).c_str(), entry->block_id_);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager.giveup_lease(entry->block_id_, id(), now, entry);
        }
        if (TFS_SUCCESS == ret)
        {
          cleanup_invalid_block_(pblock);
        }
      }
      return TFS_SUCCESS;
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
      next_report_block_time_ = 0xFFFFFFFF;
      scan_writable_block_id_ = 0;
      rb_status_ = REPORT_BLOCK_STATUS_REPORTING;
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      total_network_bandwith_ = info.total_network_bandwith_;
      current_load_ = info.current_load_;
      block_count_ = info.block_count_;
      startup_time_ = info.startup_time_;
      disk_type_   = info.type_;
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
        else if (left_element < right_element->block_id_)
        {
          ++left_index;
          left.push_back(left_element);
        }
        else if (right_element->block_id_ < left_element)
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

    void ServerCollect::set_next_report_block_time(const time_t now, const int64_t time_seed, const bool ns_switch)
    {
      int32_t hour = SYSPARAM_NAMESERVER.report_block_time_upper_ - SYSPARAM_NAMESERVER.report_block_time_lower_ ;
      time_t current = time(NULL);
      time_t next    = current;
      if (ns_switch)
      {
        next += (time_seed % (hour * 3600));
      }
      else
      {
        if (SYSPARAM_NAMESERVER.report_block_time_interval_ > 0)
        {
          next += (SYSPARAM_NAMESERVER.report_block_time_interval_ * 24 * 3600);
          struct tm lt;
          localtime_r(&next, &lt);
          if (hour > 0)
            lt.tm_hour = time_seed % hour + SYSPARAM_NAMESERVER.report_block_time_lower_;
          else
            lt.tm_hour = SYSPARAM_NAMESERVER.report_block_time_lower_;
          lt.tm_min  = time_seed % 60;
          lt.tm_sec =  time_seed % 60;
          next = mktime(&lt);
        }
        else
        {
          next += (SYSPARAM_NAMESERVER.report_block_time_interval_min_ * 60);
        }
      }
      time_t diff_sec = next - current;
      next_report_block_time_ = now + diff_sec;
      TBSYS_LOG(DEBUG, "%s next: %"PRI64_PREFIX"d, diff: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d, hour: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), next_report_block_time_, diff_sec, now, hour);
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
      TBSYS_LOG(DEBUG, "%s expand, hold: %d, writable: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), hold_->size(), writable_->size());
      return TFS_SUCCESS;
    }

    int ServerCollect::clear_(const int32_t args, LayoutManager& manager)
    {
      bool all_over = false;
      const int32_t MAX_QUERY_NUM = 2048;
      uint64_t start = 0, now = Func::get_monotonic_time();
      uint64_t blocks[MAX_QUERY_NUM];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
      BlockManager& block_manager = manager.get_block_manager();
      ServerManager& server_manager = manager.get_server_manager();
      while (!all_over)
      {
        helper.clear();
        all_over = get_range_blocks(start, *hold_, helper);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          start = *helper.at(index);
          now   = Func::get_monotonic_time();
          BlockCollect* pblock = block_manager.get(start);
          if (SERVICE_STATUS_OFFLINE == get_status() && NULL != pblock)
          {
            ServerCollect* pserver = server_manager.get(id());
            if (pblock->exist(id()) && (NULL == pserver))
            {
              block_manager.relieve_relation(pblock, start, now);
            }
            if ((CALL_BACK_FLAG_PUSH & args)
               && (NULL == pserver)
               && (block_manager.need_replicate(pblock)))
            {
              block_manager.push_to_emergency_replicate_queue(pblock);
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
          block_manager.giveup_lease(start, id(), now, NULL);
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

    bool ServerCollect::cleanup_invalid_block_(BlockCollect* block)
    {
      bool ret = false;
      if (NULL != block)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        if (!block->is_writable() || !block->is_master(id()))
        {
          ret = true;
          remove_(block->id(), *issued_leases_);
        }
        if (block->is_full() || !block->is_master(id()) || block->has_version_conflict())
        {
          ret = true;
          remove_(block->id(), *writable_);
        }
      }
      return ret;
    }
  } /** nameserver **/
}/** tfs **/
