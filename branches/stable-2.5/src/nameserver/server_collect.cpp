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
    const int8_t ServerCollect::MULTIPLE = 2;
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

    ServerCollect::ServerCollect(LayoutManager& manager, const uint64_t id):
      GCObject(0),
      id_(id),
      hold_(NULL),
      writable_(NULL),
      hold_master_(NULL),
      manager_(manager)
    {
      //for query
    }

    ServerCollect::ServerCollect(LayoutManager& manager, const DataServerStatInfo& info, const time_t now):
      GCObject((now + common::SYSPARAM_NAMESERVER.heart_interval_ * MULTIPLE)),
      id_(info.id_),
      use_capacity_(info.use_capacity_),
      total_capacity_(info.total_capacity_),
      startup_time_(now),
      rb_expired_time_(0xFFFFFFFF),
      next_report_block_time_(0xFFFFFFFF),
      in_dead_queue_timeout_(0xFFFFFFFF),
      scan_writable_block_id_(0),
      current_load_(info.current_load_ <= 0 ? 1 : info.current_load_),
      block_count_(info.block_count_),
      total_network_bandwith_(128),//MB
      write_index_(0),
      status_(common::DATASERVER_STATUS_ALIVE),
      rb_status_(REPORT_BLOCK_STATUS_NONE),
      manager_(manager)
    {
        int32_t block_nums = 0;
        int32_t min_expand_size = 0;
        float   expand_ratio = 0.0;
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
        hold_master_ = new (std::nothrow)TfsVector<uint64_t>(MAX_WRITE_FILE_COUNT, 16, 0.1);
        assert(NULL != hold_);
        assert(NULL != writable_);
        assert(NULL != hold_master_);
    }

    ServerCollect::~ServerCollect()
    {
      tbsys::gDelete(hold_);
      tbsys::gDelete(writable_);
      tbsys::gDelete(hold_master_);
    }

    int ServerCollect::add(const uint64_t block, const bool master, const bool writable)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        RWLock::Lock lock(mutex_, WRITE_LOCKER);
        uint64_t result;
        ret = hold_->insert_unique(result, block);
        //TBSYS_LOG(DEBUG, "id: %"PRI64_PREFIX"u, ret: %d master: %d, writable: %d, %d", block->id(), ret, master, writable, is_equal_group(block->id()));
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
      return (INVALID_BLOCK_ID != block) ? remove_(block) : EXIT_PARAMETER_ERROR;
    }

    int ServerCollect::remove_(const uint64_t block)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        hold_->erase(block);
        writable_->erase(block);
        hold_master_->erase(block);
      }
      return ret;
    }

    int ServerCollect::remove(const common::ArrayHelper<uint64_t>& blocks)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      for (int64_t index = 0; index < blocks.get_array_index(); ++index)
      {
        uint64_t block = *blocks.at(index);
        int32_t ret = remove_(block);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "remove block : %"PRI64_PREFIX"u, form %s error, ret: %d", block,
            tbsys::CNetUtil::addrToString(id()).c_str(),ret);
        }
      }
      return TFS_SUCCESS;
    }

    int ServerCollect::add_writable(const uint64_t block, const bool isfull)
    {
      int32_t ret = (INVALID_BLOCK_ID != block) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = isfull ? EXIT_BLOCK_FULL : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (is_equal_group(block))
          {
            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            uint64_t result = INVALID_SERVER_ID;
            ret = writable_->insert_unique(result, block);
          }
        }
      }
      return ret;
    }

    bool ServerCollect::clear(LayoutManager& manager, const time_t now)
    {
      uint64_t start = 0;
      const int32_t MAX_QUERY_NUM = 2048;
      uint64_t blocks[MAX_QUERY_NUM];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
      bool complete = false;
      do
      {
        helper.clear();
        complete = get_range_blocks(helper, start, MAX_QUERY_NUM);
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          start = *helper.at(index);
          manager.get_block_manager().relieve_relation(start, id(), now);
        }
      }
      while (!complete);

      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      hold_->clear();
      writable_->clear();
      hold_master_->clear();
      return true;
    }

    int ServerCollect::choose_writable_block(BlockCollect*& result)
    {
      result = NULL;
      int32_t ret = TFS_SUCCESS;
      int64_t count = hold_master_->size();
      uint64_t block = INVALID_BLOCK_ID;
      BlockCollect* blocks[MAX_WRITE_FILE_COUNT];
      ArrayHelper<BlockCollect*> helper(MAX_WRITE_FILE_COUNT, blocks);
      for (int64_t index = 0; index < count && NULL == result; ++index)
      {
        ret = choose_writable_block_(block);
        if (TFS_SUCCESS == ret)
        {
          result = manager_.get_block_manager().get(block);
          ret = (NULL != result) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            ret = (result->is_writable() && is_equal_group(result->id())) ? TFS_SUCCESS : EXIT_BLOCK_NO_WRITABLE;
          }
          if (TFS_SUCCESS != ret && NULL != result)
          {
            helper.push_back(result);
            result = NULL;
          }
        }
      }
      remove_writable_(helper);
      return NULL == result ? EXIT_BLOCK_NOT_FOUND : TFS_SUCCESS;
    }

    int ServerCollect::choose_writable_block_(uint64_t& result) const
    {
      result = INVALID_BLOCK_ID;
      RWLock::Lock lock(mutex_, READ_LOCKER);
      int32_t ret = !hold_master_->empty() ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
      if (TFS_SUCCESS == ret)
      {
        int32_t index = write_index_;
        if (write_index_ >= hold_master_->size())
          write_index_ = 0;
        if (index >= hold_master_->size())
          index = 0;
        ++write_index_;
        result = hold_master_->at(index);
      }
      return (INVALID_BLOCK_ID != result) ? TFS_SUCCESS : EXIT_BLOCK_NOT_FOUND;
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
        param.data_.writeInt64(last_update_time_);
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
        param.data_.writeInt32(hold_master_->size());
        for (BLOCK_TABLE_ITER iter = hold_master_->begin(); iter != hold_master_->end(); iter++)
        {
          param.data_.writeInt64((*iter));
        }
      }
      return TFS_SUCCESS;
    }

    bool ServerCollect::touch(bool& promote, int32_t& count, const double average_used_capacity)
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
        int32_t current = hold_master_->size();
        int32_t writable_size = writable_->size();
        mutex_.unlock();
        TBSYS_LOG(DEBUG, "%s touch current %d, writable size: %d", CNetUtil::addrToString(id()).c_str(),current, writable_size);
        if (current >= SYSPARAM_NAMESERVER.max_write_file_count_)
          count = 0;

        if (count > 0)
        {
          count = std::min(count, (SYSPARAM_NAMESERVER.max_write_file_count_ - current));
          if (count > 0)
          {
            int32_t actual = 0;
            int64_t index  = 0;
            const int32_t MAX_QUERY_NUM = 128;
            uint64_t blocks[MAX_QUERY_NUM];
            uint64_t invalid_blocks[MAX_QUERY_NUM];
            ArrayHelper<uint64_t> helper(MAX_QUERY_NUM, blocks);
            ArrayHelper<uint64_t> invalid(MAX_QUERY_NUM,invalid_blocks);
            mutex_.rdlock();
            bool complete = get_range_writable_blocks_(helper, scan_writable_block_id_, MAX_QUERY_NUM);
            mutex_.unlock();

            BlockCollect* block = NULL;
            double use_capacity_ratio = static_cast<double>(use_capacity_) / static_cast<double>(total_capacity_);
            double max_average_use_capacity_ratio = average_used_capacity * AVERAGE_USED_CAPACITY_MULTIPLE;

            for (index = 0; index < helper.get_array_index() && actual < count; ++index)
            {
              scan_writable_block_id_ = *helper.at(index);
              if (INVALID_BLOCK_ID == scan_writable_block_id_)
                continue;
              block = manager_.get_block_manager().get(scan_writable_block_id_);
              if (NULL == block)
              {
                invalid.push_back(scan_writable_block_id_);
                continue;
              }
              if (!is_equal_group(scan_writable_block_id_))
              {
                invalid.push_back(scan_writable_block_id_);
                continue;
              }
              if (!block->is_master(this->id()))
                continue;
              if (!block->is_writable())
              {
                if (block->is_full())
                  invalid.push_back(scan_writable_block_id_);
                continue;
              }
              RWLock::Lock lock(mutex_, WRITE_LOCKER);
              if (exist_in_writable_(scan_writable_block_id_)
                  && !exist_in_master_(scan_writable_block_id_))
              {
                ++actual;
                hold_master_->push_back(scan_writable_block_id_);
              }
            }
            if (complete)
              scan_writable_block_id_ = 0;

            RWLock::Lock lock(mutex_, WRITE_LOCKER);
            uint64_t id = INVALID_BLOCK_ID;
            for (index = 0; index < invalid.get_array_index(); ++index)
            {
              id = *invalid.at(index);
              writable_->erase(id);
            }

            if (!is_full() && ((use_capacity_ratio < max_average_use_capacity_ratio)
                              || max_average_use_capacity_ratio <= PERCENTAGE_MIN))
              count -= actual;
            else
              count = 0;
          }
        }
      }
      return ret;
    }

    bool ServerCollect::get_range_blocks_(common::ArrayHelper<uint64_t>& blocks, const uint64_t begin, const int32_t count) const
    {
      blocks.clear();
      int32_t actual = 0;
      BLOCK_TABLE_ITER iter = 0 == begin ? hold_->begin() : hold_->upper_bound(begin);
      while(iter != hold_->end() && actual < count)
      {
        blocks.push_back((*iter));
        actual++;
        iter++;
      }
      return actual < count;
    }

    bool ServerCollect::get_range_blocks(common::ArrayHelper<uint64_t>& blocks, const uint64_t begin, const int32_t count) const
    {
      RWLock::Lock lock(mutex_, READ_LOCKER);
      return get_range_blocks_(blocks, begin, count);
    }

    bool ServerCollect::get_range_writable_blocks_(common::ArrayHelper<uint64_t>& blocks, uint64_t& begin, const int32_t count) const
    {
      blocks.clear();
      bool complete = true;
      int32_t ret = (blocks.get_array_size() >= count) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BLOCK_TABLE_ITER iter = 0 == begin ? writable_->begin() : writable_->upper_bound(begin);
        while (iter != writable_->end() && blocks.get_array_index() < count)
        {
          blocks.push_back((*iter));
          ++iter;
        }
        complete = blocks.get_array_index() < count;
      }
      return complete;
    }

    bool ServerCollect::exist_in_hold_(const uint64_t block) const
    {
      BLOCK_TABLE_ITER iter = hold_->find(block);
      return (hold_->end() != iter);
    }

    bool ServerCollect::exist_in_writable_(const uint64_t block) const
    {
      BLOCK_TABLE_ITER iter = writable_->find(block);
      return (writable_->end() != iter);
    }

    bool ServerCollect::exist_in_master_(const uint64_t block) const
    {
      TfsVector<uint64_t>::iterator iter = hold_master_->find(block);
      return (hold_master_->end() != iter);
    }

    void ServerCollect::statistics(NsGlobalStatisticsInfo& stat, const bool is_new) const
    {
      if (is_new)
      {
        stat.use_capacity_ += use_capacity_;
        stat.total_capacity_ += total_capacity_;
        stat.total_load_ += current_load_;
        stat.total_block_count_ += block_count_;
        stat.alive_server_count_ += 1;
      }
      stat.max_load_ = std::max(current_load_, stat.max_load_);
      stat.max_block_count_ = std::max(block_count_, stat.max_block_count_);
    }

    void ServerCollect::update(const common::DataServerStatInfo& info, const time_t now, const bool is_new)
    {
      use_capacity_ = info.use_capacity_;
      total_capacity_ = info.total_capacity_;
      total_network_bandwith_ = info.total_network_bandwith_;
      current_load_ = info.current_load_;
      block_count_ = info.block_count_;
      update_last_time(!is_new ? now : (now + common::SYSPARAM_NAMESERVER.heart_interval_ * MULTIPLE));
      startup_time_ = is_new ? now : info.startup_time_;
      status_ = info.status_;
      for (int8_t index = 0; index < MAX_RW_STAT_PAIR_NUM; ++index)
      {
        write_bytes_[index] = info.write_bytes_[index];
        read_bytes_[index] = info.read_bytes_[index];
        write_file_count_[index] = info.write_file_count_[index];
        read_file_count_[index] = info.read_file_count_[index];
        unlink_file_count_[index] = info.unlink_file_count_[index];
      }
    }

    void ServerCollect::reset(LayoutManager& manager, const common::DataServerStatInfo& info, const time_t now)
    {
      clear(manager, now);
      update(info, now, true);
      rb_expired_time_ = 0xFFFFFFFF;
      next_report_block_time_ = 0xFFFFFFFF;
      in_dead_queue_timeout_  = 0xFFFFFFFF;
      write_index_ = 0;
      scan_writable_block_id_ = 0;
      status_ = common::DATASERVER_STATUS_ALIVE;
      rb_status_ = REPORT_BLOCK_STATUS_NONE;
    }

    void ServerCollect::callback(LayoutManager& manager)
    {
      UNUSED(manager);
    }

    void ServerCollect::set_next_report_block_time(const time_t now, const int64_t time_seed, const bool ns_switch)
    {
      int32_t hour = common::SYSPARAM_NAMESERVER.report_block_time_upper_ - common::SYSPARAM_NAMESERVER.report_block_time_lower_ ;
      time_t current = time(NULL);
      time_t next    = current;
      if (ns_switch)
      {
        next += (time_seed % (hour * 3600));
      }
      else
      {
        if (common::SYSPARAM_NAMESERVER.report_block_time_interval_ > 0)
        {
          next += (common::SYSPARAM_NAMESERVER.report_block_time_interval_ * 24 * 3600);
          struct tm lt;
          localtime_r(&next, &lt);
          if (hour > 0)
            lt.tm_hour = time_seed % hour + common::SYSPARAM_NAMESERVER.report_block_time_lower_;
          else
            lt.tm_hour = common::SYSPARAM_NAMESERVER.report_block_time_lower_;
          lt.tm_min  = time_seed % 60;
          lt.tm_sec =  time_seed % 60;
          next = mktime(&lt);
        }
        else
        {
          next += (common::SYSPARAM_NAMESERVER.report_block_time_interval_min_ * 60);
        }
      }
      time_t diff_sec = next - current;
      next_report_block_time_ = now + diff_sec;
      TBSYS_LOG(DEBUG, "%s next: %"PRI64_PREFIX"d, diff: %"PRI64_PREFIX"d, now: %"PRI64_PREFIX"d, hour: %d",
        tbsys::CNetUtil::addrToString(id()).c_str(), next_report_block_time_, diff_sec, now, hour);
    }

    bool ServerCollect::remove_writable_(const common::ArrayHelper<BlockCollect*>& blocks)
    {
      RWLock::Lock lock(mutex_, WRITE_LOCKER);
      BlockCollect* block = NULL;
      for (int64_t i = 0; i < blocks.get_array_index(); ++i)
      {
        block = *blocks.at(i);
        TBSYS_LOG(DEBUG, "%s remove_writable: is_full: %d, servers size: %d",
          tbsys::CNetUtil::addrToString(id()).c_str(), block->is_full(), block->get_servers_size());
        if (!block->is_writable() || !is_equal_group(block->id()))
           hold_master_->erase(block->id());
        if (block->is_full() || !is_equal_group(block->id()))
           writable_->erase(block->id());
      }
      return true;
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
  } /** nameserver **/
}/** tfs **/
