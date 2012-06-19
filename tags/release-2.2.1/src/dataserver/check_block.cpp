/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: check_block.cpp 476 2012-04-12 14:43:42Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#include <Time.h>
#include <Memory.hpp>
#include "message/replicate_block_message.h"
#include "message/write_data_message.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/status_message.h"
#include "blockfile_manager.h"
#include "check_block.h"


namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace tbutil;

    void CheckBlock::add_check_task(const uint32_t block_id)
    {
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.find(block_id);
      if (iter == changed_block_map_.end())
      {
        uint32_t mtime = time(NULL);
        changed_block_map_.insert(std::make_pair(block_id, mtime));
        TBSYS_LOG(INFO, "add check task %u, %u", block_id, mtime);
      }
      else
      {
        iter->second = time(NULL);
        TBSYS_LOG(INFO, "update check task %u, %u", iter->first, iter->second);
      }
      changed_block_mutex_.unlock();
    }

    void CheckBlock::remove_check_task(const uint32_t block_id)
    {
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.find(block_id);
      if (iter != changed_block_map_.end())
      {
        changed_block_map_.erase(iter);
      }
      changed_block_mutex_.unlock();
    }

    int CheckBlock::check_all_blocks(common::CheckBlockInfoVec& check_result,
        const int32_t check_flag, const uint32_t check_time, const uint32_t last_check_time)
    {
      TBSYS_LOG(DEBUG, "check all blocks: check_flag: %d", check_flag);
      TIMER_START();

      // get the list need to check, then free lock
      VUINT should_check_blocks;
      changed_block_mutex_.lock();
      ChangedBlockMapIter iter = changed_block_map_.begin();
      for ( ; iter != changed_block_map_.end(); iter++)
      {
        // check if modify time between check range
        if (iter->second < check_time &&
            iter->second >= last_check_time)
        {
          should_check_blocks.push_back(iter->first);
        }
      }
      changed_block_mutex_.unlock();

      // do real check, call check_one_block
      int ret = 0;
      CheckBlockInfo result;
      VUINT::iterator it = should_check_blocks.begin();
      for ( ; it != should_check_blocks.end(); it++)
      {
         ret = check_one_block(*it, result, check_flag);
         if (TFS_SUCCESS == ret)
         {
           check_result.push_back(result);
         }
      }
      TIMER_END();
      TBSYS_LOG(INFO, "check all blocks. count: %u, cost: %u\n",
          should_check_blocks.size(), TIMER_DURATION());

      return TFS_SUCCESS;
    }

    int CheckBlock::check_one_block(const uint32_t& block_id,
          common::CheckBlockInfo& result, const int32_t check_flag)
    {
      TBSYS_LOG(DEBUG, "check one, block_id: %u, check_flag: %d", block_id, check_flag);
      int ret = TFS_SUCCESS;
      UNUSED(check_flag);  // not used now
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)  // already deleted block, remove it
      {
        TBSYS_LOG(WARN, "blockid: %u is not exist.", block_id);
        remove_check_task(block_id);
        ret = EXIT_NO_LOGICBLOCK_ERROR;
      }
      else
      {
        logic_block->rlock();   // lock block
        common::BlockInfo bi;
        ret = logic_block->get_block_info(&bi);
        logic_block->unlock();

        if (TFS_SUCCESS == ret)
        {
          result.block_id_ = bi.block_id_;    // block id
          result.version_ = bi.version_;     // version
          result.file_count_ = bi.file_count_ - bi.del_file_count_; // file count
          result.total_size_ = bi.size_ - bi.del_size_;   // file size
        }

        TBSYS_LOG(INFO, "blockid: %u, file count: %u, total size: %u",
              result.block_id_, result.file_count_, result.total_size_);
      }
      return ret;
    }
  }
}
