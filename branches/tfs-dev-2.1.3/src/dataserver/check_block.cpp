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
 *   duolong <duolong@taobao.com>
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
        ChangedBlock* changed_block = new(std::nothrow) ChangedBlock();
        if (NULL == changed_block) // just ignore this change
        {
          TBSYS_LOG(WARN, "alloc memory fail.");
        }
        else
        {
          changed_block->block_id_ = block_id;
          changed_block->mod_time_ = time(NULL);
          changed_block_map_.insert(std::make_pair(block_id, changed_block));
        }
      }
      else
      {
        iter->second->mod_time_ = time(NULL);
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
      TBSYS_LOG(DEBUG, "[CHECK_ALL_BLOCKS]");
      VUINT should_check_blocks;
      ChangedBlockMapIter iter = changed_block_map_.begin();

      // get the list need to check, then free lock
      changed_block_mutex_.lock();
      for ( ; iter != changed_block_map_.end(); iter++)
      {

        if (iter->second->mod_time_ < check_time &&
            iter->second->mod_time_ >= last_check_time)
        {
          should_check_blocks.push_back(iter->second->block_id_);
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

      // TODO: recheck block

      return TFS_SUCCESS;
    }

    int CheckBlock::check_one_block(const uint32_t& block_id,
          common::CheckBlockInfo& result, const int32_t check_flag)
    {
      TBSYS_LOG(DEBUG, "CHECK_ONE_BLOCK block_id: %u, check_flag: %d", block_id, check_flag);
      int ret = TFS_SUCCESS;
      LogicBlock* logic_block = BlockFileManager::get_instance()->get_logic_block(block_id);
      if (NULL == logic_block)
      {
        TBSYS_LOG(ERROR, "blockid: %u is not exist.", block_id);
        ret =  EXIT_NO_LOGICBLOCK_ERROR;
      }
      else
      {
        TIMER_START();
        common::BlockInfo *bi;
        common::RawMetaVec meta_infos;  // meta info vector
        common::RawMetaVecIter meta_it;
        logic_block->rlock();
        bi = logic_block->get_block_info();
        logic_block->get_meta_infos(meta_infos);
        logic_block->unlock();
        result.block_id_ = block_id;       // block id
        result.version_ = bi->version_;     // version
        result.file_count_ = 0;
        result.total_size_ = 0;
        FileInfo fi;
        for (meta_it = meta_infos.begin(); meta_it != meta_infos.end(); meta_it++)
        {
          if (0 != check_flag) // check in detail
          {
            int32_t size = sizeof(FileInfo);
            if (TFS_SUCCESS == logic_block->read_raw_data((char*)&fi, size, meta_it->get_offset()))
            {
              // not a deleted file
              if (0 == (fi.flag_ & FI_DELETED))
              {
                result.file_count_++;
                result.total_size_ += meta_it->get_size();
              }
            }
          }
          else // just check index
          {
            result.file_count_++;
            result.total_size_ += meta_it->get_size();
          }
        }
        TIMER_END();

        TBSYS_LOG(DEBUG, "blockid: %u, file count in block: %u, total file size: %u, cost: %"PRI64_PREFIX"d",
            result.block_id_, result.file_count_, result.total_size_, TIMER_DURATION());
      }
      return ret;
    }
  }
}
