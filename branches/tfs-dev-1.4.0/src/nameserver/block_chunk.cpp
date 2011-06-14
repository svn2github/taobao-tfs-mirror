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

#include <Memory.hpp>
#include "block_chunk.h"
#include "block_collect.h"
#include "global_factory.h"
using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    BlockChunk::BlockChunk()
    {

    }

    BlockChunk::~BlockChunk()
    {
      //RWLock::Lock lock(*this, WRITE_LOCKER);
      BLOCK_MAP::iterator iter = block_map_.begin();
      for (; iter != block_map_.end(); ++iter)
      {
        tbsys::gDelete(iter->second);
      }
      block_map_.clear();
    }
    /**
     * create new BlockCollect object 
     * @param[in] block_id : block id
     * @return NULL if failed 
     */
    BlockCollect* BlockChunk::add(const uint32_t block_id, const time_t now)
    {
      BlockCollect* block = new BlockCollect(block_id, now);
      std::pair<BLOCK_MAP_ITER, bool> res = 
        block_map_.insert(BLOCK_MAP::value_type(block_id, block));
      if (!res.second)
      {
        tbsys::gDelete(block);
        block = NULL;
      }
      return block;
    }

    /**
     * add new BlockCollect
     * @param[in] block_id: block id
     * @return NULL if add failed or block exist
     */
    bool BlockChunk::connect(BlockCollect* block, ServerCollect* server, 
        const time_t now, const bool force, bool& writable)
    {
      bool bret = (block != NULL && server != NULL);
      if (bret)
      {
        bret = block->add(server, now, force, writable);
      }
      return bret;
    }

    bool BlockChunk::remove(const uint32_t block_id)
    {
      BLOCK_MAP::iterator iter = block_map_.find(block_id);
      if (iter != block_map_.end())
      {
        iter->second->set_dead_time();
        GFactory::get_gc_manager().add(iter->second);
        block_map_.erase(iter);
      }
      return true;
    }

    BlockCollect* BlockChunk::find(const uint32_t block_id)
    {
      BLOCK_MAP::iterator iter = block_map_.find(block_id);
      return iter == block_map_.end() ? NULL : iter->second;
    }

    bool BlockChunk::exist(const uint32_t block_id) const
    {
      BLOCK_MAP::const_iterator iter = block_map_.find(block_id);
      return (iter != block_map_.end());
    }

    uint32_t BlockChunk::calc_max_block_id() const
    {
      uint32_t block_id = 0;
      BLOCK_MAP::const_iterator iter = block_map_.begin();
      for (; iter != block_map_.end(); ++iter)
      {
        if (block_id < iter->first && iter->first < UINT_MAX)
        {
          block_id = iter->first;
        }
      }
      return block_id;
    }

    int64_t BlockChunk::calc_all_block_bytes() const
    {
      int64_t total_bytes = 0;
      BLOCK_MAP::const_iterator iter = block_map_.begin();
      for (; iter != block_map_.end(); ++iter)
      {
        total_bytes += iter->second->size();
      }
      return total_bytes;
    }

    uint32_t BlockChunk::calc_size() const
    {
      return block_map_.size();
    }

    int BlockChunk::scan(SSMScanParameter& param, int32_t& actual,
        bool& end, const int32_t should, const bool cutover_chunk)
    {
      //RWLock::Lock lock(*this,  READ_LOCKER);
      int32_t jump_count = 0;
      BLOCK_MAP::iterator iter; 
      if (cutover_chunk)
      {
        iter = block_map_.begin();
        if (iter != block_map_.end())
        {
          param.addition_param1_ = iter->second->id();
        }
      }
      else
      {
        iter = block_map_.find(param.addition_param1_);
      }

      TBSYS_LOG(DEBUG, "block_map_size: : %u", block_map_.size());
      bool has_block = iter != block_map_.end();
      if (has_block)
      {
        for (; iter != block_map_.end() && actual < should ; ++iter, ++jump_count)
        {
          if (iter->second->scan(param) == TFS_SUCCESS)
          {
            TBSYS_LOG(DEBUG, "BlockChunk scan block: %u", iter->second->id());
            ++actual;
          }
        }
        end = iter == block_map_.end();
        if (!end)
        {
          param.addition_param2_ = iter->second->id();
        }
      }
      return jump_count;
    }
  } /** nameserver **/
} /** tfs **/
