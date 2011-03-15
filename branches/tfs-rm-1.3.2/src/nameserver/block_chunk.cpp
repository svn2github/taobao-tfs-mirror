/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#include <Lock.h>
#include <Memory.hpp>
#include "block_chunk.h"

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
      remove_all();
    }

    /**
     * @param block_collect : BlockCollect object
     * @param overwrite : if true then replace BlockCollect object already exist.
     * @return true if success
     */
    bool BlockChunk::insert(const BlockCollect* block_collect, const bool overwrite)
    {
      uint32_t block_id = (block_collect->get_block_info()->block_id_);
      if (block_id == 0)
        return false;
      BLOCK_MAP::iterator it = block_map_.find(block_id);
      if (it != block_map_.end())
      {
        if (overwrite)
        {
          tbsys::gDelete(it->second);
          block_map_.erase(it);
        }
        else
        {
          return false;
        }
      }

      block_map_.insert(BLOCK_MAP::value_type(block_id, const_cast<BlockCollect*> (block_collect))).second;
      return true;
    }

    bool BlockChunk::exist(const uint32_t block_id) const
    {
      BLOCK_MAP::const_iterator it = block_map_.find(block_id);
      return it != block_map_.end();
    }

    BlockCollect* BlockChunk::find(const uint32_t block_id) const
    {
      BLOCK_MAP::const_iterator it = block_map_.find(block_id);
      if (it == block_map_.end())
      {
        return NULL;
      }
      return it->second;
    }

    BlockCollect *BlockChunk::create(const uint32_t block_id)
    {
      BlockCollect *block_collect = new BlockCollect();
      BlockInfo* blk = const_cast<BlockInfo*> (block_collect->get_block_info());
      memset(blk, 0, sizeof(BlockInfo));
      blk->block_id_ = block_id;
      blk->version_ = 0;

      block_map_.insert(BLOCK_MAP::value_type(blk->block_id_, block_collect));
      return block_collect;
    }

    bool BlockChunk::remove(const uint32_t block_id)
    {
      BLOCK_MAP_ITER it = block_map_.find(block_id);
      if (it == block_map_.end())
        return false;
      BlockCollect* block_collect = it->second;
      // now remove from hashtable.
      block_map_.erase(it);
      // this object is allocated by create(block_id),
      // so remove must delete it.
      tbsys::gDelete(block_collect);
      return true;
    }

    bool BlockChunk::connect(const uint32_t block_id, const uint64_t server_id, const bool master /*= false*/)
    {
      TBSYS_LOG(DEBUG, "master(%s)", master ? "true" : "false");
      BLOCK_MAP_ITER it = block_map_.find(block_id);
      if (it == block_map_.end())
      {
        TBSYS_LOG(DEBUG, "block(%u) not found in blockchunk", block_id);
        return false;
      }
      bool before = false;
      if (master)
        before = true;

      if (!it->second->join(server_id, before))
      {
        TBSYS_LOG(DEBUG, "block(%u) join failed", block_id);
        return false;
      }
      TBSYS_LOG(DEBUG, "ds size(%u)", it->second->get_ds()->size());
      if (master)
        it->second->set_master_ds(server_id);
      return true;
    }

    bool BlockChunk::release(const uint32_t block_id, const uint64_t server_id)
    {
      BLOCK_MAP_ITER it = block_map_.find(block_id);
      if (it == block_map_.end())
        return false;
      if (it->second->get_master_ds() == server_id)
        it->second->set_master_ds(0);
      return it->second->leave(server_id);
    }

    void BlockChunk::remove_all()
    {
      for (BLOCK_MAP_ITER it = block_map_.begin(); it != block_map_.end(); it++)
      {
        tbsys::gDelete(it->second);
      }
      block_map_.clear();
    }

    uint32_t BlockChunk::calc_max_block_id() const
    {
      uint32_t block_id = 0;
      for (BLOCK_MAP::const_iterator bit = block_map_.begin(); bit != block_map_.end(); bit++)
      {
        if (block_id < bit->first && bit->first < 100000000)
        {
          block_id = bit->first;
        }
      }
      return block_id;
    }

    int64_t BlockChunk::calc_all_block_bytes() const
    {
      int64_t total_bytes = 0;
      for (BLOCK_MAP::const_iterator it = block_map_.begin(); it != block_map_.end(); ++it)
      {
        total_bytes += it->second->get_block_info()->size_;
      }
      return total_bytes;
    }
  }
}

