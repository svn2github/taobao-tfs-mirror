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
#ifndef TFS_NAMESERVER_BLOCK_CHUNK_H_
#define TFS_NAMESERVER_BLOCK_CHUNK_H_ 

#include <Shared.h>
#include <Handle.h>
#include "common/lock.h"
#include "common/define.h"
#include "block_collect.h"

namespace tfs
{
  namespace nameserver
  {
    class BlockChunk: public tbutil::Shared
    {
    public:
      BlockChunk();
      virtual ~BlockChunk();

      BlockCollect* find(const uint32_t block_id) const;
      BlockCollect* create(const uint32_t block_id);

      bool exist(const uint32_t block_id) const;
      bool remove(const uint32_t block_id);
      bool connect(const uint32_t block_id, const uint64_t server_id, const bool master = false);
      bool release(const uint32_t block_id, const uint64_t server_id);
      bool insert(const BlockCollect* blkcol, const bool overwrite);

      uint32_t calc_max_block_id() const;
      int64_t calc_all_block_bytes() const;

      inline const common::BLOCK_MAP & get_block_map() const
      {
        return block_map_;
      }

    private:
      DISALLOW_COPY_AND_ASSIGN( BlockChunk);
      void remove_all();
      common::BLOCK_MAP block_map_;

    public:
      common::RWLock mutex_;
    };
    typedef tbutil::Handle<BlockChunk> BlockChunkPtr;
  }
}

#endif
