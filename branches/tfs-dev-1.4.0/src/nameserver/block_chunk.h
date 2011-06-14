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

#ifndef TFS_NAMESERVER_BLOCKCHUNK_H_
#define TFS_NAMESERVER_BLOCKCHUNK_H_

#include <stdint.h>
#include <Shared.h>
#include <Handle.h>
#include "ns_define.h"
#include "common/lock.h"
#include "common/internal.h"

namespace tfs
{
  namespace nameserver
  {
    class BlockChunk : public virtual common::RWLock,
    public virtual tbutil::Shared
    {
      friend class LayoutManager;
      public:
      BlockChunk();
      virtual ~BlockChunk();
      BlockCollect* add(const uint32_t block_id, const time_t now);
      static bool connect(BlockCollect* block, ServerCollect* server, const time_t now, const bool force, bool& writable);

      bool remove(const uint32_t block_id);

      BlockCollect* find(const uint32_t block_id);

      bool exist(const uint32_t block_id) const;

      uint32_t calc_max_block_id() const;
      int64_t  calc_all_block_bytes() const;
      uint32_t calc_size() const;
      int scan(common::SSMScanParameter& param, int32_t& actual, bool& end, int32_t should, bool cutover_chunk);

#if defined(TFS_NS_GTEST) || defined(TFS_NS_INTEGRATION)
      public:
#else
      private:
#endif
      BLOCK_MAP block_map_;
    };
    typedef tbutil::Handle<BlockChunk> BlockChunkPtr;
  }/** nameserver **/
}/** tfs **/

#endif /* BLOCKCHUNK_H_ */
