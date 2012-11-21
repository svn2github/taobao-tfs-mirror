/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
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

#ifndef TFS_DATASERVER_PHYSICAL_BLOCK_MANAGER_H_
#define TFS_DATASERVER_PHYSICAL_BLOCK_MANAGER_H_

#include <string>
#include "common/lock.h"
#include "common/tfs_vector.h"
#include "physical_blockv2.h"
#include "super_block_manager.h"

namespace tfs
{
  namespace dataserver
  {
    class  BlockManager;
    struct PhysicalBlockIdCompare
    {
      bool operator ()(const BasePhysicalBlock* lhs, const BasePhysicalBlock* rhs) const
      {
        return lhs->id() < rhs->id();
      }
    };
    class PhysicalBlockManager
    {
      typedef common::TfsSortedVector<BasePhysicalBlock*,PhysicalBlockIdCompare> PHYSICAL_BLOCK_MAP;
      typedef PHYSICAL_BLOCK_MAP::iterator PHYSICAL_BLOCK_MAP_ITER;
      public:
      explicit PhysicalBlockManager(BlockManager& manager);
      virtual ~PhysicalBlockManager();
      int insert(const BlockIndex& index, const int32_t physical_block_id, const std::string& path,
          const int32_t start, const int32_t end);
      int remove(BasePhysicalBlock*& physical_block, const int32_t physcical_block_id);
      bool exist(const int32_t physical_block_id) const;
      int alloc_block(BlockIndex& index, const int8_t split_flag = BLOCK_SPLIT_FLAG_NO);
      int alloc_ext_block(BlockIndex& index, BlockIndex& ext_index);
      int free_ext_block(const BlockIndex& index, bool flush = true);
      BasePhysicalBlock* get(const int32_t physical_block_id) const;

      inline BlockManager& get_block_manager() { return manager_;}

      private:
      BasePhysicalBlock* get_(const int32_t physical_block_id) const;
      bool exist_(const int32_t physcical_block_id) const;
      int insert_alloc_physical_blocks_(BasePhysicalBlock* physical_block);
      int remove_alloc_physical_blocks_(BasePhysicalBlock* physical_block);
      int insert_(const BlockIndex& index, const int32_t physical_block_id, const std::string& path,
          const int32_t start, const int32_t end);
      private:
      BlockManager& manager_;
      PHYSICAL_BLOCK_MAP physical_blocks_;
      PHYSICAL_BLOCK_MAP alloc_physical_blocks_;
      mutable common::RWLock rwmutex_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif /* PHYSICAL_BLOCK_MANAGER_H_ */
