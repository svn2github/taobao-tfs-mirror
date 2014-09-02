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
#ifndef TFS_DATASERVER_SUPER_BLOCK_MANAGER_H_
#define TFS_DATASERVER_SUPER_BLOCK_MANAGER_H_

#include "common/lock.h"
#include "common/internal.h"
#include "common/mmap_file_op.h"
#include "ds_define.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    // super block file implementation, inner format:
    // ------------------------------------------------------------
    // | reserve         | SuperBlock| {BlockIndex|...|BlockIndex}|
    // ------------------------------------------------------------
    class SuperBlockManager
    {
      #ifdef TFS_GTEST
      friend class TestSuperBlockManager;
      FRIEND_TEST(TestSuperBlockManager, format);
      #endif
      public:
        explicit SuperBlockManager(const std::string& path);
        virtual ~SuperBlockManager();
        int format(SuperBlockInfo& info);
        int load();
        int get_super_block_info(SuperBlockInfo*& info) const;
        int update_super_block_info(const SuperBlockInfo& info);
        int cleanup_block_index(const int32_t physical_block_id);
        int get_block_index(BlockIndex& index, const int32_t physical_block_id) const;
        int update_block_index(const BlockIndex& index, const int32_t physical_block_id);
        int get_legal_physical_block_id(int32_t& physical_block_id, const bool extend = false) const;
        int dump(tbnet::DataBuffer& buf) const;
        int dump(const int32_t level, const char* file, const int32_t line,
            const char* function, const char* format, ...);
        int flush();
      public:
        static const int32_t MAX_BLOCK_INDEX_SIZE;
      private:
        DISALLOW_COPY_AND_ASSIGN(SuperBlockManager);
        static const int32_t SUPERBLOCK_RESERVER_LENGTH;
        static const int32_t MAX_INITIALIZE_BLOCK_INDEX_SIZE;
        common::MMapFileOperation file_op_;
        mutable int32_t index_;
        mutable int32_t ext_index_;
        mutable common::RWLock mutex_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* TFS_DATASERVER_SUPER_BLOCK_MANAGER_H_ */
