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

#ifndef TFS_DATASERVER_PHYSICAL_BLOCK_H_
#define TFS_DATASERVER_PHYSICAL_BLOCK_H_
// -------------------------------------------------------------------------------------
// | reserve |   file data(current stored data)                                        |
// -------------------------------------------------------------------------------------
// | reserve | FileInfoInDiskExt+data|FileInfoInDiskExt+data|...|FileInfoInDiskExt+data|
// -------------------------------------------------------------------------------------
#include "common/internal.h"
#include "common/file_opv2.h"
#include "ds_define.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif
namespace tfs
{
  namespace dataserver
  {
    //class PhysicalBlockManager;
    class BasePhysicalBlock : public GCObject
    {
      public:
        BasePhysicalBlock(const int32_t physical_block_id, const int32_t start, const int32_t end);
        virtual ~BasePhysicalBlock();
        explicit BasePhysicalBlock(const int32_t physical_block_id);//for query
        inline  int32_t id() const { return physical_block_id_;}
        inline  int32_t length() const { return end_ - start_;}
      protected:
        DISALLOW_COPY_AND_ASSIGN(BasePhysicalBlock);
        static const int32_t MAX_WARN_CONSUME_TIME_US;
        int32_t physical_block_id_;
        int32_t start_;//the data start offset of this block file
        int32_t end_;//the data end offset of this block file
    };

    class AllocPhysicalBlock : public BasePhysicalBlock
    {
      #ifdef TFS_GTEST
      friend class TestPhysicalBlock;
      FRIEND_TEST(TestPhysicalBlock, alloc_free);
      #endif
      friend class PhysicalBlockManager;
      public:
        AllocPhysicalBlock(const int32_t physical_block_id, const std::string& path,
          const int32_t start, const int32_t end);
        virtual ~AllocPhysicalBlock();
        int alloc(int8_t& index, int32_t& start, int32_t& end,
              const int32_t max_main_block_size, const int32_t max_ext_block_size);
        int free(const int8_t index, const int32_t max_main_block_size, const int32_t max_ext_block_size);
        bool empty(const int32_t max_main_block_size, const int32_t max_ext_block_size) const;
        bool full(const int32_t max_main_block_size, const int32_t max_ext_block_size) const;

        static const int32_t STORE_ALLOC_BIT_MAP_SIZE;
        static const int32_t ALLOC_BIT_MAP_SIZE;
      private:
        int load_alloc_bit_map_();//只能被PhysicalBlockManager->insert调用

      private:
        uint64_t alloc_bit_map_;//已分配扩展块位图
        common::FileOperation file_op_;//file operation handle
    };

    class PhysicalBlock : public BasePhysicalBlock
    {
      public:
        PhysicalBlock(const int32_t physical_block_id, const std::string& path,
            const int32_t start, const int32_t end);
        virtual ~PhysicalBlock();
        int pwrite(const char* buf, const int32_t nbytes, const int32_t offset);
        int pread(char* buf, const int32_t nbytes, const int32_t offset);
      private:
        DISALLOW_COPY_AND_ASSIGN(PhysicalBlock);
        common::FileOperation file_op_;//file operation handle
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif /* TFS_DATASERVER_PHYSICAL_BLOCK_H_ */
