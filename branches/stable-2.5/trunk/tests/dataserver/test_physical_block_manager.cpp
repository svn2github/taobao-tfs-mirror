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
 *   duafnei@taobao.com
 *      - initial release
 *
 */
#include <string>
#include <Memory.hpp>
#include <gtest/gtest.h>
#include "ds_define.h"
#include "common/error_msg.h"
#include "block_manager.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    class TestPhysicalBlockManager: public ::testing::Test
    {
      public:
        TestPhysicalBlockManager():
          block_manager_("./super_block")
        {

        }

        virtual ~TestPhysicalBlockManager()
        {

        }

        virtual void SetUp()
        {
          SuperBlockInfo info;
          memset(&info, 0, sizeof(info));
          info.max_block_index_element_count_ = 1024;
          info.total_main_block_count_        = 512;
          info.max_main_block_size_ = 72 * 1024 * 1024;
          info.max_extend_block_size_ = 4 * 1024 * 1024;
          sprintf(info.mount_point_, "%s", ".");
          EXPECT_EQ(TFS_SUCCESS, block_manager_.get_super_block_manager().format(info));
        }
        virtual void TearDown()
        {
          ::unlink("./super_block");
        }
      protected:
        BlockManager block_manager_;
    };

    TEST_F(TestPhysicalBlockManager, insert_remove)
    {
      const uint64_t logic_block_id  = 0xffff;
      const int32_t physical_block_id = 0xf;
      const int32_t MAX_BLOCK_SIZE    = 72 * 1024 * 1024;
      int32_t start = BLOCK_RESERVER_LENGTH;
      int32_t end   = MAX_BLOCK_SIZE;
      const std::string path("./test_physical_block_manager.data");
      BlockIndex index;
      index.logic_block_id_ = logic_block_id;
      index.physical_block_id_ = physical_block_id;
      index.physical_file_name_id_ = physical_block_id;
      index.next_index_ = 0;
      index.prev_index_ = 0;
      index.index_  = 0;
      index.status_ = BLOCK_CREATE_COMPLETE_STATUS_COMPLETE;
      index.split_flag_ = BLOCK_SPLIT_FLAG_NO;
      index.split_status_ = BLOCK_SPLIT_STATUS_UNCOMPLETE;

      SuperBlockManager& super_block_manager = block_manager_.get_super_block_manager();
      int32_t ret = super_block_manager.update_block_index(index, physical_block_id);
      EXPECT_EQ(TFS_SUCCESS, ret);
      if (TFS_SUCCESS == ret)
      {
        PhysicalBlockManager& physical_block_manager = block_manager_.get_physical_block_manager();
        FileOperation file_op(path, O_RDWR | O_CREAT);
        int32_t fd = file_op.open();
        EXPECT_TRUE(fd >= 0);
        if (fd >= 0)
        {
          char data[1024];
          memset(data, 0, 1024);
          file_op.write(data, 1024);
          file_op.close();
          EXPECT_EQ(TFS_SUCCESS, physical_block_manager.insert_(index, physical_block_id, path, start, end));
          EXPECT_EQ(1, physical_block_manager.physical_blocks_.size());
          EXPECT_EQ(0, physical_block_manager.alloc_physical_blocks_.size());
          EXPECT_TRUE(physical_block_manager.exist(physical_block_id));

          BasePhysicalBlock* gcobject = NULL;
          EXPECT_EQ(TFS_SUCCESS, physical_block_manager.remove(gcobject, physical_block_id));
          EXPECT_FALSE(physical_block_manager.exist(physical_block_id));
          EXPECT_EQ(0, physical_block_manager.physical_blocks_.size());
          EXPECT_EQ(0, physical_block_manager.alloc_physical_blocks_.size());
          EXPECT_TRUE(NULL != gcobject);
          BlockIndex tmp_index;
          memset(&tmp_index, 0, sizeof(tmp_index));
          ret = super_block_manager.get_block_index(tmp_index, physical_block_id);
          EXPECT_EQ(TFS_SUCCESS , ret);
          EXPECT_EQ(0U, tmp_index.logic_block_id_);
          EXPECT_EQ(0, tmp_index.physical_block_id_);
          tbsys::gDelete(gcobject);

          index.split_flag_ = BLOCK_SPLIT_FLAG_YES;
          ret = super_block_manager.update_block_index(index, physical_block_id);
          EXPECT_EQ(TFS_SUCCESS , ret);
          if (TFS_SUCCESS == ret)
          {
            gcobject = NULL;
            EXPECT_EQ(TFS_SUCCESS, physical_block_manager.insert_(index, physical_block_id, path, start, end));
            EXPECT_EQ(1, physical_block_manager.physical_blocks_.size());
            EXPECT_EQ(1, physical_block_manager.alloc_physical_blocks_.size());
            EXPECT_TRUE(physical_block_manager.exist(physical_block_id));
            EXPECT_EQ(TFS_SUCCESS, physical_block_manager.remove(gcobject, physical_block_id));
            EXPECT_FALSE(physical_block_manager.exist(physical_block_id));
            EXPECT_EQ(0, physical_block_manager.physical_blocks_.size());
            EXPECT_EQ(0, physical_block_manager.alloc_physical_blocks_.size());
            EXPECT_TRUE(NULL == gcobject);
          }
        }
        file_op.unlink();
      }
    }

    TEST_F(TestPhysicalBlockManager, alloc_block)
    {
      const uint64_t logic_block_id  = 0xffff;
      const int32_t MAX_ALLOC_BLOCK_SIZE = 128;
      for (int32_t i = 0; i < MAX_ALLOC_BLOCK_SIZE; ++i)
      {
        BlockIndex index;
        memset(&index, 0, sizeof(index));
        index.logic_block_id_ =  logic_block_id + i;
        PhysicalBlockManager& physical_block_manager = block_manager_.get_physical_block_manager();
        EXPECT_EQ(TFS_SUCCESS, physical_block_manager.alloc_block(index,  BLOCK_SPLIT_FLAG_NO));
        EXPECT_EQ(i + 1, index.physical_block_id_);
        EXPECT_EQ(logic_block_id + i , index.logic_block_id_);
        EXPECT_EQ(BLOCK_SPLIT_FLAG_NO, index.split_flag_);
        EXPECT_EQ((i + 1), physical_block_manager.physical_blocks_.size());
      }
    }

    TEST_F(TestPhysicalBlockManager, alloc_free_ext_block)
    {
      const uint64_t logic_block_id  = 0xffff;
      const int32_t BASE_PHYSICAL_BLOCK_ID = 0xf;
      const std::string path("./1");
      FileOperation file_op(path, O_RDWR | O_CREAT);
      int32_t fd = file_op.open();
      EXPECT_TRUE(fd >= 0);
      if (fd >= 0)
      {
        char data[1024];
        memset(data, 0, 1024);
        file_op.write(data, 1024);
        file_op.close();
        BlockIndex index, ext_index;
        PhysicalBlockManager& physical_block_manager = block_manager_.get_physical_block_manager();
        for (int32_t i = 1; i <= AllocPhysicalBlock::ALLOC_BIT_MAP_SIZE; ++i)
        {
          memset(&index, 0, sizeof(index));
          memset(&ext_index, 0, sizeof(ext_index));
          index.logic_block_id_ = logic_block_id;
          index.physical_block_id_ = BASE_PHYSICAL_BLOCK_ID;
          EXPECT_EQ(TFS_SUCCESS, physical_block_manager.alloc_ext_block(index, ext_index, false));
          EXPECT_EQ(i, ext_index.index_);
          EXPECT_EQ(1, ext_index.physical_file_name_id_);
        }
        EXPECT_EQ(EXIT_PHYSICAL_BLOCK_NOT_FOUND, physical_block_manager.alloc_ext_block(index, ext_index, false));
      }

      ::unlink(path.c_str());
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
