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
#include <gtest/gtest.h>
#include "ds_define.h"
#include "physical_blockv2.h"
#include "common/error_msg.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    class TestPhysicalBlock: public ::testing::Test
    {
      public:
        TestPhysicalBlock()
        {
        }
        virtual ~TestPhysicalBlock()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(TestPhysicalBlock, alloc_free)
    {
      const int32_t MAX_MAIN_BLOCK_SIZE = 72 * 1024 * 1024;
      const int32_t MAX_EXT_BLOCK_SIZE = 4 * 1024 * 1024;
      const int32_t physical_block_id = 0xff;
      const int32_t start = BLOCK_RESERVER_LENGTH;
      const int32_t end   = 72 * 1024 * 1024;
      std::string path("./physical_block.data");
      FileOperation file_op(path, O_RDWR | O_CREAT);
      int32_t fd = file_op.open();
      EXPECT_TRUE(fd >= 0);
      if (fd >= 0)
      {
        char data[1024];
        memset(data, 0, 1024);
        file_op.write(data, 1024);
        file_op.close();
        srandom(time(NULL));
        AllocPhysicalBlock physical_block(physical_block_id, path, start, end);;
        EXPECT_EQ(TFS_SUCCESS, physical_block.load_alloc_bit_map_());
        EXPECT_EQ(0U, physical_block.alloc_bit_map_);
        EXPECT_TRUE(physical_block.empty(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        int8_t index = 0;
        int32_t ext_start = 0, ext_end = 0, COUNT = MAX_MAIN_BLOCK_SIZE / MAX_EXT_BLOCK_SIZE;

        for (int8_t i = 0; i < COUNT; ++i)
        {
          index = 0;
          ext_start = 0, ext_end = 0;
          EXPECT_FALSE(physical_block.full(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
          EXPECT_EQ(TFS_SUCCESS, physical_block.alloc(index, ext_start, ext_end, MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
          EXPECT_FALSE(physical_block.empty(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
          EXPECT_EQ(i + 1, index);
        }

        index = 10;
        EXPECT_EQ(TFS_SUCCESS, physical_block.free(index, MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        EXPECT_FALSE(physical_block.full(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        ext_start = 0, ext_end = 0;
        int8_t tmp_index = 0;
        EXPECT_EQ(TFS_SUCCESS, physical_block.alloc(tmp_index, ext_start, ext_end, MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        EXPECT_EQ(index, tmp_index);

        EXPECT_TRUE(physical_block.full(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        for (int8_t j = 1; j <=  COUNT; ++j)
        {
          EXPECT_EQ(TFS_SUCCESS, physical_block.free(j, MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        }
        EXPECT_FALSE(physical_block.full(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        EXPECT_TRUE(physical_block.empty(MAX_MAIN_BLOCK_SIZE, MAX_EXT_BLOCK_SIZE));
        EXPECT_EQ(0U, physical_block.alloc_bit_map_);
      }
      file_op.unlink();
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
