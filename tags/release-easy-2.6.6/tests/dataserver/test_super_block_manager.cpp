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
#include "super_block_manager.h"
#include "common/error_msg.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    class TestSuperBlockManager: public ::testing::Test
    {
      public:
        TestSuperBlockManager()
        {
        }
        virtual ~TestSuperBlockManager()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(TestSuperBlockManager, format)
    {
      SuperBlockInfo info;
      const std::string path("./super_block");
      SuperBlockManager manager(path);
      memset(&info, 0, sizeof(info));
      info.max_block_index_element_count_ = 1024;
      info.total_main_block_count_        = 512;
      EXPECT_EQ(TFS_SUCCESS, manager.format(info));

      const uint64_t logic_block_id = 0xffffff;
      int32_t physical_block_id = 0;
      EXPECT_EQ(TFS_SUCCESS, manager.get_legal_physical_block_id(physical_block_id));

      BlockIndex index, new_index;
      index.logic_block_id_ = logic_block_id;
      index.physical_block_id_ = physical_block_id;
      index.physical_file_name_id_ = physical_block_id;
      index.next_index_ = 0;
      index.prev_index_ = 0;
      index.index_  = 0;
      index.status_ = 0;
      index.split_flag_ = 0;
      index.split_status_ = 0;
      EXPECT_EQ(TFS_SUCCESS, manager.update_block_index(index,physical_block_id));
      EXPECT_EQ(TFS_SUCCESS, manager.get_block_index(new_index, physical_block_id));
      EXPECT_TRUE(index.logic_block_id_== new_index.logic_block_id_);
      EXPECT_TRUE(index.physical_block_id_== new_index.physical_block_id_);

      SuperBlockInfo* pinfo = NULL;
      EXPECT_EQ(TFS_SUCCESS, manager.get_super_block_info(pinfo));
      EXPECT_TRUE(NULL != pinfo);
      EXPECT_EQ(info.max_block_index_element_count_, pinfo->max_block_index_element_count_);
      EXPECT_EQ(info.total_main_block_count_, pinfo->total_main_block_count_);

      memset(&new_index, 0, sizeof(new_index));
      EXPECT_EQ(TFS_SUCCESS, manager.cleanup_block_index(physical_block_id));
      EXPECT_EQ(TFS_SUCCESS, manager.get_block_index(new_index, physical_block_id));
      EXPECT_TRUE(index.logic_block_id_!= new_index.logic_block_id_);
      EXPECT_TRUE(index.physical_block_id_!= new_index.physical_block_id_);
      ::unlink(path.c_str());
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
