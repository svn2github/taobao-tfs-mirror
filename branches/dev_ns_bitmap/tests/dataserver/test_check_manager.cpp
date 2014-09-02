/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   linqing.zyd
 *      - initial release
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>

#include "tbsys.h"
#include "internal.h"
#include "check_manager.h"
#include "dataservice.h"

using namespace tbutil;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace dataserver
  {
    class TestCheckManager: public ::testing::Test
    {
      public:
        TestCheckManager()
        {
        }
        ~TestCheckManager()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
    };

    TEST_F(TestCheckManager, test_compare_block_fileinfos)
    {
      DataService service;
      CheckManager check_manager(service);

      const int32_t left_size = 20;
      const int32_t right_size = 15;
      uint64_t left_ids[left_size] = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
      uint64_t right_ids[right_size] = {30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2};
      std::vector<FileInfoV2> left;
      std::vector<FileInfoV2> right;
      std::vector<FileInfoV2> more;
      std::vector<FileInfoV2> less;
      std::vector<FileInfoV2> diff;

      for (int index = 0; index < left_size; index++)
      {
        FileInfoV2 temp;
        temp.id_ = left_ids[index];
        temp.status_ = 0;
        left.push_back(temp);
      }
      for (int index = 0; index < right_size; index++)
      {
        FileInfoV2 temp;
        temp.id_ = right_ids[index];
        temp.status_ = 0;
        right.push_back(temp);
      }
      check_manager.compare_block_fileinfos(left, right, more, diff, less);
      EXPECT_EQ(static_cast<int>(more.size()), 10);
      EXPECT_EQ(static_cast<int>(less.size()), 5);
    }

  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

