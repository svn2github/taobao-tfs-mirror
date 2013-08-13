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
#include "array_helper.h"
#include "dataservice.h"
#include "writable_block.h"

using namespace tbutil;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace dataserver
  {
    class TestWritableBlock: public ::testing::Test
    {
      public:
        TestWritableBlock()
        {
        }
        ~TestWritableBlock()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
    };

    TEST_F(TestWritableBlock, general)
    {
      // construct
      WritableBlock block(100);
      ASSERT_TRUE(!block.get_use_flag());

      // normal set
      uint64_t servers[] = {1001, 1002, 1003};
      ArrayHelper<uint64_t> helper(3, servers, 3);
      ASSERT_EQ(TFS_SUCCESS, block.set_servers(helper));

      // normal get
      VUINT64 servers2;
      block.get_servers(servers2);
      ASSERT_EQ(3, (int)servers2.size());
      ASSERT_EQ(servers[0], servers2[0]);
      ASSERT_EQ(servers[1], servers2[1]);
      ASSERT_EQ(servers[2], servers2[2]);

      // wrong set
      uint64_t servers3[256] = {1000};
      ArrayHelper<uint64_t> wrong(256, servers3, 256);
      ASSERT_NE(TFS_SUCCESS, block.set_servers(wrong));

      // normal get
      uint64_t servers4[16];
      ArrayHelper<uint64_t> normal(16, servers4, 16);
      ASSERT_EQ(TFS_SUCCESS, block.get_servers(normal));
      ASSERT_EQ(3, (int)normal.get_array_index());
      ASSERT_EQ(servers[0], servers4[0]);
      ASSERT_EQ(servers[1], servers4[1]);
      ASSERT_EQ(servers[2], servers4[2]);

      // wrong get
      ArrayHelper<uint64_t> unnormal(2, servers4, 2);
      ASSERT_NE(TFS_SUCCESS, block.get_servers(unnormal));

    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

