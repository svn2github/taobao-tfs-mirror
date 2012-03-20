/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_chunk.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   duanfei 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "global_factory.h"
#include "block_collect.h"
#include "gc.h"
#include "ns_define.h"

using namespace tfs::nameserver;
using namespace tfs::common;

class GCTest : public virtual ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    SYSPARAM_NAMESERVER.object_dead_max_time_ = 0xffff;
    TBSYS_LOGGER.setLogLevel("debug");
    GFactory::initialize();
  }
  static void TearDownTestCase()
  {
    GFactory::destroy();
    GFactory::wait_for_shut_down();
  }
  GCTest(){}
  ~GCTest(){}
};

TEST_F(GCTest, gcobject)
{
  time_t now = time(NULL);
  GCObject object(now);
  EXPECT_EQ(false, object.is_dead(now));
  now += SYSPARAM_NAMESERVER.object_dead_max_time_ ;
  EXPECT_EQ(true, object.is_dead(now));
  now += 0xff;
  EXPECT_EQ(true, object.is_dead(now));
}

TEST_F(GCTest, gc_run)
{
  GCObjectManager& instance = GFactory::get_gc_manager();
  EXPECT_EQ(0U, instance.object_list_.size());

  EXPECT_TRUE(TFS_ERROR == instance.add(NULL));

  time_t now = time(NULL);
  GCObject* obj = new GCObject(now);
  EXPECT_TRUE(TFS_SUCCESS == instance.add(obj));
  EXPECT_EQ(1U, instance.object_list_.size());
  EXPECT_TRUE(TFS_SUCCESS == instance.add(obj));
  EXPECT_EQ(1U, instance.object_list_.size());

  const int32_t MAX_COUNT = 0x0f;
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    BlockCollect* block = new BlockCollect(i, time(NULL));
    block->set_dead_time(0);
    instance.add(block);
  }

  uint32_t size = 1 + MAX_COUNT;
  EXPECT_EQ(size, instance.object_list_.size());

  instance.destroy_ = true;
  instance.run();
  EXPECT_EQ(size, instance.object_list_.size());

  instance.destroy_ = false;
  instance.run();
  EXPECT_EQ(1U, instance.object_list_.size());

  BlockCollect* block = new BlockCollect(0xff, time(NULL));
  EXPECT_TRUE(TFS_SUCCESS == instance.add(block));
  EXPECT_TRUE(TFS_SUCCESS == instance.add(block));
  EXPECT_EQ(2U, instance.object_list_.size());
  EXPECT_EQ(TFS_SUCCESS, instance.wait_for_shut_down());
  EXPECT_EQ(0U, instance.object_list_.size());
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
