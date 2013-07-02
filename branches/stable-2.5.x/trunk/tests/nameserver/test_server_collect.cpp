/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_server_collect.cpp 5 2011-03-26 08:55:56Z
 *
 * Authors:
 *  duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "server_collect.h"
#include "block_collect.h"
#include "nameserver.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {

    class ServerCollectTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        ServerCollectTest():
          layout_manager_(ns_),
          block_manager_(layout_manager_)
        {
          SYSPARAM_NAMESERVER.max_replication_ = 2;
          SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
          SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
          SYSPARAM_NAMESERVER.max_block_size_ = 100;
          SYSPARAM_NAMESERVER.max_write_file_count_= 16;
          SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
          SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
          SYSPARAM_NAMESERVER.group_count_ = 2;
          SYSPARAM_NAMESERVER.group_seq_ = 0;
          SYSPARAM_NAMESERVER.add_primary_block_count_ = 2;
          SYSPARAM_NAMESERVER.heart_interval_ = 1000;
        }

        ~ServerCollectTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
        protected:
        NameServer ns_;
        LayoutManager layout_manager_;
        BlockManager block_manager_;
    };

    TEST_F(ServerCollectTest, add)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;

      ServerCollect server(layout_manager_, info, now);

      EXPECT_EQ(EXIT_PARAMETER_ERROR, server.add(INVALID_BLOCK_ID, writable, master));
      EXPECT_EQ(0, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(0, server.hold_->size());

      EXPECT_EQ(TFS_SUCCESS, server.add(block.id(), writable, master));
      EXPECT_EQ(0, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(1, server.hold_->size());

      master = true;
      writable = true;
      server.hold_->clear();
      EXPECT_EQ(TFS_SUCCESS, server.add(block.id(), writable, master));
      EXPECT_EQ(1, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(1, server.hold_->size());

      master = true;
      writable = true;
      server.hold_->clear();
      server.writable_->clear();
      BlockCollect block1(101, now);
      EXPECT_EQ(TFS_SUCCESS, server.add(block.id(), writable, master));
      EXPECT_EQ(1, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(1, server.hold_->size());
    }

    TEST_F(ServerCollectTest, remove)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = true;
      bool writable = true;
      ServerCollect server(layout_manager_, info, now);
      EXPECT_EQ(TFS_SUCCESS, server.add(block.id(), writable, master));
      EXPECT_EQ(1, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(1, server.hold_->size());
      EXPECT_EQ(TFS_SUCCESS, server.remove(block.id()));
      EXPECT_EQ(0, server.writable_->size());
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(0, server.hold_->size());
    }

    TEST_F(ServerCollectTest, get_range_blocks_)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      bool master   = true;
      bool writable = true;
      ServerCollect server(layout_manager_, info, now);
      const int32_t BLOCK_COUNT = random() % 10000 + 40960;
      BlockCollect* blocks[BLOCK_COUNT];
      for (int32_t i = 0; i < BLOCK_COUNT; ++i)
        blocks[i] = new BlockCollect(100 + i);

      for (int32_t k = 0; k < BLOCK_COUNT; ++k)
      {
        EXPECT_EQ(TFS_SUCCESS, server.add(blocks[k]->id(), writable, master));
      }

      for (int32_t j = 0; j < BLOCK_COUNT; ++j)
        tbsys::gDelete(blocks[j]);
      EXPECT_EQ(BLOCK_COUNT, server.hold_->size());

      const int32_t MAX_QUERY_NUMS = random() % 512 + 1024;
      uint64_t tmp[MAX_QUERY_NUMS];
      ArrayHelper<uint64_t> helper(MAX_QUERY_NUMS, tmp);
      int32_t actual = 0;
      uint32_t begin = 0;
      bool complete = true;
      do
      {
        helper.clear();
        complete = server.get_range_blocks_(helper, begin, MAX_QUERY_NUMS);
        if (!helper.empty())
        {
          actual += helper.get_array_index();
          begin = *helper.at(helper.get_array_index() - 1);
        }
      }
      while (!complete);
      EXPECT_EQ(BLOCK_COUNT, actual);
    }

    TEST_F(ServerCollectTest, touch)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      info.use_capacity_ = 0xffffff;
      info.total_capacity_ = 0xffffffff;
      time_t now = Func::get_monotonic_time();
      bool master   = true;
      bool writable = true;
      ServerCollect server(layout_manager_, info, now);
      info.id_++;
      ServerCollect server2(layout_manager_, info,now);
      const int32_t BLOCK_COUNT = random() % 10000 + 40960;
      for (int32_t i = 0; i < BLOCK_COUNT; ++i)
      {
        BlockCollect* block = layout_manager_.get_block_manager().insert(100 + i, now);
        EXPECT_TRUE(NULL != block);
        block->add(writable, master, server.id(), now);
        EXPECT_EQ(TFS_SUCCESS, server.add(block->id(), writable, master));
      }
      EXPECT_TRUE(server.writable_->size() > 0);
      EXPECT_EQ(0, server.hold_master_->size());
      EXPECT_EQ(BLOCK_COUNT, server.hold_->size());

      bool promote = false;
      int32_t count = 0;
      int64_t average_used_capacity = 0xfff;
      EXPECT_FALSE(server.touch(promote, count, average_used_capacity));
      count = 3;
      EXPECT_FALSE(server.touch(promote, count, average_used_capacity));
      count = 3;
      promote = true;
      EXPECT_TRUE(server.touch(promote, count, average_used_capacity));
      EXPECT_EQ(3, count);
      average_used_capacity = 0xffffff;
      EXPECT_TRUE(server.touch(promote, count, average_used_capacity));
      EXPECT_EQ(3, count);

      for (int32_t i = 0; i < BLOCK_COUNT; ++i)
      {
        BlockCollect* block = layout_manager_.get_block_manager().get(100 + i);
        EXPECT_TRUE(NULL != block);
        block->add(writable, master, server2.id(), now);
        server2.add(block->id(), writable, master);
      }

      EXPECT_TRUE(server2.touch(promote, count, average_used_capacity));
      EXPECT_EQ(0, count);
      EXPECT_TRUE(server2.hold_master_->size() == 3);
      EXPECT_EQ(BLOCK_COUNT, server.hold_->size());
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
