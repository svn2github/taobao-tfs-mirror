/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_collect.cpp 5 2011-03-20 08:55:56Z
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

    class BlockCollectTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        BlockCollectTest()
        {
          SYSPARAM_NAMESERVER.max_replication_ = 2;
          SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
          SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
          SYSPARAM_NAMESERVER.max_block_size_ = 100;
          SYSPARAM_NAMESERVER.max_write_file_count_= 10;
          SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
          SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
          SYSPARAM_NAMESERVER.group_count_ = 1;
          SYSPARAM_NAMESERVER.group_seq_ = 0;
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          ngi.owner_role_= NS_ROLE_MASTER;
        }

        ~BlockCollectTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(BlockCollectTest, add)
    {
      srand(time(NULL));
      uint64_t invalid_server_id = INVALID_SERVER_ID;
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;
      EXPECT_EQ(EXIT_PARAMETER_ERROR,block.add(writable, master, invalid_server_id,now));
      EXPECT_FALSE(master);
      EXPECT_FALSE(writable);

      EXPECT_EQ(TFS_SUCCESS,block.add(writable, master, info.id_,now));
      EXPECT_TRUE(writable);

      uint64_t result = block.get_server();
      EXPECT_TRUE(result == info.id_);
      EXPECT_TRUE(block.exist(info.id_));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      //已经存在了
      EXPECT_EQ(EXIT_SERVER_EXISTED,block.add(writable, master, info.id_,now));
      result = block.get_server();
      EXPECT_TRUE(result == info.id_);
      EXPECT_TRUE(block.exist(info.id_));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, info.id_, now));
      EXPECT_EQ(2, block.get_servers_size());
      EXPECT_TRUE(block.exist(info.id_));
      EXPECT_EQ(now, block.last_update_time_);
      EXPECT_EQ(EXIT_SERVER_EXISTED,block.add(writable, master, info.id_,now));
    }

    TEST_F(BlockCollectTest, remove)
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

      uint64_t server = info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server, now));
      EXPECT_TRUE(writable);
      uint64_t result = block.get_server();
      EXPECT_TRUE(server == result);
      EXPECT_TRUE(block.exist(server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);


      uint64_t server2 = ++info.id_;
      EXPECT_EQ(EXIT_DATASERVER_NOT_FOUND, block.remove(server2, now));
      EXPECT_EQ(TFS_SUCCESS, block.remove(server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(server));
    }

    TEST_F(BlockCollectTest, exist)
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

      uint64_t server = info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server, now));
      EXPECT_TRUE(writable);

      EXPECT_TRUE(block.exist(server));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(now, block.last_update_time_);

      EXPECT_EQ(TFS_SUCCESS, block.remove(server, now));
      EXPECT_EQ(0, block.get_servers_size());
      EXPECT_FALSE(block.exist(server));
    }

    TEST_F(BlockCollectTest, check_version)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      BlockCollect block(100, now);
      bool isnew = false, master = false, writable = false;
      uint64_t server = info.id_;

      // construct block info
      BlockInfoV2 block_info;
      memset(&block_info, 0, sizeof(block_info));
      block_info.block_id_ = 100;
      block_info.file_count_ = 10;
      block_info.version_ = 100;
      block_info.size_ = 110;
      NameServer ns;
      LayoutManager manager(ns);

      uint64_t expires[SYSPARAM_NAMESERVER.max_replication_];
      ArrayHelper<uint64_t> helper(SYSPARAM_NAMESERVER.max_replication_, expires);
      EXPECT_EQ(EXIT_PARAMETER_ERROR, block.check_version(manager, helper, INVALID_SERVER_ID, isnew, block_info, now));

      //当前没任何dataserver在列表，接受当前版本
      helper.clear();
      EXPECT_EQ(TFS_SUCCESS, block.check_version(manager, helper, server, isnew, block_info, now));
      EXPECT_TRUE(block_info.version_ == block.version());
      EXPECT_TRUE(block_info.size_ ==  block.size());
      EXPECT_EQ(0, helper.get_array_index());
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server, now));

      //当前DATASERVER已经存存了，并且版本比有更新，更新BLOCKINFO
      block_info.version_+=1;
      EXPECT_EQ(EXIT_SERVER_EXISTED, block.check_version(manager, helper, server, isnew, block_info, now));
      EXPECT_EQ(0, helper.get_array_index());
      EXPECT_TRUE(block_info.version_== block.version());
      EXPECT_TRUE(block_info.size_ == block.size());

      //新的版本比nameserver上的版本高，接受新的版本
      uint64_t server2 = ++info.id_;
      block_info.version_ += 2;
      EXPECT_EQ(TFS_SUCCESS, block.check_version(manager, helper, server2, isnew, block_info, now));
      EXPECT_EQ(1, helper.get_array_index());
      EXPECT_TRUE(block_info.version_== block.version());
      EXPECT_TRUE(block_info.size_ == block.size());
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server2, now));

      //新的版本比nameserver上的版本低，失效当前版本
      uint64_t server3  = ++info.id_;
      block_info.version_ -= 3;
      EXPECT_EQ(EXIT_EXPIRE_SELF_ERROR, block.check_version(manager, helper, server3, isnew, block_info, now));
    }

    TEST_F(BlockCollectTest, check_replicate)
    {
      srand(time(NULL));
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      time_t now = Func::get_monotonic_time();
      SYSPARAM_NAMESERVER.max_replication_ = 4;
      BlockCollect block(100, now);
      bool master   = false;
      bool writable = false;

      EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now));

      uint64_t server = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server, now));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, block.check_replicate(now));

      uint64_t server2 = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server2, now));
      EXPECT_EQ(2, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now));

      uint64_t server3 = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server3, now));
      TBSYS_LOG(DEBUG, "SIZE = %d", block.get_servers_size());
      EXPECT_EQ(3, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now));

      uint64_t server4 = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server4, now));
      EXPECT_EQ(4, block.get_servers_size());
      EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now));

      SYSPARAM_NAMESERVER.max_replication_ = 2;
    }

    TEST_F(BlockCollectTest, check_compact)
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

      uint64_t server = info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server, now));
      EXPECT_EQ(1, block.get_servers_size());
      EXPECT_FALSE(block.check_compact());

      uint64_t server2 = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block.add(writable, master, server2, now));
      EXPECT_EQ(2, block.get_servers_size());
      EXPECT_FALSE(block.check_compact());

      BlockInfoV2 block_info;
      memset(&block_info, 0, sizeof(block_info));
      block_info.block_id_ = 100;
      block_info.file_count_ = 0;
      block_info.version_ = 100;
      block.update(block_info);

      EXPECT_FALSE(block.check_compact());

      block_info.file_count_ = 10;
      block_info.del_file_count_ = 1;
      block.update(block_info);
      EXPECT_FALSE(block.check_compact());

      block_info.del_file_count_ = 8;
      block_info.size_ = 110;
      block_info.del_size_ = 1;
      block.update(block_info);
      EXPECT_TRUE(block.check_compact());

      block_info.del_file_count_ = 1;
      block_info.size_ = 100;
      block_info.del_size_ = 1;
      block.update(block_info);
      EXPECT_FALSE(block.check_compact());

      block_info.del_size_ = 80;
      block.update(block_info);
      EXPECT_TRUE(block.check_compact());
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
