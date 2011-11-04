/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_server_collect.cpp 5 2010-10-21 07:44:56Z
 *
 * Authors:
 *   chuyu 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <tbsys.h>
#include <set>
#include "server_collect.h"
#include "block_collect.h"
#include "layout_manager.h"
#include "global_factory.h"

using namespace tfs::nameserver;
using namespace tfs::common;

class ServerCollectTest:public::testing::Test
{
  protected:
    static void SetUpTestCase()
    {
      TBSYS_LOGGER.setLogLevel("debug");
      GFactory::initialize();
    }

    static void TearDownTestCase()
    {
      GFactory::destroy();
    }
  public:
    ServerCollectTest()
    {
      SYSPARAM_NAMESERVER.max_block_size_ = 1024;
      SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 60;
      SYSPARAM_NAMESERVER.max_write_file_count_ = 5;
      SYSPARAM_NAMESERVER.add_primary_block_count_ = 10;
      SYSPARAM_NAMESERVER.heart_interval_ = 1000;
      SYSPARAM_NAMESERVER.min_replication_ = 2;
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
};
void clear_container(ServerCollect& server_collect);

TEST_F(ServerCollectTest, is_full)
{
  DataServerStatInfo ds_stat_info;
  ds_stat_info.total_capacity_ = 100;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  // is full
  ds_stat_info.use_capacity_ = 65;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(true, server_collect.is_full());

  // is not full
  ds_stat_info.use_capacity_ = 35;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(false, server_collect.is_full());
}

TEST_F(ServerCollectTest, is_alive_by_time)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  server_collect.touch(time(NULL));
  EXPECT_EQ(true, server_collect.is_alive(time(NULL) - 1000));
  server_collect.touch(time(NULL));
  EXPECT_EQ(false, server_collect.is_alive(time(NULL) + 10000));
}

TEST_F(ServerCollectTest, is_alive_by_stat)
{
  DataServerStatInfo ds_stat_info;
  ds_stat_info.status_ = DATASERVER_STATUS_ALIVE;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  EXPECT_EQ(true, server_collect.is_alive());
  ds_stat_info.status_ = DATASERVER_STATUS_DEAD;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(false, server_collect.is_alive());
}
TEST_F(ServerCollectTest, dead)
{
  DataServerStatInfo ds_stat_info;
  ds_stat_info.status_ = DATASERVER_STATUS_ALIVE;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  EXPECT_EQ(true, server_collect.is_alive());
  server_collect.dead();
  EXPECT_EQ(false, server_collect.is_alive());
}

TEST_F(ServerCollectTest, add)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));
  BlockInfo block_info;
  std::vector<BlockCollect*>::iterator iter;

  // block is null
  BlockCollect* block_collect = NULL;
  EXPECT_EQ(false, server_collect.add(block_collect, false));

  block_info.size_ = 10000;
  // block is full, not master
  BlockCollect block_collect_1(1001, time(NULL));
  block_info.block_id_ = 1001;
  block_collect_1.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_1, false));
  EXPECT_EQ(true, server_collect.hold_.find(&block_collect_1) != server_collect.hold_.end());
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_1) != server_collect.writable_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_1);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(true, server_collect.add(&block_collect_1, false));// warn
  EXPECT_EQ(0x01U, server_collect.block_count());
  TBSYS_LOG(DEBUG, "***************************************");

  // block is full, master
  BlockCollect block_collect_2(1003, time(NULL));
  block_info.block_id_ = 1003;
  block_collect_2.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_2, true));
  EXPECT_EQ(true, server_collect.hold_.find(&block_collect_2) != server_collect.hold_.end());
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_2) != server_collect.writable_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_2);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  TBSYS_LOG(DEBUG, "---------------------------------------");
  EXPECT_EQ(true, server_collect.add(&block_collect_2, true));// warn
  TBSYS_LOG(DEBUG, "---------------------------------------");

  block_info.size_ = 1000;
  // block is not full, not master
  BlockCollect block_collect_3(1002, time(NULL));
  block_info.block_id_ = 1002;
  block_collect_3.update(block_info);
  EXPECT_EQ(false, server_collect.hold_.find(&block_collect_3) != server_collect.hold_.end());
  EXPECT_EQ(true, server_collect.add(&block_collect_3, false));
  EXPECT_EQ(true, server_collect.hold_.find(&block_collect_3) != server_collect.hold_.end());
  EXPECT_EQ(true, server_collect.writable_.find(&block_collect_3) != server_collect.writable_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_3);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(server_collect.add(&block_collect_3, false), true);// warn
  TBSYS_LOG(DEBUG, "***************************************");

  // block is not full, master
  BlockCollect block_collect_4(1004, time(NULL));
  block_info.block_id_ = 1004;
  block_collect_4.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_4, true));
  EXPECT_EQ(true, server_collect.hold_.find(&block_collect_4) != server_collect.hold_.end());
  EXPECT_EQ(true, server_collect.writable_.find(&block_collect_4) != server_collect.writable_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_4);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  TBSYS_LOG(DEBUG, "---------------------------------------");
  EXPECT_EQ(server_collect.add(&block_collect_4, true), true);// warn
  TBSYS_LOG(DEBUG, "---------------------------------------");
}

TEST_F(ServerCollectTest, remove)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));
  BlockInfo block_info;
  // block collect is null
  BlockCollect* block_collect = NULL;
  EXPECT_EQ(false, server_collect.remove(block_collect));

  // block collect is full, not writable
  BlockCollect block_collect_1(1000, time(NULL));
  block_info.block_id_ = 1000;
  block_info.size_ = 10000;
  block_collect_1.update(block_info);
  EXPECT_EQ(server_collect.remove(&block_collect_1), true); // hold list is null
  server_collect.add(&block_collect_1, false);
  EXPECT_EQ(true, server_collect.remove(&block_collect_1));
  EXPECT_EQ(false, server_collect.hold_.find(&block_collect_1) != server_collect.hold_.end());
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_1) != server_collect.writable_.end());

  block_info.size_ = 1000;
  // block is writable
  BlockCollect block_collect_2(1001, time(NULL));
  block_info.block_id_ = 1001;
  block_collect_2.update(block_info);
  server_collect.add(&block_collect_2, false);
  EXPECT_EQ(true, server_collect.remove(&block_collect_2));
  EXPECT_EQ(false, server_collect.hold_.find(&block_collect_2) != server_collect.hold_.end());
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_2) != server_collect.writable_.end());

  // block not exist
  BlockCollect block_collect_3(1002, time(NULL));
  block_info.block_id_ = 1002;
  block_collect_3.update(block_info);
  EXPECT_EQ(true, server_collect.remove(&block_collect_3));
  EXPECT_EQ(false, server_collect.hold_.find(&block_collect_3) != server_collect.hold_.end());
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_3) != server_collect.writable_.end());
}
TEST_F(ServerCollectTest, exist)
{
  // NO CODE
}
TEST_F(ServerCollectTest, update)
{
  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));
  ServerCollect server_collect(ds_stat_info, time(NULL));
  // initial value
  EXPECT_EQ(0U, server_collect.id_);
  EXPECT_EQ(0, server_collect.use_capacity_);
  EXPECT_EQ(0, server_collect.total_capacity_);
  EXPECT_EQ(0x01, server_collect.current_load_);
  EXPECT_EQ(0, server_collect.block_count_);
  EXPECT_EQ(0, server_collect.status_);
  EXPECT_EQ(0, server_collect.read_count_);
  EXPECT_EQ(0, server_collect.write_count_);

  // new info
  ds_stat_info.id_ = 12769289;
  ds_stat_info.use_capacity_ = 1024;
  ds_stat_info.total_capacity_ = 104824;
  ds_stat_info.current_load_ = 14824;
  ds_stat_info.block_count_ = 384;
  time_t start_time = time(NULL) - 3600;
  ds_stat_info.startup_time_ = start_time;
  ds_stat_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_stat_info.total_tp_.read_file_count_ = 2891;
  ds_stat_info.total_tp_.read_byte_ = 128789723;
  ds_stat_info.total_tp_.write_file_count_ = 2391;
  ds_stat_info.total_tp_.write_byte_ = 1290706721;

  // update
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(12769289U, server_collect.id_);
  EXPECT_EQ(1024, server_collect.use_capacity_);
  EXPECT_EQ(104824, server_collect.total_capacity_);
  EXPECT_EQ(14824, server_collect.current_load_);
  EXPECT_EQ(384, server_collect.block_count_);
  EXPECT_EQ(false, (server_collect.last_update_time_ == 0));
  EXPECT_EQ(start_time, server_collect.startup_time_);
  EXPECT_EQ(DATASERVER_STATUS_ALIVE, server_collect.status_);
  EXPECT_EQ(2891, server_collect.read_count_);
  EXPECT_EQ(128789723, server_collect.read_byte_);
  EXPECT_EQ(2391, server_collect.write_count_);
  EXPECT_EQ(1290706721, server_collect.write_byte_);
}
TEST_F(ServerCollectTest, statistics)
{
  NsGlobalInfo global_info;
  memset(&global_info, 0, sizeof(global_info));

  global_info.use_capacity_ = 1024;
  global_info.total_capacity_ = 104824;
  global_info.total_load_ = 6564824;
  global_info.max_load_ = 14824;
  global_info.total_block_count_ = 840;
  global_info.max_block_count_ = 384;
  global_info.alive_server_count_ = 500;

  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));
  ServerCollect server_collect(ds_stat_info, time(NULL));
  ds_stat_info.use_capacity_ = 1000;
  ds_stat_info.total_capacity_ = 10000;
  ds_stat_info.current_load_ = 44824;
  ds_stat_info.block_count_ = 458;
  server_collect.update(ds_stat_info, time(NULL));

  // not new server
  server_collect.statistics(global_info, false);
  EXPECT_EQ(1024, global_info.use_capacity_);
  EXPECT_EQ(104824, global_info.total_capacity_);
  EXPECT_EQ(6564824, global_info.total_load_);
  EXPECT_EQ(840, global_info.total_block_count_);
  EXPECT_EQ(500, global_info.alive_server_count_);
  EXPECT_EQ(44824, global_info.max_load_);
  EXPECT_EQ(458, global_info.max_block_count_);

  // new server
  server_collect.statistics(global_info, true);
  EXPECT_EQ(2024, global_info.use_capacity_);
  EXPECT_EQ(114824, global_info.total_capacity_);
  EXPECT_EQ(6609648, global_info.total_load_);
  EXPECT_EQ(1298, global_info.total_block_count_);
  EXPECT_EQ(501, global_info.alive_server_count_);
  EXPECT_EQ(44824, global_info.max_load_);
  EXPECT_EQ(458, global_info.max_block_count_);
}
TEST_F(ServerCollectTest, remove_master)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));
  BlockCollect* block_collect = NULL;
  BlockInfo block_info;
  std::vector<BlockCollect*>::iterator iter;
  // block collect is null
  EXPECT_EQ(false, server_collect.remove_master(block_collect));

  // block collect is not master
  BlockCollect block_collect_1(1000, time(NULL));
  block_info.block_id_ = 1000;
  block_info.size_ = 10000;
  block_collect_1.update(block_info);
  server_collect.add(&block_collect_1, false);
  EXPECT_EQ(true, server_collect.remove_master(&block_collect_1));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_1);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());

  // block is master 
  BlockCollect block_collect_2(1001, time(NULL));
  block_info.block_id_ = 1001;
  block_info.size_ = 1000;
  block_collect_2.update(block_info);
  server_collect.add(&block_collect_2, true);
  EXPECT_EQ(true, server_collect.remove_master(&block_collect_2));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_2);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());

  // block not exist
  BlockCollect block_collect_3(1002, time(NULL));
  block_info.block_id_ = 1002;
  block_info.size_ = 1000;
  block_collect_3.update(block_info);
  EXPECT_EQ(true, server_collect.remove_master(&block_collect_3));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_3);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
}
TEST_F(ServerCollectTest, remove_writable)
{
  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));
  ServerCollect server_collect(ds_stat_info, time(NULL));
  BlockInfo block_info;
  std::vector<BlockCollect*>::iterator iter;
  // block collect is null
  BlockCollect* block_collect = NULL;
  EXPECT_EQ(false, server_collect.remove_writable(block_collect));

  // block collect is not writable 
  BlockCollect block_collect_1(1000, time(NULL));
  block_info.block_id_ = 1000;
  block_info.size_ = 10000;
  block_collect_1.update(block_info);
  server_collect.add(&block_collect_1, false);
  EXPECT_EQ(true, server_collect.remove_writable(&block_collect_1));
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_1) != server_collect.writable_.end());

  // block is writable 
  BlockCollect block_collect_2(1001, time(NULL));
  block_info.block_id_ = 1001;
  block_info.size_ = 1000;
  block_collect_2.update(block_info);
  server_collect.add(&block_collect_2, false);
  EXPECT_EQ(true, server_collect.remove_writable(&block_collect_2));
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_2) != server_collect.writable_.end());

  // block not exist
  BlockCollect block_collect_3(1002, time(NULL));
  block_info.block_id_ = 1002;
  block_info.size_ = 1000;
  block_collect_3.update(block_info);
  EXPECT_EQ(true, server_collect.remove_writable(&block_collect_3));
  EXPECT_EQ(false, server_collect.writable_.find(&block_collect_3) != server_collect.writable_.end());
}
TEST_F(ServerCollectTest, clear)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  //LayerManager is null
  LayoutManager manager;

  // hold list is null, return true
  EXPECT_EQ(true, server_collect.clear(manager, time(NULL)));

  // construct two blocks
  manager.initialize();
  bool master = false;
  BlockCollect block_collect_1(1001, time(NULL));// delete success
  ServerCollect server_collect_1(ds_stat_info, time(NULL));
  block_collect_1.add(&server_collect, time(NULL), false, master);
  block_collect_1.add(&server_collect_1, time(NULL), false, master);
  server_collect.add(&block_collect_1, true);

  BlockCollect block_collect_2(1002, time(NULL));// delete fail, warn, check blockid
  block_collect_2.add(&server_collect_1, time(NULL), false, master);
  server_collect.add(&block_collect_2, true);

  BlockCollect block_collect_3(1003, time(NULL));// delete fail, warn, check blockid
  server_collect.add(&block_collect_3, true);

  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(true, server_collect.clear(manager, time(NULL)));
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(false, block_collect_1.exist(&server_collect));
  EXPECT_EQ(false, block_collect_2.exist(&server_collect));

  manager.destroy();
  manager.wait_for_shut_down();
}
TEST_F(ServerCollectTest, is_writable)
{
  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));
  ServerCollect server_collect(ds_stat_info, time(NULL));

  // disk is full
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 65;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(true, server_collect.is_full());
  EXPECT_EQ(false, server_collect.is_writable(50));

  // disk is not full && use_capacity_ > average_used_capacity
  ds_stat_info.total_capacity_ = 100;
  ds_stat_info.use_capacity_ = 55;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(false, server_collect.is_full());
  EXPECT_EQ(false, server_collect.is_writable(50));

  // is writable
  EXPECT_EQ(true, server_collect.is_writable(60));
}
TEST_F(ServerCollectTest, is_readable)
{
  DataServerStatInfo ds_stat_info;
  memset(&ds_stat_info, 0, sizeof(ds_stat_info));
  ServerCollect server_collect(ds_stat_info, time(NULL));

  // less than
  ds_stat_info.current_load_ = 65;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(true, server_collect.is_readable(50));

  // more than
  ds_stat_info.current_load_ = 65;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(false, server_collect.is_readable(30));
}
TEST_F(ServerCollectTest, touch)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));
  BlockInfo block_info;
  std::vector<BlockCollect*>::iterator iter;
  uint32_t max_block_id = 1006;
  int32_t count = 0;

  LayoutManager manager;
  manager.initialize();
  ds_stat_info.total_capacity_ = 100;
  // promote is false
  EXPECT_EQ(true, server_collect.touch(manager, time(NULL), max_block_id, 3, false, count));

  // is full
  ds_stat_info.use_capacity_ = 65;
  server_collect.update(ds_stat_info, time(NULL));
  EXPECT_EQ(true, server_collect.is_full());
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(true, server_collect.touch(manager,time(NULL), max_block_id, 3, true, count));
  TBSYS_LOG(DEBUG, "***************************************");

  // not full
  // current < SYSPARAM_NAMESERVER.max_write_file_count_
  // && alive_server_size > SYSPARAM_NAMESERVER.add_primary_block_count_
  ds_stat_info.use_capacity_ = 35;
  server_collect.update(ds_stat_info, time(NULL));

  BlockCollect block_collect_1(1001, time(NULL));//both in writable and hold_master
  block_info.block_id_ = 1001;
  block_info.size_ = 1000;
  block_collect_1.update(block_info);
  server_collect.add(&block_collect_1, true);

  ServerCollect server_collect_1(ds_stat_info, time(NULL));
  ServerCollect server_collect_2(ds_stat_info, time(NULL));
  BlockCollect block_collect_2(1002, time(NULL));//is_need_master
  block_info.block_id_ = 1002;
  block_info.size_ = 1000;
  block_collect_2.update(block_info);
  block_collect_2.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;
  block_collect_2.hold_.push_back(&server_collect_1);
  block_collect_2.hold_.push_back(&server_collect_2);
  EXPECT_EQ(true, server_collect.add(&block_collect_2, false));

  BlockCollect block_collect_3(1003, time(NULL));//is_writable
  block_info.block_id_ = 1003;
  block_info.size_ = 1000;
  block_collect_3.update(block_info);
  block_collect_3.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_YES;
  block_collect_3.hold_.push_back(&server_collect_1);
  block_collect_3.hold_.push_back(&server_collect_2);
  EXPECT_EQ(true, server_collect.add(&block_collect_3, false));

  BlockCollect block_collect_4(1004, time(NULL));// do not need master and not writable
  block_info.block_id_ = 1004;
  block_info.size_ = 1000;
  block_collect_4.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_4, false));

  // diff < count
  count = 5;
  TBSYS_LOG(DEBUG, "---------------------------------------");
  EXPECT_EQ(true, server_collect.touch(manager, time(NULL), max_block_id, 25, true, count));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_1);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_2);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_3);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_4);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
  EXPECT_EQ(0, count);
  TBSYS_LOG(DEBUG, "---------------------------------------");

  // diff >= count
  TBSYS_LOG(DEBUG, "***************************************");
  count = 3;
  server_collect.remove_master(&block_collect_2);
  server_collect.remove_master(&block_collect_3);
  EXPECT_EQ(true, server_collect.touch(manager, time(NULL), max_block_id, 25, true, count));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_1);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_2);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_3);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_4);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
  EXPECT_EQ(1, count);
  TBSYS_LOG(DEBUG, "***************************************");

  // current < SYSPARAM_NAMESERVER.max_write_file_count_
  // && alive_server_size <= SYSPARAM_NAMESERVER.add_primary_block_count_
  TBSYS_LOG(DEBUG, "---------------------------------------");
  count = 7;
  server_collect.remove_master(&block_collect_2);
  server_collect.remove_master(&block_collect_3);
  EXPECT_EQ(true, server_collect.touch(manager, time(NULL), max_block_id, 10, true, count));
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_1);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_2);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_3);
  EXPECT_EQ(true, iter != server_collect.hold_master_.end());
  iter = find(server_collect.hold_master_.begin(), server_collect.hold_master_.end(), &block_collect_4);
  EXPECT_EQ(false, iter != server_collect.hold_master_.end());
  EXPECT_EQ(2, count);
  TBSYS_LOG(DEBUG, "---------------------------------------");

  // current >= SYSPARAM_NAMESERVER.max_write_file_count_
  BlockCollect block_collect_5(1005, time(NULL));
  block_info.block_id_ = 1005;
  block_info.size_ = 1000;
  block_collect_5.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_5, true));

  BlockCollect block_collect_6(1006, time(NULL));
  block_info.block_id_ = 1006;
  block_info.size_ = 1000;
  block_collect_6.update(block_info);
  EXPECT_EQ(true, server_collect.add(&block_collect_6, true));

  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(true, server_collect.touch(manager, time(NULL), max_block_id, 3, true, count));
  TBSYS_LOG(DEBUG, "***************************************");

  manager.destroy();
  manager.wait_for_shut_down();
}
TEST_F(ServerCollectTest, elect_write_block)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  //LayerManager is null
  EXPECT_EQ(true, server_collect.elect_write_block() == NULL);

  // write_index >= hold size, return write_index = 0
  EXPECT_EQ(true, server_collect.elect_write_block() == NULL);
  EXPECT_EQ(0, server_collect.write_index_);

  // write_index < hold size && get lease is false
  BlockCollect block_collect_1(1001, time(NULL));
  server_collect.add(&block_collect_1, true);
  BlockCollect block_collect_2(1002, time(NULL));
  server_collect.add(&block_collect_2, true);
  TBSYS_LOG(DEBUG, "***************************************");
  BlockCollect* ret_block = server_collect.elect_write_block();
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(1001U, ret_block->id());
  EXPECT_EQ(1, server_collect.write_index_);

  // write_index < hold size && get lease is true 
  server_collect.write_index_ = 0;
  GFactory::get_lease_factory().add(1001);
  TBSYS_LOG(DEBUG, "---------------------------------------");
  ret_block = server_collect.elect_write_block();
  TBSYS_LOG(DEBUG, "---------------------------------------");
  EXPECT_EQ(1002U, ret_block->id());
  EXPECT_EQ(2, server_collect.write_index_);

  // write_index < hold size && all block has valid lease
  server_collect.write_index_ = 0;
  GFactory::get_lease_factory().add(1002);
  TBSYS_LOG(DEBUG, "***************************************");
  ret_block = server_collect.elect_write_block();
  TBSYS_LOG(DEBUG, "***************************************");
  EXPECT_EQ(true, ret_block == NULL);
  EXPECT_EQ(2, server_collect.write_index_);
}

TEST_F(ServerCollectTest, can_be_master)
{
  DataServerStatInfo ds_stat_info;
  ServerCollect server_collect(ds_stat_info, time(NULL));

  BlockCollect block_collect_1(1001, time(NULL));
  server_collect.add(&block_collect_1, true);

  BlockCollect block_collect_2(1002, time(NULL));
  server_collect.add(&block_collect_2, true);

  BlockCollect block_collect_3(1003, time(NULL));
  server_collect.add(&block_collect_3, true);

  EXPECT_EQ(true, server_collect.can_be_master(5));
  EXPECT_EQ(false, server_collect.can_be_master(3));
}

TEST_F(ServerCollectTest, add_writable)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  ServerCollect server(info, time(NULL));
  BlockCollect* tmp  = NULL;
  EXPECT_TRUE(server.add_writable(tmp) == false);
  
  BlockCollect block(0xffff, time(NULL));
  BlockInfo binfo;
  memset(&binfo, 0, sizeof(binfo));
  binfo.block_id_ = 0xffff;
  binfo.size_ = 0x00;
  block.update(binfo);

  EXPECT_TRUE(server.add_writable(&block) == true);
  EXPECT_TRUE(server.add_writable(&block) == true);

  binfo.block_id_ = 0xffff;
  binfo.size_ = SYSPARAM_NAMESERVER.max_block_size_ * 1000;
  block.update(binfo);

  EXPECT_TRUE(server.add_writable(&block) == false);

  server.remove_writable(&block);
  binfo.block_id_ = 0xffff;
  binfo.size_ = 0x00;
  block.update(binfo);
  EXPECT_TRUE(server.add_writable(&block) == true);
}

TEST_F(ServerCollectTest, scan)
{
  uint32_t block_id = 0xffff;
  time_t now = time(NULL);
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.current_load_ = 0x01;
  ServerCollect server(info, now);

  BlockCollect block(0xffff, time(NULL));
  BlockInfo binfo;
  memset(&binfo, 0, sizeof(binfo));
  binfo.block_id_ = block_id;
  binfo.size_ = 0x00;
  block.update(binfo);

  EXPECT_TRUE(server.add(&block, true) == true);
 
  SSMScanParameter param;
  int8_t scan_flag = 0;
  scan_flag |= SSM_CHILD_SERVER_TYPE_INFO;
  scan_flag |= SSM_CHILD_SERVER_TYPE_HOLD;
  scan_flag |= SSM_CHILD_SERVER_TYPE_WRITABLE;
  scan_flag |= SSM_CHILD_SERVER_TYPE_MASTER;

  EXPECT_EQ(TFS_SUCCESS, server.scan(param, scan_flag));
  EXPECT_TRUE(info.id_ == param.data_.readInt64());
  EXPECT_TRUE(info.use_capacity_ == param.data_.readInt64());
  EXPECT_TRUE(info.total_capacity_ == param.data_.readInt64());
  EXPECT_TRUE(info.current_load_ == param.data_.readInt32());
  EXPECT_TRUE(info.block_count_ == param.data_.readInt32());
  EXPECT_TRUE(now == param.data_.readInt64());
  EXPECT_TRUE(now == param.data_.readInt64());
  EXPECT_TRUE(0 == param.data_.readInt64());
  EXPECT_TRUE(0 == param.data_.readInt64());
  EXPECT_TRUE(0 == param.data_.readInt64());
  EXPECT_TRUE(0 == param.data_.readInt64());
  param.data_.readInt64();
  EXPECT_TRUE(info.status_ == param.data_.readInt32());

  EXPECT_TRUE(1 == param.data_.readInt32());
  EXPECT_TRUE(block_id == param.data_.readInt32());
  EXPECT_TRUE(1 == param.data_.readInt32());
  EXPECT_TRUE(block_id == param.data_.readInt32());
  EXPECT_TRUE(1 == param.data_.readInt32());
  EXPECT_TRUE(block_id == param.data_.readInt32());
}

void clear_container(ServerCollect& server_collect)
{
  std::set<BlockCollect*>::iterator hold_iter;
  for(hold_iter = server_collect.hold_.begin(); hold_iter != server_collect.hold_.end(); hold_iter++)
  {
    if(*hold_iter != NULL)
    {
      delete *hold_iter;
    }
  }
}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
