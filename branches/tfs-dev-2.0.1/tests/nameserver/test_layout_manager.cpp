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
 *   chuyu 
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <Handle.h>
#include <Shared.h>
#include <Memory.hpp>
#include "nameserver.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include <vector>
#include <bitset>

using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

NameServer g_name_server;

class LayoutManagerTest: public virtual ::testing::Test
{
public:
  LayoutManagerTest()
  {
  }   
  ~LayoutManagerTest()
  {
  }
  void clear_task();
  void clear();
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
  virtual void SetUp()
  {
    SYSPARAM_NAMESERVER.min_replication_ = 2;
    SYSPARAM_NAMESERVER.max_replication_ = 4;
    SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
    SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
    SYSPARAM_NAMESERVER.max_block_size_ = 100;
    SYSPARAM_NAMESERVER.max_write_file_count_= 10;
    SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
    SYSPARAM_NAMESERVER.group_mask_= 0xffffffff;
    SYSPARAM_NAMESERVER.run_plan_expire_interval_= 120;
    SYSPARAM_NAMESERVER.run_plan_ratio_ = 100;
    SYSPARAM_NAMESERVER.build_plan_interval_ = 1;
    SYSPARAM_NAMESERVER.add_primary_block_count_ = 3;
    SYSPARAM_NAMESERVER.safe_mode_time_ = -1;
    SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
    SYSPARAM_NAMESERVER.object_clear_max_time_ = 1;
    SYSPARAM_NAMESERVER.group_count_ = 1;
    SYSPARAM_NAMESERVER.group_seq_ = 0;
    meta_mgr_ = &g_name_server.get_layout_manager(); 
    meta_mgr_->initialize(4);
  }
  virtual void TearDown()
  {
    meta_mgr_->destroy();
    meta_mgr_->wait_for_shut_down();
    clear();
  }
  LayoutManager* meta_mgr_;
};

TEST_F(LayoutManagerTest, initialize)
{

}

TEST_F(LayoutManagerTest, wait_for_shut_down)
{

}

TEST_F(LayoutManagerTest, get_server)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);

  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(true, server->id() == info.id_);

  if (server != NULL)
    server->dead();

  ServerCollect* server2 = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server2 == NULL);
}

TEST_F(LayoutManagerTest, add_server) 
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);

  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);

  ServerCollect* server = meta_mgr_->get_server(info.id_);
  int32_t current_load = server->load(); 
  EXPECT_EQ(true, server != NULL);
  if (server != NULL)
    server->dead();

  info.current_load_ = 100;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);

  ServerCollect* server2 = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server2->load() != current_load);
}

TEST_F(LayoutManagerTest, remove_server) 
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  const int32_t count = 5;
  BlockInfo binfo;
  std::vector<BlockInfo> blocks;
  for (int i = 0; i < count; ++i)
  {
    memset(&binfo, 0, sizeof(binfo));
    binfo.block_id_ = 100 + i;
    blocks.push_back(binfo);
  }

  EXPIRE_BLOCK_LIST expires;

  EXPECT_EQ(TFS_ERROR, meta_mgr_->update_relation(NULL, blocks, expires, now));

  server->status_= DATASERVER_STATUS_DEAD;
  EXPECT_EQ(TFS_ERROR, meta_mgr_->update_relation(server, blocks, expires, now));

  server->status_= DATASERVER_STATUS_ALIVE;

  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->update_relation(server, blocks, expires, now));
  
  EXPECT_EQ(count, server->block_count());

  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->remove_server(info.id_, now));

  EXPECT_EQ(0, server->block_count());
  EXPECT_EQ(DATASERVER_STATUS_DEAD, server->status_);
  EXPECT_EQ(0, meta_mgr_->alive_server_size_);

  for (int i = 0; i < count; ++i)
  {
    BlockChunkPtr ptr = meta_mgr_->get_chunk(100 + i);
    BlockCollect* block = ptr->find(100 + i);
    EXPECT_EQ(true, block != NULL);
    
    EXPECT_EQ(false, block->exist(server));
    EXPECT_EQ(0, block->get_hold_size());
  }
}

TEST_F(LayoutManagerTest, add_block) 
{
  uint32_t block_id = 0;
  EXPECT_EQ(true, meta_mgr_->add_block(block_id) == NULL);
  block_id = 100;
  EXPECT_EQ(true, meta_mgr_->add_block(block_id) != NULL);
  BlockChunkPtr ptr = meta_mgr_->get_chunk(block_id);
  BlockCollect* block = ptr->find(block_id);
  EXPECT_EQ(true, block != NULL);
}

TEST_F(LayoutManagerTest, add_new_block)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  const int32_t count = 3;
  for (int32_t i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xfffff;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  }
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  uint32_t block_id = 0;
  
  EXPECT_EQ(true, meta_mgr_->add_new_block(block_id, server) != NULL);
}

TEST_F(LayoutManagerTest, update_relation)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  const int32_t count = 5;
  BlockInfo binfo;
  std::vector<BlockInfo> blocks;
  for (int i = 0; i < count; ++i)
  {
    memset(&binfo, 0, sizeof(binfo));
    binfo.block_id_ = 100 + i;
    blocks.push_back(binfo);
  }

  EXPIRE_BLOCK_LIST expires;

  EXPECT_EQ(TFS_ERROR, meta_mgr_->update_relation(NULL, blocks, expires, now));

  server->status_= DATASERVER_STATUS_DEAD;
  EXPECT_EQ(TFS_ERROR, meta_mgr_->update_relation(server, blocks, expires, now));

  server->status_= DATASERVER_STATUS_ALIVE;

  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->update_relation(server, blocks, expires, now));
  
  EXPECT_EQ(count, server->block_count());
}

TEST_F(LayoutManagerTest, build_relation)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool force = false;
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  uint32_t block_id = 100;
  BlockCollect* block = meta_mgr_->add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());

  EXPECT_EQ(TFS_ERROR, meta_mgr_->build_relation(NULL, NULL, now, force));
  EXPECT_EQ(TFS_ERROR, meta_mgr_->build_relation(NULL, server, now, force));
  EXPECT_EQ(TFS_ERROR, meta_mgr_->build_relation(block, NULL, now, force));
  server->dead();
  EXPECT_EQ(TFS_ERROR, meta_mgr_->build_relation(block, NULL, now, force));

  server->status_ = DATASERVER_STATUS_ALIVE;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->build_relation(block, server, now, force));
  force = true;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->build_relation(block, server, now, force));
}

TEST_F(LayoutManagerTest, relieve_relation_all)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  bool master = false;
  uint32_t block_id = 100;
  BlockCollect* block = meta_mgr_->add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  ++block_id; 
  block = meta_mgr_->add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  ++block_id; 
  block = meta_mgr_->add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  EXPECT_EQ(0x03, server->block_count());

  EXPECT_EQ(true, meta_mgr_->relieve_relation(server, now));

  EXPECT_EQ(0x00, server->block_count());
}

TEST_F(LayoutManagerTest, relieve_relation)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  bool master = false;
  uint32_t block_id = 100;
  BlockCollect* block = meta_mgr_->add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  EXPECT_EQ(0x01, server->block_count());

  EXPECT_EQ(true, meta_mgr_->relieve_relation(block, server, now));

  EXPECT_EQ(0x00, server->block_count());
}

TEST_F(LayoutManagerTest, update_block_info)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  EXPECT_EQ(1, meta_mgr_->alive_server_size_);
  
  ServerCollect* server = meta_mgr_->get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  BlockInfo binfo;
  memset(&binfo, 0, sizeof(binfo));
  binfo.block_id_ = 1000;

  bool add_new = true;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->update_block_info(binfo, server->id(), now, add_new));

  BlockChunkPtr ptr = meta_mgr_->get_chunk(binfo.block_id_);
  BlockCollect* block = ptr->find(binfo.block_id_); 
  EXPECT_TRUE(block != NULL);
  EXPECT_EQ(binfo.version_, block->version());

  add_new = false;
  binfo.version_ += 10;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->update_block_info(binfo, server->id(), now, add_new));
  ptr = meta_mgr_->get_chunk(binfo.block_id_);
  block = ptr->find(binfo.block_id_);
  EXPECT_TRUE(block != NULL);
  EXPECT_EQ(binfo.version_, block->version());

  binfo.version_ -= 5;
  EXPECT_EQ(EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR, meta_mgr_->update_block_info(binfo, server->id(), now, add_new));

  ptr = meta_mgr_->get_chunk(binfo.block_id_);
  ptr->remove(block->id());

  binfo.version_ += 20;
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, meta_mgr_->update_block_info(binfo, server->id(), now, add_new));
}

TEST_F(LayoutManagerTest, rotate)
{
 
}

TEST_F(LayoutManagerTest, get_alive_block_id)
{
  // add_new_block has been called once, thus +2
  EXPECT_EQ(static_cast<uint32_t>(BlockIdFactory::BLOCK_START_NUMBER + 2), meta_mgr_->get_alive_block_id());
}


TEST_F(LayoutManagerTest, calc_all_block_bytes)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;
  bool is_new = false;
  time_t now = time(NULL);
  uint32_t block_id = 0;

  int32_t i = 0;
  for(i = 0; i < 3; i++)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }
  for(i = 0; i < 10; i++)
  {
    block_id = 0;
    BlockCollect* block_collect = meta_mgr_->add_new_block(block_id);
    BlockInfo block_info;
    block_info.block_id_ = block_id;
    block_info.size_ = 100;
    block_collect->update(block_info);
  }
  EXPECT_EQ(1000, meta_mgr_->calc_all_block_bytes());
  
}

TEST_F(LayoutManagerTest, calc_all_block_count)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;

  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;
  bool is_new = false;
  time_t now = time(NULL);
  uint32_t block_id = 0;

  int32_t i = 0;
  for(i = 0; i < 3; i++)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }
  for(i = 0; i < 10; i++)
  {
    block_id = 0;
    meta_mgr_->add_new_block(block_id);
  }
  EXPECT_EQ(10, meta_mgr_->calc_all_block_count());
}

TEST_F(LayoutManagerTest, elect_write_block)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;

  EXPECT_EQ(true, meta_mgr_->elect_write_block() == NULL);

  const int32_t count = 5;
  for (int i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xffffff + i;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
    EXPECT_EQ((i + 1), meta_mgr_->alive_server_size_);
  }

  uint32_t block_base = 0;
  const int32_t block_count = 20;
  for (int i = 0; i < block_count; ++i)
  {
    block_base = 0;
    BlockCollect* block = meta_mgr_->add_new_block(block_base);
    if (block != NULL)
    {
      TBSYS_LOG(DEBUG, "block(%u), size(%d)", block_base, block->get_hold_size());
    }
  } 
  uint64_t id = 10000;
  for (int i = 0; i < count; ++i)
  {
    id++;
    ServerCollect* server = meta_mgr_->get_server(id);
    if (server != NULL)
    {
      std::string buf;
      for (uint32_t j = 0; j < server->hold_master_.size(); ++j)
      {
        stringstream str;
        str << server->hold_master_[j]->id();
        buf += " ";
        buf += str.str();
      }
      TBSYS_LOG(DEBUG, "hold_(%u), writable(%u), hold_master_(%u:%s)", server->hold_.size(),
        server->writable_.size(), server->hold_master_.size(), buf.c_str());
    }
  }
  const int32_t elect_count = 100;
  std::string buf;
  for (int i = 0; i < elect_count; i++)
  {
    stringstream str;
    BlockCollect* block = meta_mgr_->elect_write_block();
    if (block != NULL)
    {
      str << block->id();
      buf += " ";
      buf += str.str();
      if (i % block_count == 0)
        buf += "\n";
    }
  }
  uint32_t block_index = 129;
  TBSYS_LOG(DEBUG, "elect result(%s)", buf.c_str());
  for (int i = 0; i <block_count; ++i)
  {
    GFactory::get_lease_factory().add(block_index + i);
  }

  buf.clear();
  for (int i = 0; i < elect_count; i++)
  {
    stringstream str;
    BlockCollect* block = meta_mgr_->elect_write_block();
    if (block != NULL)
    {
      str << block->id();
      buf += " ";
      buf += str.str();
      if (i % block_count == 0)
        buf += "\n";
    }
  }
  TBSYS_LOG(DEBUG, "second elect result(%s)", buf.c_str());
}

TEST_F(LayoutManagerTest, touch)
{
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 10000;
  ds_info.total_capacity_ = 0xffffff;
  bool is_new = false;
  time_t now = time(NULL);
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  ServerCollect* server_collect = meta_mgr_->get_server(ds_info.id_);

  EXPECT_EQ(TFS_ERROR, meta_mgr_->touch(ds_info.id_ + 1, now, false));
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->touch(ds_info.id_, now, false));
  EXPECT_EQ(0, server_collect->block_count());

  for (int32_t i = 0; i < 3; ++i)
  {
    ds_info.id_++;
    ds_info.total_capacity_ = 0xfffff;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  // master
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.owner_role_ = NS_ROLE_MASTER; 
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->touch(ds_info.id_, now, true));
  EXPECT_EQ(SYSPARAM_NAMESERVER.add_primary_block_count_, server_collect->block_count());
}

TEST_F(LayoutManagerTest, check_server)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;

  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  
  DataServerStatInfo ds_info; 
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.id_ = 0xfffffff0;
  ds_info.total_capacity_ = 0xffffff;
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  time_t now = time(NULL);
  bool is_new = false;
 
  const int32_t count = 6;
  for (int i = 0; i < count; ++i)
  {
    ds_info.id_++;
    ds_info.total_capacity_ = 0xffffff + i;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
    ServerCollect* server = meta_mgr_->get_server(ds_info.id_);
    server->touch(time(NULL) + 10800);
    EXPECT_EQ((i + 1), meta_mgr_->alive_server_size_);
    if(i == 2 || i == 4)
    {
      server->touch(time(NULL) - 3600);
      TBSYS_LOG(DEBUG, "dead server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
    }
  }
  EXPECT_EQ(6U, meta_mgr_->servers_.size());
  meta_mgr_->check_server();
  EXPECT_EQ(4U, meta_mgr_->servers_.size());
}

TEST_F(LayoutManagerTest, repair)
{
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.total_capacity_ = 0xffffff;

  uint32_t block_id = 0;
  uint64_t server_id = ds_info.id_;
  int32_t flag = UPDATE_BLOCK_MISSING;
  std::string error_msg = "";
  bool is_new = false;
  time_t now = time(NULL);
  bool master = false;

  // block not exist
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, meta_mgr_->repair(block_id, server_id, flag, now, error_msg));

  // server not find
  block_id = 101;
  BlockCollect* block_collect = meta_mgr_->add_block(block_id);
  EXPECT_EQ(EXIT_DATASERVER_NOT_FOUND, meta_mgr_->repair(block_id, server_id, flag, now, error_msg));

  int32_t i = 0;
  for(i = 0; i < 2; i++)
  {
    ds_info.id_ ++;
    ds_info.current_load_ = 1000;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  ServerCollect* server_collect = meta_mgr_->get_server(ds_info.id_ - 1);
  block_collect->add(server_collect, now, false, master);
  server_collect->add(block_collect, false);

  // dest server is owner
  server_id = ds_info.id_ - 1;
  EXPECT_EQ(EXIT_NO_DATASERVER, meta_mgr_->repair(block_id, server_id, flag, now, error_msg));

  // emergency replicate
  server_id = ds_info.id_;
  EXPECT_EQ(STATUS_MESSAGE_REMOVE, meta_mgr_->repair(block_id, server_id, flag, now, error_msg));
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(server_collect));
  //server_collect = meta_mgr_->get_server(server_id);
  //EXPECT_EQ(true, find_server_in_plan(server_collect));

  // server count >= min_replicate
  server_collect = meta_mgr_->get_server(server_id);
  block_collect->add(server_collect, now, false, master);
  server_collect->add(block_collect, false);
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, meta_mgr_->repair(block_id, server_id, flag, now, error_msg));

  clear_task();
}

TEST_F(LayoutManagerTest, build_plan)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  ngi.owner_role_   = NS_ROLE_MASTER;

  bool is_new = false;
  time_t now = time(NULL);
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xffffff10;
  ds_info.current_load_ = 0xff;
  ds_info.total_capacity_ = 0xffffff;

  uint32_t block_id = 0;
  int32_t i = 0;
  for (i = 0; i < SYSPARAM_NAMESERVER.min_replication_ ; ++i)
  {
    ds_info.id_ ++;
    ds_info.current_load_ = ds_info.current_load_ * (i + 1);
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new)); //replicate && balance source server
  }
  const int32_t MAX_REPLICATE_BLOCK = 0x02;
  for (i = 0; i < MAX_REPLICATE_BLOCK; ++i) //replicate block
  {
    block_id = 0;
    meta_mgr_->add_new_block(block_id);
  }

  //add new server
  ds_info.id_ ++;
  ds_info.current_load_ = ds_info.current_load_ * (SYSPARAM_NAMESERVER.min_replication_ + 1);
  const int32_t MAX_BALANCE_BLOCK = 0x02;//balance
  for (block_id = 0, i = 0; i < MAX_BALANCE_BLOCK; i++)
  {
    block_id = 0;
    BlockCollect* block = meta_mgr_->add_new_block(block_id);
    BlockInfo info;
    memset(&info, 0, sizeof(info));
    info.block_id_ = block_id;
    info.size_ = SYSPARAM_NAMESERVER.max_block_size_ + 0xff;
    if (block != NULL)
    {
      block->update(info);
    }
  }

  for (i = SYSPARAM_NAMESERVER.min_replication_; i < SYSPARAM_NAMESERVER.max_replication_; i++)
  {
    ds_info.id_ ++;
    ds_info.current_load_ = ds_info.current_load_ * ((i % 2) + 1);
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new)); 
  } 

  const int32_t MAX_COMPACT_BLOCK_COUNT = 0x2;//compact
  for (block_id = 0, i = 0; i < MAX_COMPACT_BLOCK_COUNT; i++)
  {
    block_id = 0;
    BlockCollect* block = meta_mgr_->add_new_block(block_id);
    BlockInfo binfo;
    memset(&binfo, 0, sizeof(binfo));
    binfo.block_id_ = block_id;
    binfo.size_ = SYSPARAM_NAMESERVER.max_block_size_ + 0xff;
    binfo.del_size_ = binfo.size_ / 2 + 1;
    binfo.file_count_ = 3;
    binfo.del_file_count_ = binfo.file_count_ / 2 + 1;
    if (block != NULL)
    {
      block->update(binfo);
    }
  }

  SYSPARAM_NAMESERVER.compact_delete_ratio_ = 40;//%40
  bool master = false;
  const int32_t MAX_DELETE_BLOCK_COUNT = 0x02;//delete
  for (block_id = 0, i = 0; i < MAX_DELETE_BLOCK_COUNT; i++)
  {
    block_id = 0;
    BlockCollect* block = meta_mgr_->add_new_block(block_id);
    BlockInfo binfo;
    memset(&binfo, 0, sizeof(binfo));
    binfo.block_id_ = block_id;
    binfo.size_ = SYSPARAM_NAMESERVER.max_block_size_ + 0xff;
    binfo.del_size_ = binfo.size_ / 2 + 1;
    binfo.file_count_ = 3;
    binfo.del_file_count_ = binfo.file_count_ / 2 + 1;
    if (block != NULL)
    {
      block->update(binfo);
      ++ds_info.id_;
      ds_info.current_load_ = ds_info.current_load_ * ((i%2) + 1);
      meta_mgr_->add_server(ds_info, now, is_new);
      ServerCollect* server = meta_mgr_->get_server(ds_info.id_);
      block->add(server, now, false, master);
      server->add(block, false);
    }
  }

  const int32_t MAX_ADD_TARGET_SERVER = 0x0f;
  for (i = 0; i < MAX_ADD_TARGET_SERVER; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new)); 
  }

  EXPECT_EQ(0, meta_mgr_->build_plan());

  meta_mgr_->dump_plan();

  EXPECT_TRUE(meta_mgr_->server_to_task_.size() != 0U);
  EXPECT_TRUE(meta_mgr_->block_to_task_.size() != 0U);
  EXPECT_TRUE(meta_mgr_->pending_plan_list_.size() != 0U);
  clear_task();
}

TEST_F(LayoutManagerTest, run_plan)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;

  bool is_new = false;
  time_t now = time(NULL);
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;

  for (int32_t i = 0; i <  SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  int64_t need = 0x2;
  uint32_t block_id = 0;
  BlockCollect* block = meta_mgr_->add_new_block(block_id);

  ++ds_info.id_;
  meta_mgr_->add_server(ds_info, now, is_new);
  ServerCollect* server = meta_mgr_->get_server(ds_info.id_);

  bool master = false;
  block->add(server, now, false, master);
  server->add(block, false);

  int current_plan_seqno = 1;
  need = 0x02;
  std::vector<uint32_t> blocks;
  meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks);
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x1U, meta_mgr_->block_to_task_.size());

  meta_mgr_->dump_plan();

  meta_mgr_->run_plan();

  LayoutManager::DeleteBlockTaskPtr task = LayoutManager::DeleteBlockTaskPtr::dynamicCast(meta_mgr_->find_task(block->id()));
  LayoutManager::DeleteBlockTaskPtr task_move =  LayoutManager::DeleteBlockTaskPtr::dynamicCast((*meta_mgr_->running_plan_list_.begin()));
  EXPECT_EQ(task->block_id_, task_move->block_id_);
  EXPECT_EQ(task->type_, task_move->type_);
  EXPECT_EQ(task->priority_, task_move->priority_);
  EXPECT_EQ(0x0U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x1U, meta_mgr_->block_to_task_.size());

  meta_mgr_->dump_plan();

  clear_task();
}

TEST_F(LayoutManagerTest, handle_task_complete)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;

  // build replicate plan
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;
  std::vector<ServerCollect*> runer;
  VUINT64 ds_list;
  bool is_new = false;
  time_t now = time(NULL);
  uint32_t block_id = 0;

  for(int32_t i = 0; i < 2; i++)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
    ds_list.push_back(ds_info.id_);
  }

  block_id = 0;
  BlockCollect* block_collect = meta_mgr_->add_new_block(block_id);//normal replicate

  ds_info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));

  EXPECT_EQ(0, meta_mgr_->build_plan());

  // handle replicate task complete as master
  ngi.owner_role_ = NS_ROLE_MASTER;
  ReplicateBlockMessage replicate_msg;
  ReplBlock repl_block;
  memset(&repl_block, 0, sizeof(repl_block));
  repl_block.block_id_ = block_id;
  repl_block.source_id_ = ds_info.id_ - 2; 
  repl_block.destination_id_ = ds_info.id_; 
  repl_block.start_time_ = now;
  repl_block.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_NO;
  repl_block.server_count_ = 0;
  replicate_msg.set_repl_block(&repl_block);
  replicate_msg.set_command(PLAN_STATUS_END);
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&replicate_msg));
  EXPECT_EQ(1U, meta_mgr_->finish_plan_list_.size());
  // handle again
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  EXPECT_EQ(0, meta_mgr_->build_plan());
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&replicate_msg)); // warn, task is exist
  //replicate_msg.set_command(COMMAND_REPLICATE);
  //EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&replicate_msg)); // error, handle complete failed

  // handle replicate task complete as slave
  ngi.owner_role_ = NS_ROLE_SLAVE; 
  meta_mgr_->expire();
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&replicate_msg));
  EXPECT_EQ(0U, meta_mgr_->finish_plan_list_.size());
  clear_task();

  // build compact plan
  int64_t need = 2;
  BlockInfo block_info;
  block_info.block_id_ = block_id;
  block_info.del_size_ = 256;
  block_info.size_ = 384;
  block_info.del_file_count_ = 70;
  block_info.file_count_= 100;
  block_collect->update(block_info);

  int current_plan_seqno = 1;
  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(1, need);
  //run_plan();
  // handle compact task complete as master
  ngi.owner_role_ = NS_ROLE_MASTER;
  CompactBlockCompleteMessage compact_msg;
  compact_msg.set_block_id(block_id);
  compact_msg.set_server_id(ds_list.at(0));
  compact_msg.set_block_info(block_info);
  compact_msg.set_success(PLAN_STATUS_END);
  std::bitset < 3 > bset;
  bset[0] = false;
  bset[1] = true;
  bset[2] = true;
  compact_msg.set_flag(bset.to_ulong());
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&compact_msg));
  EXPECT_EQ(1U, meta_mgr_->finish_plan_list_.size());

  // handle again
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&compact_msg)); // warn, task is exist
  // handle compact task complete as slave
  ngi.owner_role_ = NS_ROLE_SLAVE; 
  meta_mgr_->expire();
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&compact_msg));
  EXPECT_EQ(0U, meta_mgr_->finish_plan_list_.size());
  clear_task();
}

TEST_F(LayoutManagerTest, build_replicate_plan)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;

  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 10;
  ds_info.total_capacity_ = 0xffffff;

  const int32_t BASE_LOAD = 0xff;

  bool is_new = false;
  time_t now = time(NULL);
  const int32_t MAX_COUNT_SERVER = 10;
  int32_t j = MAX_COUNT_SERVER - 1;
  for (int32_t i = 0; i < MAX_COUNT_SERVER; ++i, --j)
  {
    ++ds_info.id_;
    ds_info.current_load_ = j * BASE_LOAD;
    TBSYS_LOG(DEBUG, "LOAD(%d)", ds_info.current_load_);
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }
  int32_t count = 0;
  uint32_t block_id = 0;
  BlockCollect* block = NULL;
  do
  {
    block_id = 0;
    block = meta_mgr_->add_new_block(block_id);//emergency replicate
    if (block != NULL)
    {
      block->dump();
      std::vector<ServerCollect*> vec = block->get_hold();
      if (!vec.empty())
      {
        std::vector<ServerCollect*>::reverse_iterator iter = vec.rbegin();
        ++iter;
        for (; iter != vec.rend(); ++iter)
        {
          block->remove(*iter, now);
        }
        count++;
      }
      block->dump();
    }
  }
  while (count < 0x02);

  TBSYS_LOG(DEBUG, "===============================");

  uint32_t normal_block_id = 0;
  BlockCollect* normal_block = meta_mgr_->add_new_block(normal_block_id);//normal replicate
  if (normal_block != NULL)
  {
    normal_block->dump();
    std::vector<ServerCollect*> tmp = normal_block->get_hold();
    if (!tmp.empty())
    {
      normal_block->remove(tmp[0], now);
    }
    normal_block->dump();
  }

  TBSYS_LOG(DEBUG, "===============================");
  int current_plan_seqno = 1;
  int64_t need = 1;
  int64_t adjust = 0;
  int64_t emergency_replicate_count = 0;
  ds_info.id_ ++;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, meta_mgr_->build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count, blocks));

  TBSYS_LOG(DEBUG, "===============================");
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));
  LayoutManager::ReplicateTaskPtr task = LayoutManager::ReplicateTaskPtr::dynamicCast(meta_mgr_->find_task(block_id));
  EXPECT_EQ(PLAN_TYPE_REPLICATE, task->type_);
  EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, task->priority_);
  EXPECT_EQ(block_id, task->block_id_);
  EXPECT_EQ(0x4U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x2U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x2U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x0, need);

  meta_mgr_->dump_plan();

  adjust = 0;
  need = 0x02;
  emergency_replicate_count = 0;
  SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
  ds_info.id_ ++;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  EXPECT_EQ(true, meta_mgr_->build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(false, meta_mgr_->find_block_in_plan(normal_block_id));
  EXPECT_EQ(false, meta_mgr_->find_server_in_plan(meta_mgr_->get_server(ds_info.id_)));
  EXPECT_EQ(0x02, need);

  meta_mgr_->dump_plan();

  adjust = 0;
  emergency_replicate_count = 0;
  SYSPARAM_NAMESERVER.replicate_ratio_ = 25;
  EXPECT_EQ(true, meta_mgr_->build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(normal_block_id));
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(meta_mgr_->get_server(ds_info.id_)));
  EXPECT_EQ(0x6U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x3U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x3U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x01, need);

  meta_mgr_->dump_plan();

  meta_mgr_->server_to_task_.clear();
  clear_task();
  
  //target server not found
  if (block != NULL)
  {
    std::vector<ServerCollect*> vec = block->get_hold();
    std::vector<ServerCollect*>::iterator iter = vec.begin();
    for (; iter != vec.end(); ++iter)
    {
      block->remove((*iter), now);
    }
  }

  ds_info.id_ = 0xfffffff0;
  for (int32_t i = 0; i < MAX_COUNT_SERVER; ++i)
  {
    ++ds_info.id_;
    meta_mgr_->remove_server(ds_info.id_, now);
  }

  need = 0x02;
  adjust = 0;
  emergency_replicate_count = 0;
  EXPECT_EQ(true, meta_mgr_->build_replicate_plan(current_plan_seqno, now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(false, meta_mgr_->find_block_in_plan(normal_block_id));
  EXPECT_EQ(false, meta_mgr_->find_server_in_plan(meta_mgr_->get_server(ds_info.id_)));
  EXPECT_EQ(0x0U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x0U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x0U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x02, need);
}

TEST_F(LayoutManagerTest, build_compact_plan)
{
  bool is_new = false;
  time_t now = time(NULL);
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;

  for (int32_t i = 0; i <  SYSPARAM_NAMESERVER.min_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  int64_t need = 0x02;
  uint32_t block_id = 0;
  BlockCollect* block = meta_mgr_->add_new_block(block_id);

  int current_plan_seqno = 1; 
  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(false, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(0x0U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x0U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x0U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x02, need);

  SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;//1/2
  BlockInfo info;
  memset(&info, 0, sizeof(info));
  info.block_id_ = block_id;
  block->update(info);

  info.del_size_ = SYSPARAM_NAMESERVER.max_block_size_ / 2;
  info.size_     = SYSPARAM_NAMESERVER.max_block_size_ + 1; 
  block->update(info);

  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(false, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(0x0U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x0U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x0U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x02, need);
 
  info.file_count_ = 0x01;
  info.del_file_count_ = 0x01;
  block->update(info);

  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x01U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x01U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x01, need);

  info.file_count_ = info.del_file_count_ * 2 + 1;// > 1/2
  block->update(info);

  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x01U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x01U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x01, need);

  clear_task();

  for (int32_t i =  SYSPARAM_NAMESERVER.min_replication_; i <  SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  block_id = 0;
  block = meta_mgr_->add_new_block(block_id);
  memset(&info, 0, sizeof(info));
  info.block_id_ = block_id;
  block->update(info);
  info.del_size_ = 0xff;
  info.size_     = info.del_size_ * 2; 
  info.del_file_count_ = 0x01;
  info.file_count_ = 0x01;
  block->update(info);
  need = 0x2;

  EXPECT_EQ(true, meta_mgr_->build_compact_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x01U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x01U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(0x01, need);
  clear_task();
}

TEST_F(LayoutManagerTest, build_balance_plan)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  bool is_new = false;
  time_t now = time(NULL);
  int32_t index = 0;
  const int32_t MAX_SERVER_COUNT = SYSPARAM_NAMESERVER.min_replication_;
  const int32_t MAX_BLOCK_COUNT  = 16;
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;

  TBSYS_LOG(DEBUG, "alive_server_size:%d", meta_mgr_->alive_server_size_);
  for (; index < MAX_SERVER_COUNT; ++index)
  {
    ++ds_info.id_;
    ds_info.current_load_ = ds_info.current_load_ * (index + 1);
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  std::vector<BlockCollect*> balance_blocks;

  BlockCollect* block = NULL;
  for (uint32_t block_id = 0; index < MAX_BLOCK_COUNT; ++index, block_id = 0, block = NULL)
  {
    block = meta_mgr_->add_new_block(block_id);
    if (block != NULL)
    {
      BlockInfo info;
      memset(&info, 0, sizeof(info));
      info.block_id_ = block_id;
      info.size_ = 0xffff + (index + 1) * 0xff;
      block->update(info);
      if (block->check_balance())
      {
        balance_blocks.push_back(block);
      }
    }
  }

  int current_plan_seqno = 1;
  std::vector<uint32_t> blocks;
  int64_t need = 0x02;
  EXPECT_EQ(true, meta_mgr_->build_balance_plan(current_plan_seqno, now, need, blocks));
  int32_t find = 0;
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (meta_mgr_->find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  EXPECT_TRUE(find == 0);
  EXPECT_EQ(0x02, need);

  ++ds_info.id_;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  EXPECT_EQ(true, meta_mgr_->build_balance_plan(current_plan_seqno, now, need, blocks));
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (meta_mgr_->find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  EXPECT_TRUE(find == 0x01);
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(meta_mgr_->get_server(ds_info.id_)));
  EXPECT_EQ(0x2U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x1U, meta_mgr_->block_to_task_.size());

  find = 0;
  ++ds_info.id_;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  EXPECT_EQ(true, meta_mgr_->build_balance_plan(current_plan_seqno, now, need, blocks));
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (meta_mgr_->find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  TBSYS_LOG(DEBUG, "balance_blocks.size(%u), find(%d)", balance_blocks.size(), find);
  EXPECT_TRUE(find == 0x02);
  EXPECT_EQ(0x00, need);
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(meta_mgr_->get_server(ds_info.id_)));
  EXPECT_EQ(0x4U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x2U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x2U, meta_mgr_->block_to_task_.size());

  clear_task();
}

TEST_F(LayoutManagerTest, build_redundant_plan)
{
  std::vector<uint32_t> blocks;
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;

  bool is_new = false;
  time_t now = time(NULL);
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;

  for (int32_t i = 0; i <  SYSPARAM_NAMESERVER.min_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  int current_plan_seqno = 1;
  int64_t need = 0x2;
  uint32_t block_id = 0;
  BlockCollect* block = meta_mgr_->add_new_block(block_id);
  EXPECT_EQ(true, meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(0x02, need);

  for (int32_t i = SYSPARAM_NAMESERVER.min_replication_; i <  SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }
  block_id = 0;
  block = meta_mgr_->add_new_block(block_id);
  EXPECT_EQ(true, meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(0x02, need);

  need = 0;
  EXPECT_EQ(true, meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(0x00, need);

  ++ds_info.id_;
  meta_mgr_->add_server(ds_info, now, is_new);
  ServerCollect* server = meta_mgr_->get_server(ds_info.id_);

  bool master = false;
  block->add(server, now, false, master);
  server->add(block, false);

  need = 0x02;
  EXPECT_EQ(true, meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x1U, meta_mgr_->block_to_task_.size());

  EXPECT_EQ(true, meta_mgr_->build_redundant_plan(current_plan_seqno, now, need, blocks));
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, meta_mgr_->server_to_task_.size());
  EXPECT_EQ(0x1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(0x1U, meta_mgr_->block_to_task_.size());

  clear_task();
}

TEST_F(LayoutManagerTest, add_task)
{
  PlanPriority priority = PLAN_PRIORITY_NORMAL;
  std::vector<ServerCollect*> runer;

  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffff0;
  time_t now = time(NULL);
  bool is_new = false;
  uint32_t block_id = 100;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* source_server = meta_mgr_->get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* dest_server = meta_mgr_->get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  LayoutManager::ReplicateTaskPtr replicate_task = new LayoutManager::ReplicateTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(replicate_task);
  EXPECT_EQ(1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(1U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(2U, meta_mgr_->server_to_task_.size());

  meta_mgr_->pending_plan_list_.clear();
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  LayoutManager::MoveTaskPtr move_task = new LayoutManager::MoveTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(move_task);
  EXPECT_EQ(1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(1U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(2U, meta_mgr_->server_to_task_.size());

  meta_mgr_->pending_plan_list_.clear();
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  LayoutManager::DeleteBlockTaskPtr redundant_task = new LayoutManager::DeleteBlockTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(redundant_task);
  EXPECT_EQ(1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(1U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(2U, meta_mgr_->server_to_task_.size());

  meta_mgr_->pending_plan_list_.clear();
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  LayoutManager::CompactTaskPtr compact_task = new LayoutManager::CompactTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(compact_task);
  EXPECT_EQ(1U, meta_mgr_->pending_plan_list_.size());
  EXPECT_EQ(1U, meta_mgr_->block_to_task_.size());
  EXPECT_EQ(2U, meta_mgr_->server_to_task_.size());
  clear_task();
}

TEST_F(LayoutManagerTest, remove_task)
{

}

TEST_F(LayoutManagerTest, find_block_in_plan)
{
  PlanPriority priority = PLAN_PRIORITY_NORMAL;
  std::vector<ServerCollect*> runer;

  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffff0;
  time_t now = time(NULL);
  bool is_new = false;
  uint32_t block_id = 100;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* source_server = meta_mgr_->get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* dest_server = meta_mgr_->get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  LayoutManager::ReplicateTaskPtr replicate_task = new LayoutManager::ReplicateTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(replicate_task);
  EXPECT_EQ(true, meta_mgr_->find_block_in_plan(block_id));

  clear_task();
}

TEST_F(LayoutManagerTest, find_server_in_plan)
{
  PlanPriority priority = PLAN_PRIORITY_NORMAL;
  std::vector<ServerCollect*> runer;

  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffff0;
  time_t now = time(NULL);
  bool is_new = false;
  uint32_t block_id = 100;
  
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* source_server = meta_mgr_->get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(info, now, is_new));
  ServerCollect* dest_server = meta_mgr_->get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  LayoutManager::ReplicateTaskPtr replicate_task = new LayoutManager::ReplicateTask(meta_mgr_, priority, block_id, now, now + 10800, runer, 0);
  meta_mgr_->add_task(replicate_task);
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(source_server));
  EXPECT_EQ(true, meta_mgr_->find_server_in_plan(dest_server));

  clear_task();
}

TEST_F(LayoutManagerTest, expire)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.owner_role_ = NS_ROLE_MASTER;
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;

  // build replicate plan
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 100;
  ds_info.total_capacity_ = 0xffffff;
  std::vector<ServerCollect*> runer;
  bool is_new = false;
  time_t now = time(NULL);
  uint32_t block_id = 0;

  for(int32_t i = 0; i < 2; i++)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }

  block_id = 0;
  meta_mgr_->add_new_block(block_id);//normal replicate

  ds_info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));

  EXPECT_EQ(0, meta_mgr_->build_plan());

  // handle replicate task complete
  ReplicateBlockMessage replicate_msg;
  ReplBlock repl_block;
  memset(&repl_block, 0, sizeof(repl_block));
  repl_block.block_id_ = block_id;
  repl_block.source_id_ = ds_info.id_ - 2; 
  repl_block.destination_id_ = ds_info.id_; 
  repl_block.start_time_ = now;
  repl_block.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_NO;
  repl_block.server_count_ = 0;
  replicate_msg.set_repl_block(&repl_block);
  replicate_msg.set_command(PLAN_STATUS_END);
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->handle_task_complete(&replicate_msg));

  EXPECT_EQ(1U, meta_mgr_->finish_plan_list_.size());
  EXPECT_EQ(true, meta_mgr_->expire());
  EXPECT_EQ(0U, meta_mgr_->finish_plan_list_.size());
}


TEST_F(LayoutManagerTest, scan)
{
  const int32_t TRY_TIMES= 5;

  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);

  //scan all server info
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.id_ = 0xfffffff0;
  ds_info.current_load_ = 90;
  ds_info.total_capacity_ = 0xffffff;
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  bool is_new = false;
  time_t now = time(NULL);

  int32_t i = 0;
  int32_t server_count = 7;
  for(i = 0; i < server_count; i++)
  {
    ds_info.id_++;
    EXPECT_EQ(TFS_SUCCESS, meta_mgr_->add_server(ds_info, now, is_new));
  }
  ServerCollect* server_collect = meta_mgr_->get_server(ds_info.id_);
  server_collect->dead();

  uint32_t block_id = 0;
  const int32_t block_count = 29;
  // blockid: 129:158
  for(i = 0; i < block_count; i++)
  {
    block_id = 0;
    meta_mgr_->add_new_block(block_id);
  }

  SSMScanParameter param;
  memset(&param, 0, sizeof(param));

  // get servers info in one time
  param.type_ = SSM_TYPE_SERVER;
  param.child_type_ = SSM_CHILD_SERVER_TYPE_ALL;

  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->scan(param));
  EXPECT_EQ(SSM_SCAN_END_FLAG_YES, (param.end_flag_ >> 4) & 0x0f);                                                           
  EXPECT_EQ(static_cast<uint32_t>(server_count - 1), param.should_actual_count_ & 0x0ffff);

  // get servers info in batches
  // change the num each time
  int32_t count = 0;
  for (i = 1; i < TRY_TIMES; i++)
  {
    count = 0;
    memset(&param, 0, sizeof(param));
    param.type_ = SSM_TYPE_SERVER;
    param.child_type_ = SSM_CHILD_SERVER_TYPE_ALL;
    param.should_actual_count_ = (i << 16);
    while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
    {
      EXPECT_EQ(TFS_SUCCESS, meta_mgr_->scan(param));
      param.start_next_position_ = (param.start_next_position_ << 16) & 0xffff0000;                                          
      count += (param.should_actual_count_ & 0x0ffff);
    }
    EXPECT_EQ(server_count - 1, count);
  }

  // get blocks info in one time
  memset(&param, 0, sizeof(param));
  param.type_ = SSM_TYPE_BLOCK;
  param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;
  param.start_next_position_ = 0x0;
  param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
  EXPECT_EQ(TFS_SUCCESS, meta_mgr_->scan(param));
  EXPECT_EQ(static_cast<uint32_t>(block_count), param.should_actual_count_ & 0x0ffff);
  EXPECT_EQ(SSM_SCAN_END_FLAG_YES, (param.end_flag_ >> 4) & 0x0f); 

  // get all blocks in batches
  for (i = 1; i < TRY_TIMES; i++)
  {
    memset(&param, 0, sizeof(param));
    param.type_ = SSM_TYPE_BLOCK;
    param.child_type_ = SSM_CHILD_BLOCK_TYPE_INFO | SSM_CHILD_BLOCK_TYPE_SERVER;
    param.start_next_position_ = 0x0;
    param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;
    param.should_actual_count_ = (i << 16);
    count = 0;
    while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
    {
      EXPECT_EQ(TFS_SUCCESS, meta_mgr_->scan(param));
      if (param.end_flag_ & SSM_SCAN_CUTOVER_FLAG_NO)
      {
        param.addition_param1_ = param.addition_param2_;
      }
      param.start_next_position_ = (param.start_next_position_ << 16) & 0xffff0000;                                          
      count += (param.should_actual_count_ & 0x0ffff);
    }
    EXPECT_EQ(block_count, count);
  }
}

TEST_F(LayoutManagerTest, set_runtime_param)
{
  NsGlobalStatisticsInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  SYSPARAM_NAMESERVER.min_replication_ = 2;
  SYSPARAM_NAMESERVER.max_replication_ = 4;
  SYSPARAM_NAMESERVER.max_write_file_count_ = 10;
  SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
  SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
  SYSPARAM_NAMESERVER.heart_interval_ = 2;
  SYSPARAM_NAMESERVER.replicate_wait_time_ = 10;
  SYSPARAM_NAMESERVER.compact_max_load_ = 10;
  SYSPARAM_NAMESERVER.cluster_index_ = 2;
  meta_mgr_->plan_run_flag_ = 0x02;
  SYSPARAM_NAMESERVER.run_plan_expire_interval_= 120;
  SYSPARAM_NAMESERVER.run_plan_ratio_ = 10;
  SYSPARAM_NAMESERVER.object_dead_max_time_ = 20;

  uint32_t index = 0;
  uint32_t value = 0;
  char msg[0xff];
  memset(msg, 0, sizeof(msg));

  index = 0x000000001;
  index |= 0x10000000;
  value = 0x03;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0x03, SYSPARAM_NAMESERVER.min_replication_);

  index = 0x000000002;
  index |= 0x10000000;
  value = 0x04;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0x04, SYSPARAM_NAMESERVER.max_replication_);

  index = 0x000000003;
  index |= 0x10000000;
  value = 0xff;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0xff, SYSPARAM_NAMESERVER.max_write_file_count_);

  index = 0x000000004;
  index |= 0x10000000;
  value = 0xff;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0xff, SYSPARAM_NAMESERVER.max_use_capacity_ratio_);

  index = 0x000000009;
  index |= 0x10000000;
  value = 0x4;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0x4, meta_mgr_->plan_run_flag_); 

  index = 0x00000000A;
  index |= 0x10000000;
  value = 0xab;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0xab, SYSPARAM_NAMESERVER.run_plan_expire_interval_); 

  index = 0x00000000B;
  index |= 0x10000000;
  value = 100;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(100, SYSPARAM_NAMESERVER.run_plan_ratio_); 

  index = 0x00000000C;
  index |= 0x10000000;
  value = 0xab;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0xab, SYSPARAM_NAMESERVER.object_dead_max_time_); 

  index = 0x000000014;
  index |= 0x10000000;
  value = 0x3;
  meta_mgr_->set_runtime_param(index, value, msg);
  EXPECT_EQ(0x3, SYSPARAM_NAMESERVER.cluster_index_);

}

void LayoutManagerTest::clear_task()
{
  std::map<uint32_t, LayoutManager::TaskPtr>::iterator iter = meta_mgr_->block_to_task_.begin();
  for (; iter != meta_mgr_->block_to_task_.end(); ++iter)
  {
    GFactory::get_timer()->cancel((iter->second));
  }
  meta_mgr_->pending_plan_list_.clear();
  meta_mgr_->block_to_task_.clear();
  meta_mgr_->server_to_task_.clear();
  meta_mgr_->running_plan_list_.clear();
  meta_mgr_->finish_plan_list_.clear();
}

void LayoutManagerTest::clear()
{
  SERVER_MAP::iterator iter = meta_mgr_->servers_.begin();
  for(; iter != meta_mgr_->servers_.end(); ++iter)
  {
    tbsys::gDelete(iter->second);
  }
  meta_mgr_->servers_.clear();
  meta_mgr_->servers_index_.clear();
  meta_mgr_->alive_server_size_ = 0;
  clear_task();
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

