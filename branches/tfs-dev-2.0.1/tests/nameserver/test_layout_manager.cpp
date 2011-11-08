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
#include "layout_manager.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include <vector>
#include <bitset>

using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

class LayoutManagerTest: public virtual ::testing::Test,
                         public virtual LayoutManager
{
public:
  LayoutManagerTest()
  {

  }
  ~LayoutManagerTest()
  {
  }
  void clear_task();
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
    SYSPARAM_NAMESERVER.run_plan_ratio_ = 100;
    SYSPARAM_NAMESERVER.build_plan_interval_ = 1;
    initialize(4);
  }
  virtual void TearDown()
  {
    destroy();
    wait_for_shut_down();
    clear_task();
  }
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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);

  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(true, server->id() == info.id_);

  if (server != NULL)
    server->dead();

  ServerCollect* server2 = get_server(info.id_);
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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);

  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);

  ServerCollect* server = get_server(info.id_);
  int32_t current_load = server->load(); 
  EXPECT_EQ(true, server != NULL);
  if (server != NULL)
    server->dead();

  info.current_load_ = 100;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);

  ServerCollect* server2 = get_server(info.id_);
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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
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

  EXPECT_EQ(TFS_ERROR, update_relation(NULL, blocks, expires, now));

  server->status_= DATASERVER_STATUS_DEAD;
  EXPECT_EQ(TFS_ERROR, update_relation(server, blocks, expires, now));

  server->status_= DATASERVER_STATUS_ALIVE;

  EXPECT_EQ(TFS_SUCCESS, update_relation(server, blocks, expires, now));
  
  EXPECT_EQ(count, server->block_count());

  EXPECT_EQ(TFS_SUCCESS, remove_server(info.id_, now));

  EXPECT_EQ(0, server->block_count());
  EXPECT_EQ(DATASERVER_STATUS_DEAD, server->status_);
  EXPECT_EQ(0, alive_server_size_);

  for (int i = 0; i < count; ++i)
  {
    BlockChunkPtr ptr = get_chunk(100 + i);
    BlockCollect* block = ptr->find(100 + i);
    EXPECT_EQ(true, block != NULL);
    
    EXPECT_EQ(false, block->exist(server));
    EXPECT_EQ(0, block->get_hold_size());
  }
}

TEST_F(LayoutManagerTest, add_block) 
{
  uint32_t block_id = 0;
  EXPECT_EQ(true, add_block(block_id) != NULL);
  EXPECT_EQ(static_cast<uint32_t>(SKIP_BLOCK + 1), block_id);

  EXPECT_EQ(false, add_block(block_id) != NULL);
  EXPECT_EQ(static_cast<uint32_t>(SKIP_BLOCK + 1), block_id);
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
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  }
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  uint32_t block_id = 0;
  
  EXPECT_EQ(true, add_new_block(block_id, server) != NULL);
}

TEST_F(LayoutManagerTest, update_relation)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
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

  EXPECT_EQ(TFS_ERROR, update_relation(NULL, blocks, expires, now));

  server->status_= DATASERVER_STATUS_DEAD;
  EXPECT_EQ(TFS_ERROR, update_relation(server, blocks, expires, now));

  server->status_= DATASERVER_STATUS_ALIVE;

  EXPECT_EQ(TFS_SUCCESS, update_relation(server, blocks, expires, now));
  
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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  uint32_t block_id = 100;
  BlockCollect* block = add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());

  EXPECT_EQ(TFS_ERROR, build_relation(NULL, NULL, now, force));
  EXPECT_EQ(TFS_ERROR, build_relation(NULL, server, now, force));
  EXPECT_EQ(TFS_ERROR, build_relation(block, NULL, now, force));
  server->dead();
  EXPECT_EQ(TFS_ERROR, build_relation(block, NULL, now, force));

  server->status_ = DATASERVER_STATUS_ALIVE;
  
  EXPECT_EQ(TFS_SUCCESS, build_relation(block, server, now, force));
  force = true;
  EXPECT_EQ(TFS_SUCCESS, build_relation(block, server, now, force));
}

TEST_F(LayoutManagerTest, relieve_relation_all)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool is_new = false;
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  bool master = false;
  uint32_t block_id = 100;
  BlockCollect* block = add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  ++block_id; 
  block = add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  ++block_id; 
  block = add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  EXPECT_EQ(0x03, server->block_count());

  EXPECT_EQ(true, relieve_relation(server, now));

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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  bool master = false;
  uint32_t block_id = 100;
  BlockCollect* block = add_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());
  EXPECT_EQ(true, server->add(block, master));

  EXPECT_EQ(0x01, server->block_count());

  EXPECT_EQ(true, relieve_relation(block, server, now));

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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);
  EXPECT_EQ(info.id_, server->id());

  BlockInfo binfo;
  memset(&binfo, 0, sizeof(binfo));
  binfo.block_id_ = 1000;

  bool add_new = true;
  EXPECT_EQ(TFS_SUCCESS, update_block_info(binfo, server->id(), now, add_new));

  BlockCollect* block = get_block(binfo.block_id_);
  EXPECT_TRUE(block != NULL);
  EXPECT_EQ(binfo.version_, block->version());

  add_new = false;
  binfo.version_ += 10;
  EXPECT_EQ(TFS_SUCCESS, update_block_info(binfo, server->id(), now, add_new));
  block = get_block(binfo.block_id_);
  EXPECT_TRUE(block != NULL);
  EXPECT_EQ(binfo.version_, block->version());

  binfo.version_ -= 5;
  EXPECT_EQ(EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR, update_block_info(binfo, server->id(), now, add_new));

  BlockChunkPtr ptr = get_chunk(binfo.block_id_);
  ptr->remove(block->id());

  binfo.version_ += 20;
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, update_block_info(binfo, server->id(), now, add_new));
}

TEST_F(LayoutManagerTest, rotate)
{

}

TEST_F(LayoutManagerTest, get_alive_block_id)
{
  EXPECT_EQ(static_cast<uint32_t>(SKIP_BLOCK + 1), get_alive_block_id());
  atomic_add(&max_block_id_, 100);
  EXPECT_EQ(static_cast<uint32_t>(SKIP_BLOCK + 1  + 1+ 100), get_alive_block_id());
}

TEST_F(LayoutManagerTest, calc_max_block_id)
{
  EXPECT_EQ(static_cast<uint32_t>(SKIP_BLOCK), calc_max_block_id());
}

TEST_F(LayoutManagerTest, calc_all_block_bytes)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }
  for(i = 0; i < 10; i++)
  {
    block_id = 0;
    BlockCollect* block_collect = add_new_block(block_id);
    BlockInfo block_info;
    block_info.block_id_ = block_id;
    block_info.size_ = 100;
    block_collect->update(block_info);
  }
  EXPECT_EQ(1000, calc_all_block_bytes());
  
}

TEST_F(LayoutManagerTest, calc_all_block_count)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }
  for(i = 0; i < 10; i++)
  {
    block_id = 0;
    add_new_block(block_id);
  }
  EXPECT_EQ(10, calc_all_block_count());
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
  bool force = false;
  bool master = false;

  EXPECT_EQ(true, elect_write_block() == NULL);

  const int32_t count = 5;
  for (int i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xffffff + i;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    EXPECT_EQ((i + 1), alive_server_size_);
  }

  uint32_t block_base = 0;
  const int32_t block_count = 20;
  for (int i = 0; i < block_count; ++i)
  {
    block_base = 0;
    BlockCollect* block = add_new_block(block_base);
    if (block != NULL)
    {
      TBSYS_LOG(DEBUG, "block(%u), size(%d)", block_base, block->get_hold_size());
    }
  } 
  uint64_t id = 10000;
  for (int i = 0; i < count; ++i)
  {
    id++;
    ServerCollect* server = get_server(id);
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
    BlockCollect* block = elect_write_block();
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
    BlockCollect* block = elect_write_block();
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

TEST_F(LayoutManagerTest, open_read_mode)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  bool force = false;
  bool master = false;
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(1, alive_server_size_);
  ServerCollect* server = get_server(info.id_);
  EXPECT_EQ(true, server != NULL);

  uint32_t block_id = 0;
  std::vector<uint64_t> vec;
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, open_read_mode(block_id, vec)); 

  block_id = 100;
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, open_read_mode(block_id, vec)); 

  EXPECT_EQ(true, add_block(block_id) != NULL);
  BlockCollect* block = get_block(block_id);
  EXPECT_EQ(true, block != NULL);
  EXPECT_EQ(block_id, block->id());

  EXPECT_EQ(EXIT_NO_DATASERVER, open_read_mode(block_id, vec)); 
  EXPECT_EQ(0U, vec.size());

  block->add(server, now, force, master);
  EXPECT_EQ(true, master);

  EXPECT_EQ(TFS_SUCCESS, open_read_mode(block_id, vec)); 
  EXPECT_EQ(1U, vec.size());

  vec.clear();
  master = false;
  info.id_ += 100;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  EXPECT_EQ(2, alive_server_size_);
  ServerCollect* server2 = get_server(info.id_);
  EXPECT_EQ(true, server2 != NULL);
  block->add(server2, now, force, master);
  EXPECT_EQ(false, master);

  EXPECT_EQ(TFS_SUCCESS, open_read_mode(block_id, vec)); 
  EXPECT_EQ(2U, vec.size());

  server2->status_ = DATASERVER_STATUS_DEAD;
  server2->current_load_ = 0;

  server->current_load_ = 100;

  vec.clear();
  EXPECT_EQ(TFS_SUCCESS, open_read_mode(block_id, vec)); 
  EXPECT_EQ(1U, vec.size());
  EXPECT_EQ(10000U, vec[0]);
}

TEST_F(LayoutManagerTest, open_write_mode)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
 
  uint32_t block_id = 0;
  uint32_t lease_id = 0;
  int32_t mode = T_WRITE | T_CREATE;
  int32_t version = 0;
  std::vector<uint64_t> vec;

  // no blocks
  EXPECT_EQ(EXIT_NO_BLOCK, open_write_mode(mode, block_id, lease_id, version, vec));
  EXPECT_EQ(0U, block_id);

  const int32_t count = 5;
  for (int i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xffffff + i;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    EXPECT_EQ((i + 1), alive_server_size_);
  }
  uint32_t block_base = 0;
  const int32_t block_count = 20;
  for (int i = 0; i < block_count; ++i)
  {
    block_base = 0;
    BlockCollect* block = add_new_block(block_base);
    if (block != NULL)
    {
      TBSYS_LOG(DEBUG, "block(%u), size(%d)", block_base, block->get_hold_size());
    }
  } 
  // create block
  block_id = 0;
  vec.clear();
  EXPECT_EQ(TFS_SUCCESS, open_write_mode(mode, block_id, lease_id, version, vec));
  //EXPECT_EQ(129, block_id);
  EXPECT_EQ(4U,vec.size());
  // elect block success, mode error
  mode = T_CREATE;
  block_id = 0;
  vec.clear();
  EXPECT_EQ(EXIT_ACCESS_MODE_ERROR, open_write_mode(mode, block_id,lease_id, version, vec));
  //EXPECT_EQ(129, block_id);
  EXPECT_EQ(0U, vec.size());

  // new block
  mode = T_WRITE | T_NEWBLK;
  block_id = 200;
  vec.clear();
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open_write_mode(mode, block_id, lease_id, version, vec));
  EXPECT_EQ(200U, block_id);
  EXPECT_EQ(4U,vec.size());

  // new block which is exist
  vec.clear();
  block_id = 129;
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open_write_mode(mode, block_id, lease_id, version, vec));
  // block has no ds when new block
  vec.clear();
  block_id = 129;
  BlockCollect* block_collect = get_block(block_id);
  block_collect->hold_.clear();
  block_collect->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_NO);
  EXPECT_EQ(EXIT_NO_DATASERVER, open_write_mode(mode, block_id, lease_id, version, vec));
  block_collect->set_create_flag(BlockCollect::BLOCK_CREATE_FLAG_YES);
  EXPECT_EQ(EXIT_NO_BLOCK, open_write_mode(mode, block_id, lease_id, version, vec));

  // add new block error when new block
  vec.clear();
  block_id = 300;
  for(int i = 0; i < count; i++)
  {
    SERVER_MAP::iterator iter = servers_.find(info.id_--);
    if (iter != servers_.end())
    {    
      iter->second->dead();
    }
  }
  EXPECT_EQ(EXIT_NO_DATASERVER, open_write_mode(mode, block_id, lease_id, version, vec));

  // block not found in write mode
  mode = T_WRITE;
  block_id = 0;
  EXPECT_EQ(EXIT_NO_BLOCK, open_write_mode(mode, block_id, lease_id, version, vec));

  // block has no ds
  mode = T_WRITE;
  block_id = 129;
  EXPECT_EQ(EXIT_NO_DATASERVER, open_write_mode(mode, block_id, lease_id, version, vec));

  // all ds dead
  block_id = 200;
  vec.clear();
  EXPECT_EQ(EXIT_NO_DATASERVER, open_write_mode(mode, block_id, lease_id, version, vec));

  //mode |= T_READ;

  //block_id = 0;
  //EXPECT_EQ(TFS_SUCCESS, open_write_mode(block_id, mode, lease_id, version, vec));
  //EXPECT_EQ(133, block_id);
}
TEST_F(LayoutManagerTest, open)
{
  NsGlobalInfo tmp;
  memset(&tmp, 0, sizeof(tmp));
  GFactory::get_global_info().update(tmp);
  GFactory::get_global_info().elect_seq_num_ = 0;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
 
  uint32_t block_id = 0;
  uint32_t lease_id = 0;
  int32_t mode = T_READ;
  int32_t version = 0;
  std::vector<uint64_t> ds_list;
  EXPECT_EQ(EXIT_NO_DATASERVER, open(block_id, mode, lease_id, version, ds_list));

  const int32_t count = 3;
  for (int i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xffffff + i;
    info.current_load_ = 100;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    EXPECT_EQ((i + 1), alive_server_size_);
  }

  add_new_block(block_id);
  
  EXPECT_EQ(129U, block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  EXPECT_EQ(static_cast<uint32_t>(count), ds_list.size());
  GFactory::get_lease_factory().cancel(block_id);

  mode = T_WRITE | T_CREATE;
  block_id = 0;
  ds_list.clear();
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  EXPECT_EQ(static_cast<uint32_t>(count), ds_list.size());

  block_id = 200;
  ds_list.clear();
  EXPECT_EQ(EXIT_NO_BLOCK, open(block_id, mode, lease_id, version, ds_list));
  EXPECT_EQ(0U, ds_list.size());

  ds_list.clear();
  PlanPriority priority;
  std::vector<ServerCollect*> runer;
  ReplicateTaskPtr task = new ReplicateTask(this, priority, block_id, now, now + 10800, runer);
  add_task(task);
  EXPECT_EQ(EXIT_BLOCK_BUSY, open(block_id, mode, lease_id, version, ds_list));
  clear_task();
}

TEST_F(LayoutManagerTest, close)
{
  NsGlobalInfo tmp;
  memset(&tmp, 0, sizeof(tmp));
  GFactory::get_global_info().update(tmp);
  GFactory::get_global_info().elect_seq_num_ = 0;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
 
  uint32_t block_id = 0;
  uint32_t lease_id = 0;
  int32_t mode = T_WRITE | T_CREATE;
  int32_t version = 0;
  std::vector<uint64_t> ds_list;

  const int32_t count = 3;
  for (int i = 0; i < count; ++i)
  {
    info.id_++;
    info.total_capacity_ = 0xffffff + i;
    info.current_load_ = 100;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    EXPECT_EQ((i + 1), alive_server_size_);
  }

  add_new_block(block_id);
  GFactory::get_lease_factory().cancel(block_id);

  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));

  BlockInfo block_info;
  memset(&block_info, 0, sizeof(block_info));
  block_info.block_id_ =  block_id;
  block_info.version_ = version + 1;
  block_info.size_ = 10;

  CloseParameter parameter;
  memset(&parameter, 0, sizeof(parameter));
  parameter.block_info_ = block_info;
  parameter.status_ = WRITE_COMPLETE_STATUS_YES;
  parameter.unlink_flag_ = UNLINK_FLAG_NO;
  parameter.id_ = ds_list[0];
  parameter.lease_id_ = lease_id;
  parameter.need_new_ = false;

  EXPECT_EQ(TFS_SUCCESS, close(parameter));

  // block not exist
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.block_info_.block_id_ =  block_id + 1;
  parameter.lease_id_ = lease_id;
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, close(parameter));

  // write status error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.block_info_.block_id_ =  block_id;
  parameter.lease_id_ = lease_id;
  parameter.status_ = WRITE_COMPLETE_STATUS_NO;
  EXPECT_EQ(TFS_SUCCESS, close(parameter));
  // write status commit error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.lease_id_ = lease_id + 1;
  EXPECT_EQ(EXIT_COMMIT_ERROR, close(parameter));

  // write operation error by version
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  block_info.version_ = version;
  parameter.status_ = WRITE_COMPLETE_STATUS_YES;
  parameter.lease_id_ = lease_id;
  EXPECT_EQ(EXIT_COMMIT_ERROR, close(parameter));
  // write operation error by vesion and commit error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.lease_id_ = lease_id + 1;
  EXPECT_EQ(EXIT_COMMIT_ERROR, close(parameter));

  // block is full
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.lease_id_ = lease_id;
  parameter.block_info_.size_ = 100;
  parameter.block_info_.version_ = version + 1;
  EXPECT_EQ(false,  parameter.need_new_);
  EXPECT_EQ(TFS_SUCCESS, close(parameter));
  EXPECT_EQ(true,  parameter.need_new_);

  // block is  full and commit error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.lease_id_ = lease_id + 1;
  parameter.block_info_.version_ = version + 1;
  EXPECT_EQ(EXIT_COMMIT_ERROR, close(parameter));

  // block is not full, commit error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.need_new_ = false;
  parameter.lease_id_ = lease_id + 1;
  parameter.block_info_.size_ = 10;
  parameter.block_info_.version_ = version + 1;
  EXPECT_EQ(EXIT_COMMIT_ERROR, close(parameter));
  EXPECT_EQ(false,  parameter.need_new_);

  //unlink flag, update info error
  GFactory::get_lease_factory().cancel(block_id);
  EXPECT_EQ(TFS_SUCCESS, open(block_id, mode, lease_id, version, ds_list));
  parameter.lease_id_ = lease_id;
  parameter.block_info_.size_ = 10;
  parameter.block_info_.version_ = version + 1;
  parameter.unlink_flag_ = UNLINK_FLAG_YES;
  EXPECT_EQ(TFS_SUCCESS, close(parameter));
}
TEST_F(LayoutManagerTest, keep_alive) 
{
  HasBlockFlag flag = HAS_BLOCK_FLAG_YES;
  BLOCK_INFO_LIST block_list;
  VUINT32 expires;
  bool need_sent_block = false;
  DataServerStatInfo ds_info; 
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.id_ = 0xfffffff0;
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.total_capacity_ = 0xffffff;
  ds_info.use_capacity_ = 1024;
  ds_info.block_count_ = 100;
  time_t now = time(NULL);
  bool is_new = false;
 
  std::vector<uint64_t> ds_list;

  const int32_t count = 3;
  for (int i = 0; i < count; ++i)
  {
    ds_info.id_++;
    ds_info.total_capacity_ = 0xffffff + i;
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
    EXPECT_EQ((i + 1), alive_server_size_);
  }

  uint32_t block_base = 0;
  const int32_t block_count = 10;
  BlockInfo block_info;
  for (int i = 0; i < block_count; ++i)
  {
    block_base = 0;
    BlockCollect* block = add_new_block(block_base);
    if (block != NULL)
    {
      TBSYS_LOG(DEBUG, "block(%u), size(%d)", block_base, block->get_hold_size());
    }
    memset(&block_info, 0, sizeof(block_info));
    block_info.block_id_ = block->id();
    block_info.size_ = block->size();
    block_info.version_ = block->version();
    block_list.push_back(block_info);
  } 

  // check an alive existed server
  TBSYS_LOG(DEBUG, "existed server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  ServerCollect* server = get_server(ds_info.id_);
  EXPECT_EQ(true, server->is_alive());
  // has no block
  flag = HAS_BLOCK_FLAG_NO;
  TBSYS_LOG(DEBUG, "existed server,has no block (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  server = get_server(ds_info.id_);
  EXPECT_EQ(true, server->is_alive());
  EXPECT_EQ(false, need_sent_block);

  // check a dead server
  flag = HAS_BLOCK_FLAG_YES;
  server = get_server(ds_info.id_);
  ds_info.status_ = DATASERVER_STATUS_DEAD;
  server->update(ds_info, now);
  TBSYS_LOG(DEBUG, "dead server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  server = get_server(ds_info.id_);
  EXPECT_EQ(true, server == NULL);
  EXPECT_EQ(count - 1, alive_server_size_);

  // check a new server & has no block
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  flag = HAS_BLOCK_FLAG_NO;
  TBSYS_LOG(DEBUG, "new server has no block (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  server = get_server(ds_info.id_);
  EXPECT_EQ(true, server->is_alive());
  EXPECT_EQ(count, alive_server_size_);
  EXPECT_EQ(true, need_sent_block);

  // check a new server & has block
  flag = HAS_BLOCK_FLAG_YES;
  need_sent_block = false;
  ds_info.id_++;
  TBSYS_LOG(DEBUG, "new server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  server = get_server(ds_info.id_);
  EXPECT_EQ(true, server->is_alive());
  EXPECT_EQ(count + 1, alive_server_size_);
  EXPECT_EQ(false, need_sent_block);

  // role is master, server no1 has two new version block
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.owner_role_ = NS_ROLE_MASTER; 
  ds_info.id_ = 0xfffffff1;
  server = get_server(ds_info.id_);
  EXPECT_EQ(10, server->block_count()); 
  block_list[0].version_ = 3;
  block_list[1].version_ = 3;
  TBSYS_LOG(DEBUG, "master server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
  EXPECT_EQ(TFS_SUCCESS, keepalive(ds_info, flag, block_list, expires, need_sent_block)); 
  server = get_server(ds_info.id_);
  EXPECT_EQ(10, server->block_count()); 
  server = get_server(ds_info.id_ +1);
  EXPECT_EQ(8, server->block_count()); 
  server = get_server(ds_info.id_ + 2);
  EXPECT_EQ(0, server->block_count());
  server = get_server(ds_info.id_ + 3);
  EXPECT_EQ(8, server->block_count());
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
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  ServerCollect* server_collect = get_server(ds_info.id_);

  EXPECT_EQ(TFS_SUCCESS, touch(ds_info.id_ + 1, now, false));
  EXPECT_EQ(TFS_SUCCESS, touch(ds_info.id_, now, false));
  EXPECT_EQ(0, server_collect->block_count());

  // master
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.owner_role_ = NS_ROLE_MASTER; 
  EXPECT_EQ(TFS_SUCCESS, touch(ds_info.id_, now, false));
  EXPECT_EQ(SYSPARAM_NAMESERVER.add_primary_block_count_, server_collect->block_count());
}

TEST_F(LayoutManagerTest, check_server)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
    ServerCollect* server = get_server(ds_info.id_);
    server->touch(time(NULL) + 10800);
    EXPECT_EQ((i + 1), alive_server_size_);
    if(i == 2 || i == 4)
    {
      server->touch(time(NULL) - 3600);
      TBSYS_LOG(DEBUG, "dead server (%s)", tbsys::CNetUtil::addrToString(ds_info.id_).c_str());
    }
  }
  EXPECT_EQ(6, servers_.size());
  check_server();
  EXPECT_EQ(4, servers_.size());
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
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, repair(block_id, server_id, flag, now, error_msg));

  // server not find
  BlockCollect* block_collect = add_block(block_id);
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, repair(block_id, server_id, flag, now, error_msg));

  int32_t i = 0;
  for(i = 0; i < 2; i++)
  {
    ds_info.id_ ++;
    ds_info.current_load_ = 1000;
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  ServerCollect* server_collect = get_server(ds_info.id_ - 1);
  block_collect->add(server_collect, now, false, master);
  server_collect->add(block_collect, false);

  // dest server is owner
  server_id = ds_info.id_ - 1;
  EXPECT_EQ(EXIT_NO_DATASERVER, repair(block_id, server_id, flag, now, error_msg));

  // emergency replicate
  server_id = ds_info.id_;
  EXPECT_EQ(STATUS_MESSAGE_REMOVE, repair(block_id, server_id, flag, now, error_msg));
  EXPECT_EQ(true, find_block_in_plan(block_id));
  EXPECT_EQ(true, find_server_in_plan(server_collect));
  //server_collect = get_server(server_id);
  //EXPECT_EQ(true, find_server_in_plan(server_collect));

  // server count >= min_replicate
  server_collect = get_server(server_id);
  block_collect->add(server_collect, now, false, master);
  server_collect->add(block_collect, false);
  EXPECT_EQ(EXIT_BLOCK_NOT_FOUND, repair(block_id, server_id, flag, now, error_msg));

  clear_task();
}

TEST_F(LayoutManagerTest, build_plan)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  ngi.owner_role_   = NS_ROLE_MASTER;

  bool is_new = false;
  time_t now = time(NULL);
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now)); //replicate && balance source server
  }
  const int32_t MAX_REPLICATE_BLOCK = 0x02;
  for (i = 0; i < MAX_REPLICATE_BLOCK; ++i) //replicate block
  {
    block_id = 0;
    add_new_block(block_id);
  }

  //add new server
  ds_info.id_ ++;
  ds_info.current_load_ = ds_info.current_load_ * (SYSPARAM_NAMESERVER.min_replication_ + 1);
  const int32_t MAX_BALANCE_BLOCK = 0x02;//balance
  for (block_id = 0, i = 0; i < MAX_BALANCE_BLOCK; i++)
  {
    block_id = 0;
    BlockCollect* block = add_new_block(block_id);
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now)); 
  } 

  const int32_t MAX_COMPACT_BLOCK_COUNT = 0x2;//compact
  for (block_id = 0, i = 0; i < MAX_COMPACT_BLOCK_COUNT; i++)
  {
    block_id = 0;
    BlockCollect* block = add_new_block(block_id);
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
    BlockCollect* block = add_new_block(block_id);
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
      add_server(ds_info, is_new, now);
      ServerCollect* server = get_server(ds_info.id_);
      block->add(server, now, false, master);
      server->add(block, false);
    }
  }

  const int32_t MAX_ADD_TARGET_SERVER = 0x0f;
  for (i = 0; i < MAX_ADD_TARGET_SERVER; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now)); 
  }

  EXPECT_EQ(0, build_plan());

  dump_plan();

  EXPECT_TRUE(server_to_task_.size() != 0U);
  EXPECT_TRUE(block_to_task_.size() != 0U);
  EXPECT_TRUE(pending_plan_list_.size() != 0U);
  clear_task();
}

TEST_F(LayoutManagerTest, run_plan)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  int64_t need = 0x2;
  uint32_t block_id = 0;
  BlockCollect* block = add_new_block(block_id);

  ++ds_info.id_;
  add_server(ds_info, is_new, now);
  ServerCollect* server = get_server(ds_info.id_);

  bool master = false;
  block->add(server, now, false, master);
  server->add(block, false);

  need = 0x02;
  std::vector<uint32_t> blocks;
  build_redundant_plan(now, need, blocks);
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, server_to_task_.size());
  EXPECT_EQ(0x1U, pending_plan_list_.size());
  EXPECT_EQ(0x1U, block_to_task_.size());

  dump_plan();

  run_plan();

  RedundantTaskPtr task = RedundantTaskPtr::dynamicCast(find_task(block->id()));
  RedundantTaskPtr task_move =  RedundantTaskPtr::dynamicCast((*running_plan_list_.begin()));
  EXPECT_EQ(task->block_id_, task_move->block_id_);
  EXPECT_EQ(task->type_, task_move->type_);
  EXPECT_EQ(task->priority_, task_move->priority_);
  EXPECT_EQ(0x0U, pending_plan_list_.size());
  EXPECT_EQ(0x1U, block_to_task_.size());

  dump_plan();

  clear_task();
}

TEST_F(LayoutManagerTest, handle_task_complete)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
    ds_list.push_back(ds_info.id_);
  }

  block_id = 0;
  BlockCollect* block_collect = add_new_block(block_id);//normal replicate

  ds_info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));

  EXPECT_EQ(0, build_plan());

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
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&replicate_msg));
  EXPECT_EQ(1U, finish_plan_list_.size());
  // handle again
  block_to_task_.clear();
  server_to_task_.clear();
  EXPECT_EQ(0, build_plan());
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&replicate_msg)); // warn, task is exist
  //replicate_msg.set_command(COMMAND_REPLICATE);
  //EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&replicate_msg)); // error, handle complete failed

  // handle replicate task complete as slave
  ngi.owner_role_ = NS_ROLE_SLAVE; 
  expire();
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&replicate_msg));
  EXPECT_EQ(0U, finish_plan_list_.size());
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

  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
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
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&compact_msg));
  EXPECT_EQ(1U, finish_plan_list_.size());

  // handle again
  block_to_task_.clear();
  server_to_task_.clear();
  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&compact_msg)); // warn, task is exist
  // handle compact task complete as slave
  ngi.owner_role_ = NS_ROLE_SLAVE; 
  expire();
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&compact_msg));
  EXPECT_EQ(0U, finish_plan_list_.size());
  clear_task();
}

TEST_F(LayoutManagerTest, build_replicate_plan)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }
  int32_t count = 0;
  uint32_t block_id = 0;
  BlockCollect* block = NULL;
  do
  {
    block_id = 0;
    block = add_new_block(block_id);//emergency replicate
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
  BlockCollect* normal_block = add_new_block(normal_block_id);//normal replicate
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
  int64_t need = 1;
  int64_t adjust = 0;
  int64_t emergency_replicate_count = 0;
  ds_info.id_ ++;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks));

  TBSYS_LOG(DEBUG, "===============================");
  EXPECT_EQ(true, find_block_in_plan(block_id));
  ReplicateTaskPtr task = ReplicateTaskPtr::dynamicCast(find_task(block_id));
  EXPECT_EQ(PLAN_TYPE_REPLICATE, task->type_);
  EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, task->priority_);
  EXPECT_EQ(block_id, task->block_id_);
  EXPECT_EQ(0x4U, server_to_task_.size());
  EXPECT_EQ(0x2U, pending_plan_list_.size());
  EXPECT_EQ(0x2U, block_to_task_.size());
  EXPECT_EQ(0x0, need);

  dump_plan();

  adjust = 0;
  need = 0x02;
  emergency_replicate_count = 0;
  SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
  ds_info.id_ ++;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  EXPECT_EQ(true, build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(false, find_block_in_plan(normal_block_id));
  EXPECT_EQ(false, find_server_in_plan(get_server(ds_info.id_)));
  EXPECT_EQ(0x02, need);

  dump_plan();

  adjust = 0;
  emergency_replicate_count = 0;
  SYSPARAM_NAMESERVER.replicate_ratio_ = 25;
  EXPECT_EQ(true, build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(true, find_block_in_plan(normal_block_id));
  EXPECT_EQ(true, find_server_in_plan(get_server(ds_info.id_)));
  EXPECT_EQ(0x6U, server_to_task_.size());
  EXPECT_EQ(0x3U, pending_plan_list_.size());
  EXPECT_EQ(0x3U, block_to_task_.size());
  EXPECT_EQ(0x01, need);

  dump_plan();

  server_to_task_.clear();
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
    remove_server(ds_info.id_, now);
  }

  need = 0x02;
  adjust = 0;
  emergency_replicate_count = 0;
  EXPECT_EQ(true, build_replicate_plan(now, need, adjust, emergency_replicate_count, blocks));
  EXPECT_EQ(false, find_block_in_plan(normal_block_id));
  EXPECT_EQ(false, find_server_in_plan(get_server(ds_info.id_)));
  EXPECT_EQ(0x0U, server_to_task_.size());
  EXPECT_EQ(0x0U, pending_plan_list_.size());
  EXPECT_EQ(0x0U, block_to_task_.size());
  EXPECT_EQ(0x02, need);
}

TEST_F(LayoutManagerTest, build_compact_plan)
{
  bool is_new = false;
  time_t now = time(NULL);
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  int64_t need = 0x02;
  uint32_t block_id = 0;
  BlockCollect* block = add_new_block(block_id);
  
  std::vector<uint32_t> blocks;
  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(false, find_block_in_plan(block_id));
  EXPECT_EQ(0x0U, server_to_task_.size());
  EXPECT_EQ(0x0U, pending_plan_list_.size());
  EXPECT_EQ(0x0U, block_to_task_.size());
  EXPECT_EQ(0x02, need);

  SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;//1/2
  BlockInfo info;
  memset(&info, 0, sizeof(info));
  info.block_id_ = block_id;
  block->update(info);

  info.del_size_ = SYSPARAM_NAMESERVER.max_block_size_ / 2;
  info.size_     = SYSPARAM_NAMESERVER.max_block_size_ + 1; 
  block->update(info);

  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(false, find_block_in_plan(block_id));
  EXPECT_EQ(0x0U, server_to_task_.size());
  EXPECT_EQ(0x0U, pending_plan_list_.size());
  EXPECT_EQ(0x0U, block_to_task_.size());
  EXPECT_EQ(0x02, need);
 
  info.file_count_ = 0x01;
  info.del_file_count_ = 0x01;
  block->update(info);

  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(true, find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), server_to_task_.size());
  EXPECT_EQ(0x01U, pending_plan_list_.size());
  EXPECT_EQ(0x01U, block_to_task_.size());
  EXPECT_EQ(0x01, need);

  info.file_count_ = info.del_file_count_ * 2 + 1;// > 1/2
  block->update(info);

  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(true, find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), server_to_task_.size());
  EXPECT_EQ(0x01U, pending_plan_list_.size());
  EXPECT_EQ(0x01U, block_to_task_.size());
  EXPECT_EQ(0x01, need);

  clear_task();

  for (int32_t i =  SYSPARAM_NAMESERVER.min_replication_; i <  SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  block_id = 0;
  block = add_new_block(block_id);
  memset(&info, 0, sizeof(info));
  info.block_id_ = block_id;
  block->update(info);
  info.del_size_ = 0xff;
  info.size_     = info.del_size_ * 2; 
  info.del_file_count_ = 0x01;
  info.file_count_ = 0x01;
  block->update(info);
  need = 0x2;

  EXPECT_EQ(true, build_compact_plan(now, need, blocks));
  EXPECT_EQ(true, find_block_in_plan(block_id));
  EXPECT_EQ(static_cast<uint32_t>(block->get_hold_size()), server_to_task_.size());
  EXPECT_EQ(0x01U, pending_plan_list_.size());
  EXPECT_EQ(0x01U, block_to_task_.size());
  EXPECT_EQ(0x01, need);
  clear_task();
}

TEST_F(LayoutManagerTest, build_balance_plan)
{
  NsGlobalInfo gi;
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

  TBSYS_LOG(DEBUG, "alive_server_size:%d", alive_server_size_);
  for (; index < MAX_SERVER_COUNT; ++index)
  {
    ++ds_info.id_;
    ds_info.current_load_ = ds_info.current_load_ * (index + 1);
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  std::vector<BlockCollect*> balance_blocks;

  BlockCollect* block = NULL;
  for (uint32_t block_id = 0; index < MAX_BLOCK_COUNT; ++index, block_id = 0, block = NULL)
  {
    block = add_new_block(block_id);
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

  std::vector<uint32_t> blocks;
  int64_t need = 0x02;
  EXPECT_EQ(true, build_balance_plan(now, need, blocks));
  int32_t find = 0;
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  EXPECT_TRUE(find == 0);
  EXPECT_EQ(0x02, need);

  ++ds_info.id_;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  EXPECT_EQ(true, build_balance_plan(now, need, blocks));
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  EXPECT_TRUE(find == 0x01);
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(true, find_server_in_plan(get_server(ds_info.id_)));
  EXPECT_EQ(0x2U, server_to_task_.size());
  EXPECT_EQ(0x1U, pending_plan_list_.size());
  EXPECT_EQ(0x1U, block_to_task_.size());

  find = 0;
  ++ds_info.id_;
  ds_info.current_load_ = 10;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  EXPECT_EQ(true, build_balance_plan(now, need, blocks));
  if (!balance_blocks.empty())
  {
    std::vector<BlockCollect*>::iterator iter = balance_blocks.begin();
    for (; iter != balance_blocks.end(); ++iter)
    {
      if (find_block_in_plan((*iter)->id()))
      {
        ++find;
      }
    }
  }
  TBSYS_LOG(DEBUG, "balance_blocks.size(%u), find(%d)", balance_blocks.size(), find);
  EXPECT_TRUE(find == 0x02);
  EXPECT_EQ(0x00, need);
  EXPECT_EQ(true, find_server_in_plan(get_server(ds_info.id_)));
  EXPECT_EQ(0x4U, server_to_task_.size());
  EXPECT_EQ(0x2U, pending_plan_list_.size());
  EXPECT_EQ(0x2U, block_to_task_.size());

  clear_task();
}

TEST_F(LayoutManagerTest, build_redundant_plan)
{
  std::vector<uint32_t> blocks;
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  int64_t need = 0x2;
  uint32_t block_id = 0;
  BlockCollect* block = add_new_block(block_id);
  EXPECT_EQ(true, build_redundant_plan(now, need, blocks));
  EXPECT_EQ(0x02, need);

  for (int32_t i = SYSPARAM_NAMESERVER.min_replication_; i <  SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    ds_info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }
  block_id = 0;
  block = add_new_block(block_id);
  EXPECT_EQ(true, build_redundant_plan(now, need, blocks));
  EXPECT_EQ(0x02, need);

  need = 0;
  EXPECT_EQ(true, build_redundant_plan(now, need, blocks));
  EXPECT_EQ(0x00, need);

  ++ds_info.id_;
  add_server(ds_info, is_new, now);
  ServerCollect* server = get_server(ds_info.id_);

  bool master = false;
  block->add(server, now, false, master);
  server->add(block, false);

  need = 0x02;
  EXPECT_EQ(true, build_redundant_plan(now, need, blocks));
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, server_to_task_.size());
  EXPECT_EQ(0x1U, pending_plan_list_.size());
  EXPECT_EQ(0x1U, block_to_task_.size());

  EXPECT_EQ(true, build_redundant_plan(now, need, blocks));
  EXPECT_EQ(0x01, need);
  EXPECT_EQ(0x1U, server_to_task_.size());
  EXPECT_EQ(0x1U, pending_plan_list_.size());
  EXPECT_EQ(0x1U, block_to_task_.size());

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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* source_server = get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* dest_server = get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  ReplicateTaskPtr replicate_task = new ReplicateTask(this, priority, block_id, now, now + 10800, runer);
  add_task(replicate_task);
  EXPECT_EQ(1U, pending_plan_list_.size());
  EXPECT_EQ(1U, block_to_task_.size());
  EXPECT_EQ(2U, server_to_task_.size());

  pending_plan_list_.clear();
  block_to_task_.clear();
  server_to_task_.clear();
  MoveTaskPtr move_task = new MoveTask(this, priority, block_id, now, now + 10800, runer);
  add_task(move_task);
  EXPECT_EQ(1U, pending_plan_list_.size());
  EXPECT_EQ(1U, block_to_task_.size());
  EXPECT_EQ(2U, server_to_task_.size());

  pending_plan_list_.clear();
  block_to_task_.clear();
  server_to_task_.clear();
  RedundantTaskPtr redundant_task = new RedundantTask(this, priority, block_id, now, now + 10800, runer);
  add_task(redundant_task);
  EXPECT_EQ(1U, pending_plan_list_.size());
  EXPECT_EQ(1U, block_to_task_.size());
  EXPECT_EQ(2U, server_to_task_.size());

  pending_plan_list_.clear();
  block_to_task_.clear();
  server_to_task_.clear();
  CompactTaskPtr compact_task = new CompactTask(this, priority, block_id, now, now + 10800, runer);
  add_task(compact_task);
  EXPECT_EQ(1U, pending_plan_list_.size());
  EXPECT_EQ(1U, block_to_task_.size());
  EXPECT_EQ(2U, server_to_task_.size());
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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* source_server = get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* dest_server = get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  ReplicateTaskPtr replicate_task = new ReplicateTask(this, priority, block_id, now, now + 10800, runer);
  add_task(replicate_task);
  EXPECT_EQ(true, find_block_in_plan(block_id));

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
  
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* source_server = get_server(info.id_);
  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  ServerCollect* dest_server = get_server(info.id_);
  runer.push_back(source_server);
  runer.push_back(dest_server);

  ReplicateTaskPtr replicate_task = new ReplicateTask(this, priority, block_id, now, now + 10800, runer);
  add_task(replicate_task);
  EXPECT_EQ(true, find_server_in_plan(source_server));
  EXPECT_EQ(true, find_server_in_plan(dest_server));

  clear_task();
}

TEST_F(LayoutManagerTest, expire)
{
  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }

  block_id = 0;
  add_new_block(block_id);//normal replicate

  ds_info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));

  EXPECT_EQ(0, build_plan());

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
  EXPECT_EQ(TFS_SUCCESS, handle_task_complete(&replicate_msg));

  EXPECT_EQ(1U, finish_plan_list_.size());
  EXPECT_EQ(true, expire());
  EXPECT_EQ(0U, finish_plan_list_.size());
}


TEST_F(LayoutManagerTest, scan)
{
  const int32_t TRY_TIMES= 5;

  NsGlobalInfo gi;
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
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now));
  }
  ServerCollect* server_collect = get_server(ds_info.id_);
  server_collect->dead();

  uint32_t block_id = 0;
  const int32_t block_count = 29;
  // blockid: 129:158
  for(i = 0; i < block_count; i++)
  {
    block_id = 0;
    add_new_block(block_id);
  }

  SSMScanParameter param;
  memset(&param, 0, sizeof(param));

  // get servers info in one time
  param.type_ = SSM_TYPE_SERVER;
  param.child_type_ = SSM_CHILD_SERVER_TYPE_ALL;

  EXPECT_EQ(TFS_SUCCESS, scan(param));
  EXPECT_EQ(SSM_SCAN_END_FLAG_YES, (param.end_flag_ >> 4) & 0x0f);                                                           
  EXPECT_EQ(server_count - 1, param.should_actual_count_ & 0x0ffff);

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
      EXPECT_EQ(TFS_SUCCESS, scan(param));
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
  EXPECT_EQ(TFS_SUCCESS, scan(param));
  EXPECT_EQ(block_count, param.should_actual_count_ & 0x0ffff);
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
      EXPECT_EQ(TFS_SUCCESS, scan(param));
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

TEST_F(LayoutManagerTest, handle)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.destroy_flag_ = NS_DESTROY_FLAGS_NO;
  ngi.owner_role_   = NS_ROLE_MASTER;

  bool is_new = false;
  time_t now = time(NULL);
  NsGlobalInfo gi;
  memset(&gi, 0, sizeof(gi));
  GFactory::get_global_info().update(gi);
  GFactory::get_global_info().elect_seq_num_ = 0;
  DataServerStatInfo ds_info;
  memset(&ds_info, 0, sizeof(ds_info));
  ds_info.status_ = DATASERVER_STATUS_ALIVE;
  ds_info.id_ = 0xffffff10;
  ds_info.current_load_ = 0xff;
  ds_info.total_capacity_ = 0xffffff;

  int32_t i = 0;
  for (i = 0; i < SYSPARAM_NAMESERVER.min_replication_ ; ++i)
  {
    ds_info.id_++;
    ds_info.current_load_ = ds_info.current_load_ * (i + 1);
    EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now)); //replicate && balance source server
  }

  uint32_t block_id = 0;
  BlockCollect* block = add_new_block(block_id);
  ASSERT_TRUE(block != NULL);

  ClientCmdMessage msg;
  msg.set_cmd(CLIENT_CMD_LOADBLK);//CLIENT_CMD_LOADBLK
  msg.set_value1(ds_info.id_);
  msg.set_value3(0xffff);

  StatusMessage* rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value3(block_id);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  block->get_hold().clear();
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);

  block_id = 0;
  block = add_new_block(block_id);

  msg.set_cmd(CLIENT_CMD_EXPBLK);//CLIENT_CMD_EXPBLK
  msg.set_value3(0xffff);
  msg.set_value1(ds_info.id_);

  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(0xffff);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(ds_info.id_);
  msg.set_value3(block_id);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(0);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  block->get_hold().clear();
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);

  //CLIENT_CMD_COMPACT
  block_id = 0;
  block = add_new_block(block_id);

  msg.set_cmd(CLIENT_CMD_COMPACT);
  msg.set_value3(0xffff);

  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value3(block_id); 
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);
  EXPECT_EQ(1U, block_to_task_.size());

  //CLIENT_CMD_IMMEDIATELY_REPL
  msg.set_cmd(CLIENT_CMD_IMMEDIATELY_REPL);
  msg.set_value3(0xffff);

  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value3(block_id); 
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);
  EXPECT_EQ(1U, block_to_task_.size());

  clear_task();

  msg.set_value4(REPLICATE_BLOCK_MOVE_FLAG_YES);
  msg.set_value3(block_id);
  msg.set_value1(0);
  msg.set_value2(ds_info.id_);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(ds_info.id_);
  msg.set_value2(0);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(ds_info.id_);
  msg.set_value2(ds_info.id_);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value1(ds_info.id_ + 0xfff);
  msg.set_value2(ds_info.id_);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);

  msg.set_value3(block_id);
  msg.set_value1(ds_info.id_ - 1);
  msg.set_value2(ds_info.id_);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);
  EXPECT_EQ(1U, block_to_task_.size());
  clear_task();

  msg.set_value4(REPLICATE_BLOCK_MOVE_FLAG_NO);
  msg.set_value3(block_id);
  msg.set_value1(0);
  msg.set_value2(ds_info.id_);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);
  clear_task();

  msg.set_value3(block_id);
  msg.set_value1(ds_info.id_);
  msg.set_value2(0);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);
  clear_task();

  ds_info.id_++;
  EXPECT_EQ(TFS_SUCCESS, add_server(ds_info, is_new, now)); //replicate && balance source server
  msg.set_value3(block_id);
  msg.set_value1(ds_info.id_);
  msg.set_value2(0);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_ERROR, rmsg->get_status());
  tbsys::gDelete(rmsg);
  clear_task();

  msg.set_value3(block_id);
  msg.set_value1(0);
  msg.set_value2(0);
  rmsg = handle(&msg);
  ASSERT_TRUE(rmsg != NULL);
  EXPECT_EQ(STATUS_MESSAGE_OK, rmsg->get_status());
  tbsys::gDelete(rmsg);
  clear_task();
}


TEST_F(LayoutManagerTest, set_runtime_param)
{
  NsGlobalInfo gi;
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
  plan_run_flag_ = 0x02;
  SYSPARAM_NAMESERVER.run_plan_expire_interval_= 10;
  SYSPARAM_NAMESERVER.run_plan_ratio_ = 10;
  SYSPARAM_NAMESERVER.object_dead_max_time_ = 20;

  uint32_t index = 0;
  uint32_t value = 0;
  char msg[0xff];
  memset(msg, 0, sizeof(msg));

  index = 0x000000001;
  index |= 0x10000000;
  value = 0x03;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0x03, SYSPARAM_NAMESERVER.min_replication_);

  index = 0x000000002;
  index |= 0x10000000;
  value = 0x04;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0x04, SYSPARAM_NAMESERVER.max_replication_);

  index = 0x000000003;
  index |= 0x10000000;
  value = 0xff;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0xff, SYSPARAM_NAMESERVER.max_write_file_count_);

  index = 0x000000004;
  index |= 0x10000000;
  value = 0xff;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0xff, SYSPARAM_NAMESERVER.max_use_capacity_ratio_);

  index = 0x000000009;
  index |= 0x10000000;
  value = 0x3;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0x3, SYSPARAM_NAMESERVER.cluster_index_);

  index = 0x00000000A;
  index |= 0x10000000;
  value = 0x4;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0x4, plan_run_flag_); 

  index = 0x00000000B;
  index |= 0x10000000;
  value = 0xab;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0xab, SYSPARAM_NAMESERVER.run_plan_expire_interval_); 

  index = 0x00000000C;
  index |= 0x10000000;
  value = 100;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(100, SYSPARAM_NAMESERVER.run_plan_ratio_); 

  index = 0x00000000D;
  index |= 0x10000000;
  value = 0xab;
  set_runtime_param(index, value, msg);
  EXPECT_EQ(0xab, SYSPARAM_NAMESERVER.object_dead_max_time_); 
}

void LayoutManagerTest::clear_task()
{
  std::map<uint32_t, TaskPtr>::iterator iter = block_to_task_.begin();
  for (; iter != block_to_task_.end(); ++iter)
  {
    GFactory::get_timer()->cancel((iter->second));
  }
  pending_plan_list_.clear();
  block_to_task_.clear();
  server_to_task_.clear();
  running_plan_list_.clear();
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

