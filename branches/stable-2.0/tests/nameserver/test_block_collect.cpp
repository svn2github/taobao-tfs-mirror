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
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "block_collect.h"
#include "server_collect.h"

using namespace tfs::nameserver;
using namespace tfs::common;

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
    SYSPARAM_NAMESERVER.min_replication_ = 2;
    SYSPARAM_NAMESERVER.max_replication_ = 4;
    SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
    SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100;
    SYSPARAM_NAMESERVER.max_block_size_ = 100;
    SYSPARAM_NAMESERVER.max_write_file_count_= 10;
    SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
    SYSPARAM_NAMESERVER.object_dead_max_time_ = 1;
    SYSPARAM_NAMESERVER.object_clear_max_time_ = 1;
    SYSPARAM_NAMESERVER.group_count_ = 1;
    SYSPARAM_NAMESERVER.group_seq_ = 0;

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
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffff0;
  time_t now = time(NULL);
  ServerCollect* server = NULL;
  bool force = false;
  bool writable = false;
  BlockCollect block(100, now);
  EXPECT_EQ(block.add(server, now, force, writable), false);

  server = new ServerCollect(info, now);
  //hold_.size() == 0 && force == false 
  //&& hold_master_ == HOLD_MASTER_FLAG_NO
  EXPECT_EQ(true, block.add(server, now, force, writable));//mster
  EXPECT_EQ(server, block.hold_[0]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(now, block.last_update_time_);
  EXPECT_EQ(true, block.get_hold_size() == 0x01);

  //hold_.sizse() == 1 && force = false
  //&& hold_master_ == BlockCollect::HOLD_MASTER_FLAG_YES
  EXPECT_EQ(true,block.add(server, now, force, writable));
  EXPECT_EQ(server, block.hold_[0]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(now, block.last_update_time_);
  EXPECT_EQ(true, block.get_hold_size() == 0x01);

  //force update
  ++info.id_;
  force = true;
  //hold_size() == 1 &&  force == true
  ServerCollect* other = new ServerCollect(info, now);
  EXPECT_EQ(true, block.add(other, now, force, writable));
  EXPECT_EQ(true, block.get_hold_size() == 0x02);
  EXPECT_EQ(other, block.hold_[0]);
  EXPECT_EQ(server, block.hold_[1]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(now, block.last_update_time_);
  EXPECT_EQ(true, block.get_hold_size() == 0x02);

  //force update
  force = true;
  EXPECT_EQ(true, block.add(server, now, force, writable));
  EXPECT_EQ(true, block.get_hold_size() == 0x02);
  EXPECT_EQ(server, block.hold_[0]);
  EXPECT_EQ(other, block.hold_[1]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(now, block.last_update_time_);

  ++info.id_;
  ServerCollect* other2 = new ServerCollect(info, now);
  force = false;
  EXPECT_EQ(true, block.add(other2, now, force, writable));
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(server, block.hold_[0]);
  EXPECT_EQ(other, block.hold_[1]);
  EXPECT_EQ(other2, block.hold_[2]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(now, block.last_update_time_);

  EXPECT_EQ(true, block.add(other2, now, force, writable));
  EXPECT_EQ(3, block.get_hold_size());

  SYSPARAM_NAMESERVER.max_write_file_count_= -1;

  force = true;
  EXPECT_EQ(true, block.add(other2, now, force, writable));
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(other2, block.hold_[0]);
  EXPECT_EQ(server, block.hold_[1]);
  EXPECT_EQ(other, block.hold_[2]);

  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;
  ++info.id_;
  ServerCollect* other3 = new ServerCollect(info, now);
  force = false;
  EXPECT_EQ(true, block.add(other3, now, force, writable));

  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);
  EXPECT_EQ(4, block.get_hold_size());
  EXPECT_EQ(other2, block.hold_[0]);
  EXPECT_EQ(server, block.hold_[1]);
  EXPECT_EQ(other, block.hold_[2]);
  EXPECT_EQ(other3, block.hold_[3]);

  TBSYS_LOG(DEBUG, "hold_master_(%d)", block.hold_master_);
  tbsys::gDelete(server);
  tbsys::gDelete(other);
  tbsys::gDelete(other2);
  tbsys::gDelete(other3);
}

TEST_F(BlockCollectTest, relieve_relation)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 10000;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  //initialize
  BlockCollect block(100, now);
  ServerCollect* server = new ServerCollect(info, now);
  server->total_capacity_ = 1000;
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  
  info.id_ = 10001;
  master = false;
  ServerCollect* other = new ServerCollect(info, now);
  other->total_capacity_ = 1000;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(server, block.hold_[0]);

  block.relieve_relation();
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);

  //block full
  block.info_.size_ = SYSPARAM_NAMESERVER.max_block_size_;
  block.relieve_relation();
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);

  block.info_.size_ = 0;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);

  //server unwritable
  server->use_capacity_ = 1000 * 1000;
  block.relieve_relation();
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);

  server->use_capacity_ = 0;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);

  //hold_size < min_replication_
  std::vector<ServerCollect*>::iterator iter = find(block.hold_.begin(), block.hold_.end(), server);
  if (iter != block.hold_.end())
  {
    block.hold_.erase(iter);
  } 
  EXPECT_EQ(1, block.get_hold_size());
  block.relieve_relation();
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);
  
  tbsys::gDelete(server);
  tbsys::gDelete(other);
}

TEST_F(BlockCollectTest, remove)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  ServerCollect* tmp = NULL;
  BlockCollect block(100, now);
  EXPECT_EQ(true, block.remove(tmp, now));

  //initialize
  ServerCollect* server = new ServerCollect(info, now);
  server->total_capacity_ = 1000;
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  
  ++info.id_ ;
  master = false;
  ServerCollect* other = new ServerCollect(info, now);
  other->total_capacity_ = 1000;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(server, block.hold_[0]);

  EXPECT_EQ(true, block.remove(other, now));
  EXPECT_EQ(true, block.remove(server, now));
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_NO, block.hold_master_);

  tbsys::gDelete(tmp);
  tbsys::gDelete(server);
  tbsys::gDelete(other);
}

TEST_F(BlockCollectTest, exist)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  //initialize
  BlockCollect block(100, now);

  EXPECT_EQ(false, block.exist(NULL));
  
  ServerCollect server(info, now);
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(true, block.exist(&server));

  ServerCollect tmp(info, now);;
  EXPECT_EQ(false, block.exist(&tmp));
}

TEST_F(BlockCollectTest, is_master)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  //initialize
  BlockCollect block(100, now);
  ServerCollect server(info, now);
  EXPECT_EQ(false, block.is_master(NULL));
  EXPECT_EQ(false, block.is_master(&server));

  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(&server, block.hold_[0]);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(true, block.is_master(&server));

  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;
  EXPECT_EQ(false, block.is_master(&server));
}

TEST_F(BlockCollectTest, is_need_master)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);
  EXPECT_EQ(false, block.is_need_master());
  //initialize
  ServerCollect* server = new ServerCollect(info, now);
  server->total_capacity_ = 1000;
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);

  //if_full == false && hold_master_ == HOLD_MASTER_FLAG_YES 
  //&& hold_.size() < min_replication_;
  EXPECT_EQ(false, block.is_need_master());

  //if_full == false && hold_master_ == HOLD_MASTER_FLAG_NO
  //&& hold_.size() >= min_replication_;
  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect* other = new ServerCollect(info, now);
  other->total_capacity_ = 1000;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(server, block.hold_[0]);
  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;

  EXPECT_EQ(true, block.is_need_master());

  //if_full == true && hold_master_ == HOLD_MASTER_FLAG_NO 
  //&& hold_.size() >= min_replication_;
  block.info_.size_ = 0xfffffff;
  EXPECT_EQ(false, block.is_need_master());

  //if_full == false && hold_master_ == HOLD_MASTER_FLAG_YES 
  //&& hold_.size() >= min_replication_;
  block.info_.size_ = 0;
  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_YES;
  EXPECT_EQ(false, block.is_need_master());
}

TEST_F(BlockCollectTest, is_writable)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);
  EXPECT_EQ(false, block.is_writable());
  //initialize
  ServerCollect* server = new ServerCollect(info, now);
  server->total_capacity_ = 1000;
  EXPECT_EQ(block.add(server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);

  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect* other = new ServerCollect(info, now);
  other->total_capacity_ = 1000;
  EXPECT_EQ(block.add(other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(server, block.hold_[0]);

  //if_full == false && hold_master_ == HOLD_MASTER_FLAG_YES 
  //&& hold_.size() >= min_replication_ && all server writable
  EXPECT_EQ(true, block.is_writable());

  //if_full == flase && hold_master_ == HOLD_MASTER_FLAG_YES 
  //&& hold_.size() >= min_replication_  && one of the server unwrite
  //server->use_capacity_ = 1000 * 1000;
  //EXPECT_EQ(false, block.is_writable());

  //if_full == true && hold_master_ == HOLD_MASTER_FLAG_NO 
  //&& hold_.size() >= min_replication_ && all server writable
  block.info_.size_ = 1000 * 1000;
  server->use_capacity_ = 0;
  EXPECT_EQ(false, block.is_writable());

  //if_full == false && hold_master_ == HOLD_MASTER_FLAG_YES 
  //&& hold_.size() < min_replication_;
  block.remove(other, now);
  EXPECT_EQ(false, block.is_writable());
}

TEST_F(BlockCollectTest, check_version)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);
  EXPECT_EQ(false, block.is_writable());
  //initialize
  ServerCollect server(info, now);
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);

  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect other(info, now);
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  info.id_ = 0xfffffffffc;
  master = false;
  ServerCollect other2(info, now);
  EXPECT_EQ(block.add(&other2, now, force, master), true);
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  // construct block info
  BlockInfo block_info;
  memset(&block_info, 0, sizeof(block_info));
  block_info.block_id_ = 1009;
  block_info.file_count_ = 10;
  block_info.version_ = 5;
  block_info.size_ = 110;
  block.update(block_info);

  // construct new block info
  BlockInfo new_block_info;
  memcpy(&new_block_info, &block_info, sizeof(block_info));

  EXPIRE_BLOCK_LIST expires;
  bool force_be_master = false;

  NsRole role = NS_ROLE_MASTER;
  bool is_new = false;

  EXPECT_EQ(false, block.check_version(NULL, role, is_new, new_block_info, expires, force_be_master, now));

  info.id_ = 0xfffffffffc;
  ServerCollect other3(info, now);
  new_block_info.file_count_ = block_info.file_count_ - 1;

  EXPECT_EQ(false, block.check_version(&other3, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, expires.size() == 0x01);
  new_block_info.file_count_ = block_info.file_count_;
  new_block_info.size_ = 20;

  expires.clear();
  EXPECT_EQ(false, block.check_version(&other3, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(true, expires.size() == 0x01);
  EXPECT_EQ(false, force_be_master);

  new_block_info.version_ = block_info.version_ - 2;

  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == block_info.version_);

  new_block_info.version_ = block_info.version_ + 2;
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == new_block_info.version_);//version == 7 

  new_block_info.version_ = block_info.version_-2;
  expires.clear();
  EXPECT_EQ(false, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == 7);//version == 7 
  EXPECT_EQ(true, expires.size() == 0x01);

  block.hold_.clear();
  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;
  expires.clear();
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == new_block_info.version_);//version == 3 
  EXPECT_EQ(true, expires.size() == 0x00);

  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(block.add(&other2, now, force, master), true);

  is_new = true;
  new_block_info.version_ = 100;
  expires.clear();
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == new_block_info.version_);//version == 100 
  EXPECT_EQ(true, expires.size() == 0x00);

  is_new = false;
  force_be_master = false;
  new_block_info.version_ = 100 + 10;
  expires.clear();
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(false, force_be_master);
  EXPECT_EQ(true, block.info_.version_ == new_block_info.version_);//version == 110 
  EXPECT_EQ(true, expires.size() == 0x03);

  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(block.add(&other2, now, force, master), true);

  block.info_.version_ = 5;
  new_block_info.version_ = block.info_.version_ + 1;
  expires.clear();
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(true, expires.size() == 0x00);
  EXPECT_EQ(false, force_be_master);

  new_block_info.version_ = block.info_.version_ + 2;
  new_block_info.size_ = 0xfffffff; //full
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(true, expires.size() == 0x00);
  EXPECT_EQ(false, force_be_master);

  block.hold_.clear();
  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_NO;
  expires.clear();
  new_block_info.version_ = block.info_.version_ + 3;
  EXPECT_EQ(true, block.check_version(&other, role, is_new, new_block_info, expires, force_be_master, now));
  EXPECT_EQ(true, expires.size() == 0x00);
  EXPECT_EQ(true, force_be_master);
}

TEST_F(BlockCollectTest, check_replicate)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);
  now += SYSPARAM_NAMESERVER.replicate_wait_time_;
  EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now));

  //initialize
  ServerCollect server(info, now);
  server.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(PLAN_PRIORITY_EMERGENCY, block.check_replicate(now)); 

  ++info.id_; 
  master = false;
  ServerCollect other(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
  EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now)); 

  ++info.id_; 
  master = false;
  ServerCollect other2(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other2, now, force, master), true);
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
  SYSPARAM_NAMESERVER.replicate_ratio_ = 10;
  EXPECT_EQ(PLAN_PRIORITY_NORMAL, block.check_replicate(now)); 

  ++info.id_; 
  master = false;
  ServerCollect other3(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other3, now, force, master), true);
  EXPECT_EQ(4, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
  SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
  EXPECT_EQ(PLAN_PRIORITY_NONE, block.check_replicate(now)); 
}

TEST_F(BlockCollectTest, check_compact)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffff0;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);

  EXPECT_EQ(false, block.check_compact());

  //initialize
  ServerCollect server(info, now);
  server.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(false, block.check_compact());

  block.info_.size_ = 0xffffff;
  EXPECT_EQ(false, block.check_compact());

  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect other(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
  
  block.info_.file_count_ = 10;
  block.info_.del_file_count_ = 9;
  block.info_.del_size_ = 0xffffff / 2;
  EXPECT_EQ(true, block.check_compact());

  info.id_ = 0xfffffffffc;
  master = false;
  ServerCollect other2(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other2, now, force, master), true);
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
  EXPECT_EQ(true, block.check_compact());

  info.id_ = 0xfffffffffd;
  master = false;
  ServerCollect other3(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other3, now, force, master), true);
  EXPECT_EQ(4, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  EXPECT_EQ(true, block.check_compact());

  info.id_ = 0xfffffffffd;
  master = false;
  ServerCollect other4(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other4, now, force, master), true);
  EXPECT_EQ(5, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);
}

TEST_F(BlockCollectTest, check_redundant)
{
  SYSPARAM_NAMESERVER.max_replication_ = 2;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);

  EXPECT_EQ(true, block.check_redundant() == -2);

  //initialize
  ServerCollect server(info, now);
  server.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(false, block.check_compact());

  EXPECT_EQ(true, block.check_redundant()  == -1);

  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect other(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  EXPECT_EQ(true, block.check_redundant() == 0);

  info.id_ = 0xfffffffffc;
  master = false;
  ServerCollect other2(info, now);
  other2.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other2, now, force, master), true);
  EXPECT_EQ(3, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  EXPECT_EQ(true, block.check_redundant() == 1);
  SYSPARAM_NAMESERVER.max_replication_ = 4;
}

TEST_F(BlockCollectTest, check_balance)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffffffa;
  time_t now = time(NULL);
  bool force = false;
  bool master = false;

  BlockCollect block(100, now);

  EXPECT_EQ(false, block.check_balance());

  //initialize
  ServerCollect server(info, now);
  server.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&server, now, force, master), true);
  EXPECT_EQ(BlockCollect::HOLD_MASTER_FLAG_YES, block.hold_master_);
  EXPECT_EQ(false, block.check_compact());

  EXPECT_EQ(false, block.check_balance());

  info.id_ = 0xfffffffffb;
  master = false;
  ServerCollect other(info, now);
  other.total_capacity_ = 1000;
  EXPECT_EQ(block.add(&other, now, force, master), true);
  EXPECT_EQ(2, block.get_hold_size());
  EXPECT_EQ(&server, block.hold_[0]);

  EXPECT_EQ(false, block.check_balance());

  block.info_.size_ = 0xffffff;
  
  EXPECT_EQ(true, block.check_balance());
}

TEST_F(BlockCollectTest, scan)
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

  bool master = true;
  bool force  = true;
  EXPECT_EQ(true, block.add(&server, now, force, master));

  SSMScanParameter param;
  memset(&param, 0, sizeof(param));
  param.child_type_ = SSM_CHILD_BLOCK_TYPE_FULL;

  EXPECT_EQ(TFS_ERROR, block.scan(param));

  binfo.size_ = 0xffffff;
  block.update(binfo);

  param.data_.destroy();
  memset(&param, 0, sizeof(param));
  param.child_type_ = SSM_CHILD_BLOCK_TYPE_FULL;
  param.child_type_ |= SSM_CHILD_BLOCK_TYPE_INFO;
  param.child_type_ |= SSM_CHILD_BLOCK_TYPE_SERVER;
  EXPECT_EQ(TFS_SUCCESS, block.scan(param));
  BlockInfo dest;
  memset(&dest, 0, sizeof(dest));
  param.data_.readBytes(&dest, sizeof(BlockInfo));
  int8_t size = param.data_.readInt8();
  EXPECT_EQ(0x01, size);
  uint64_t server_id = param.data_.readInt64();
  EXPECT_EQ(info.id_, server_id);
}

TEST_F(BlockCollectTest, find_master)
{
  time_t now = time(NULL);
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.current_load_ = 0x01;
  ServerCollect server(info, now);

  BlockCollect block(0xfff, 0);
  EXPECT_TRUE(block.find_master() == NULL);
  block.hold_master_ = BlockCollect::HOLD_MASTER_FLAG_YES;
  EXPECT_TRUE(block.find_master() == NULL);
  block.hold_.insert(block.hold_.begin(), &server);
  EXPECT_TRUE(block.find_master() == &server);
}


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
