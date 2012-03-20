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
#include <Memory.hpp>
#include <gtest/gtest.h>
#include <tbnet.h>
#include "ns_define.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "message/compact_block_message.h"

using namespace tfs::nameserver;
using namespace tfs::common;
using namespace tfs::message;

class TaskTest:public virtual ::testing::Test,
                   public virtual LayoutManager
{
  protected:
    static void SetUpTestCase()
    {
      TBSYS_LOGGER.setLogLevel("debug");
      GFactory::initialize(); 
    }

    static void TearDownTestCase()
    {
    }
  public:
    TaskTest()
    {
    }
    ~TaskTest()
    {
    }
    virtual void SetUp()
    {
      SYSPARAM_NAMESERVER.compact_preserve_time_ = 60;
      SYSPARAM_NAMESERVER.min_replication_ = 2;
      SYSPARAM_NAMESERVER.max_replication_ = 4;
      SYSPARAM_NAMESERVER.compact_delete_ratio_ = 50;
      SYSPARAM_NAMESERVER.max_use_capacity_ratio_ = 100; 
      SYSPARAM_NAMESERVER.max_block_size_ = 100; 
      SYSPARAM_NAMESERVER.max_write_file_count_= 10;
      SYSPARAM_NAMESERVER.replicate_ratio_ = 50;
      SYSPARAM_NAMESERVER.group_mask_= 0xffffffff;
      SYSPARAM_NAMESERVER.run_plan_expire_interval_ = 1;
      initialize();
    }
    virtual void TearDown()
    {
      destroy();
      wait_for_shut_down();
    }
  protected:
};

TEST_F(TaskTest, task_run_timer_task)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffff;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
  }

  uint32_t block_id = 0xffff;
  ReplicateTaskPtr task = new ReplicateTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
  EXPECT_EQ(true, add_task(task));
  EXPECT_TRUE(find_task(block_id) != 0);
  task->runTimerTask();
  EXPECT_EQ(PLAN_STATUS_TIMEOUT, task->status_);
  EXPECT_TRUE(find_task(block_id) == 0);
}

TEST_F(TaskTest, task_dump)
{
  uint64_t BASE_SERVER_ID = 0xfffffff0;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = BASE_SERVER_ID;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
    ++info.id_;
  }

  uint32_t block_id = 0xffff;
  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);

  tbnet::DataBuffer stream;
  task->dump(stream);
  EXPECT_EQ(task->type_, stream.readInt8());
  EXPECT_EQ(task->status_, stream.readInt8());
  EXPECT_EQ(task->priority_, stream.readInt8());
  EXPECT_EQ(task->block_id_, stream.readInt32());
  EXPECT_EQ(task->begin_time_, stream.readInt64());
  EXPECT_EQ(task->begin_time_, stream.readInt64());
  int8_t size = stream.readInt8();
  EXPECT_EQ(task->runer_.size(), size);
  for (int8_t i = 0; i < size; i++)
  {
    EXPECT_EQ(BASE_SERVER_ID + i, stream.readInt64());
  }
  task->dump(TBSYS_LOG_LEVEL_DEBUG);
}

TEST_F(TaskTest, compact_task_handle)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffff;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
  }

  uint32_t block_id = 0xffff;
  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
  EXPECT_EQ(TFS_SUCCESS, task->handle());
  EXPECT_EQ(PLAN_STATUS_BEGIN, task->status_);
  EXPECT_EQ(block_id, task->block_id_);

  task->dump(TBSYS_LOG_LEVEL_DEBUG);
}

TEST_F(TaskTest, compact_task_do_complete)
{
  uint64_t BASE_SERVER_ID = 0xfffffff0;
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = BASE_SERVER_ID;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  std::vector<uint64_t> servers;

  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
  }

  uint32_t block_id = 0;
  BlockCollect* block = add_new_block(block_id);

  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
  CompactTask::CompactComplete value(BASE_SERVER_ID, block_id, PLAN_STATUS_END); 
  task->block_info_.block_id_ = block_id;
  task->block_info_.size_ = 0xfff;

  // compact has not completed, then do nothing
  value.is_complete_ = false;
  value.has_success_ = false;
  value.all_success_ = false;
  value.current_complete_result_ = false;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));
  ngi.owner_role_ = NS_ROLE_MASTER;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));

  // compact has not completed, only one server has done
  value.is_complete_ = false;
  value.has_success_ = true;
  value.all_success_ = false;
  value.current_complete_result_ = true;
  ngi.owner_role_ = NS_ROLE_SLAVE;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));
  EXPECT_EQ(0xfff, block->size());
  ngi.owner_role_ = NS_ROLE_MASTER;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));

  // compact has completed, when all servers compact success, add block as writable to servers
  value.is_complete_ = true;
  value.has_success_ = true;
  value.all_success_ = true;
  ngi.owner_role_ = NS_ROLE_SLAVE;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));
  for (int32_t i = 0; i < block->get_hold_size(); ++i)
  {
    std::set<BlockCollect*>::iterator iter;
    iter = block->get_hold()[i]->writable_.find(block);  
    EXPECT_EQ(true, iter != block->get_hold()[i]->writable_.end());
  }
  ngi.owner_role_ = NS_ROLE_MASTER;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));

  // compact has completed, current server has not complete yet
  value.block_id_ = block_id;
  value.is_complete_ = true;
  value.has_success_ = false;
  value.all_success_ = false;
  value.id_ = BASE_SERVER_ID + 2;
  servers.push_back(BASE_SERVER_ID + 2);
  servers.push_back(BASE_SERVER_ID + 3);
  value.status_ = PLAN_STATUS_TIMEOUT;
  ngi.owner_role_ = NS_ROLE_SLAVE;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));
  ngi.owner_role_ = NS_ROLE_MASTER;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));

  // compact has completed, when two servers compact failed, master will remove the block from ds
  value.is_complete_ = true;
  value.has_success_ = true;
  value.all_success_ = false;
  value.id_ = BASE_SERVER_ID;
  value.status_ = PLAN_STATUS_END;
  EXPECT_EQ(SYSPARAM_NAMESERVER.max_replication_, block->get_hold_size());
  ngi.owner_role_ = NS_ROLE_SLAVE;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));
  EXPECT_EQ(0x02, block->get_hold_size());
  ngi.owner_role_ = NS_ROLE_MASTER;
  EXPECT_EQ(TFS_SUCCESS, task->do_complete(value, servers));

  task->dump(TBSYS_LOG_LEVEL_DEBUG);
}

TEST_F(TaskTest, compact_task_check_complete)
{
  DataServerStatInfo info;
  info.id_ = 0xfffffff0;
  time_t now = time(NULL);
  uint32_t block_id = 129;
  std::vector<uint64_t> servers;
  std::vector<ServerCollect*> runer;
  std::pair<uint64_t, PlanStatus> entry;
  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now,runer);
  memset(&task->block_info_, 0, sizeof(BlockInfo));
  task->block_info_.block_id_ = block_id;
  task->block_info_.size_ = 0xff;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.min_replication_; ++i)
  {
    entry.first = ++info.id_;
    entry.second = PLAN_STATUS_BEGIN;
    task->complete_status_.push_back(entry);
  }

  CompactTask::CompactComplete complete(info.id_, block_id, PLAN_STATUS_TIMEOUT);
  complete.block_info_.block_id_ = block_id;
  complete.block_info_.size_ = 0xffff;

  task->check_complete(complete, servers);
  EXPECT_TRUE(complete.block_info_.block_id_ == task->block_info_.block_id_);
  EXPECT_TRUE(complete.block_info_.size_ != task->block_info_.size_);
  EXPECT_TRUE(complete.is_complete_ == false);
  EXPECT_TRUE(complete.has_success_ == false);
  EXPECT_TRUE(complete.all_success_ == false);
  EXPECT_TRUE(complete.current_complete_result_ == false);

  complete.status_ = PLAN_STATUS_END;
  task->check_complete(complete, servers);
  EXPECT_TRUE(complete.block_info_.block_id_ == task->block_info_.block_id_);
  EXPECT_TRUE(complete.block_info_.size_ == task->block_info_.size_);
  EXPECT_TRUE(complete.is_complete_ == false);
  EXPECT_TRUE(complete.has_success_ == true);
  EXPECT_TRUE(complete.all_success_ == false);
  EXPECT_TRUE(complete.current_complete_result_ == true);

  CompactTask::CompactComplete completev2(info.id_ - 1, block_id, PLAN_STATUS_TIMEOUT);
  completev2.block_info_.block_id_ = block_id;
  completev2.block_info_.size_ = 0xfffff;

  complete.status_ = PLAN_STATUS_TIMEOUT;
  task->check_complete(complete, servers);

  task->check_complete(completev2, servers);
  EXPECT_TRUE(completev2.block_info_.block_id_ == task->block_info_.block_id_);
  EXPECT_TRUE(completev2.block_info_.size_ != task->block_info_.size_);
  EXPECT_TRUE(completev2.is_complete_ == true);
  EXPECT_TRUE(completev2.has_success_ == false);
  EXPECT_TRUE(completev2.all_success_ == false);
  EXPECT_TRUE(completev2.current_complete_result_ == false);

  complete.status_ = PLAN_STATUS_END;
  task->check_complete(complete, servers);

  completev2.status_ = PLAN_STATUS_END;
  task->check_complete(completev2, servers);
  EXPECT_TRUE(completev2.block_info_.block_id_ == task->block_info_.block_id_);
  EXPECT_TRUE(completev2.block_info_.size_ == task->block_info_.size_);
  EXPECT_TRUE(completev2.is_complete_ == true);
  EXPECT_TRUE(completev2.has_success_ == true);
  EXPECT_TRUE(completev2.all_success_ == true);
  EXPECT_TRUE(completev2.current_complete_result_ == true);
}

TEST_F(TaskTest, compact_task_dump)
{
  uint64_t BASE_SERVER_ID = 0xfffffff0;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = BASE_SERVER_ID;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  
  uint32_t block_id = 0xffff;
  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
  std::pair <uint64_t, PlanStatus> entry;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
    entry.first = info.id_;
    entry.second = PLAN_STATUS_BEGIN;
    task->complete_status_.push_back(entry);
    ++info.id_;
  }
  task->runer_ = runer;

  tbnet::DataBuffer stream;
  task->dump(stream);
  EXPECT_EQ(task->type_, stream.readInt8());
  EXPECT_EQ(task->status_, stream.readInt8());
  EXPECT_EQ(task->priority_, stream.readInt8());
  EXPECT_EQ(task->block_id_, stream.readInt32());
  EXPECT_EQ(task->begin_time_, stream.readInt64());
  EXPECT_EQ(task->begin_time_, stream.readInt64());
  int8_t size = stream.readInt8();
  EXPECT_EQ(task->runer_.size(), size);
  for (int8_t i = 0; i < size; i++)
  {
    EXPECT_EQ(BASE_SERVER_ID + i, stream.readInt64());
  }
  task->dump(TBSYS_LOG_LEVEL_DEBUG);
}

TEST_F(TaskTest, compact_run_timer_task)
{
  uint64_t BASE_SERVER_ID = 0xfffffff0;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = BASE_SERVER_ID;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  
  uint32_t block_id = 0xffff;
  CompactTaskPtr task = new CompactTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, runer);
  std::pair <uint64_t, PlanStatus> entry;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_; ++i)
  {
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
    entry.first = info.id_;
    entry.second = PLAN_STATUS_BEGIN;
    task->complete_status_.push_back(entry);
    ++info.id_;
  }
  task->runer_ = runer;

  EXPECT_EQ(true, add_task(task));
  EXPECT_TRUE(find_task(block_id) != 0);
  task->runTimerTask();
  EXPECT_EQ(PLAN_STATUS_TIMEOUT, task->status_);
  EXPECT_TRUE(find_task(block_id) == 0);
}

TEST_F(TaskTest, replicate_task_handle)
{
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffff;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  time_t now = time(NULL);
  bool is_new = false;
  std::vector<ServerCollect*> runer;
  for (int32_t i = 0; i < SYSPARAM_NAMESERVER.min_replication_; ++i)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
    runer.push_back(get_server(info.id_));
  }

  std::vector<ServerCollect*> invalid_runer;

  uint32_t block_id = 0xffff;
  ReplicateTaskPtr task = new ReplicateTask(this, PLAN_PRIORITY_NORMAL, block_id, now, now, invalid_runer);
  EXPECT_EQ(TFS_ERROR, task->handle());
  task->runer_ = runer;
  EXPECT_EQ(TFS_SUCCESS, task->handle());
  EXPECT_EQ(PLAN_STATUS_BEGIN, task->status_);
  EXPECT_EQ(block_id, task->block_id_);

  task->dump(TBSYS_LOG_LEVEL_DEBUG);
}


TEST_F(TaskTest, replicate_task_handle_complete)
{
  NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
  ngi.owner_role_ = NS_ROLE_MASTER;
  DataServerStatInfo info;
  memset(&info, 0, sizeof(info));
  info.status_ = DATASERVER_STATUS_ALIVE;
  info.id_ = 0xfffffff0;
  info.current_load_ = 30;
  info.total_capacity_ = 0xffffff;
  int64_t BASE_SERVER_ID = info.id_;
  time_t now = time(NULL);
  bool is_new = false;
  PlanPriority priority = PLAN_PRIORITY_NORMAL;
  std::vector<ServerCollect*> runner;
  ReplBlock repl_block_info;
  ReplicateBlockMessage message;
  ServerCollect* server_collect = NULL;

  int32_t i = 0;
  for(i = 0; i < 2; i++)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  }

  uint32_t block_id = 0;
  BlockCollect* repl_block = add_new_block(block_id); // replicate block, not move

  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));

  server_collect = get_server(BASE_SERVER_ID + 1);
  runner.push_back(server_collect);// source server
  server_collect = get_server(BASE_SERVER_ID + 3);
  runner.push_back(server_collect);// dest server

  repl_block_info.block_id_ = block_id;
  repl_block_info.source_id_= BASE_SERVER_ID + 1;
  repl_block_info.destination_id_= BASE_SERVER_ID + 3;
  repl_block_info.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_NO;
  int32_t command = PLAN_STATUS_END;


  message.set_command(command);
  message.set_repl_block(&repl_block_info);

  // replicate task
  bool all_complete_flag = true;
  ReplicateTaskPtr replicate_task = new ReplicateTask(this, priority, block_id, now, now, runner);
  EXPECT_EQ(2, repl_block->get_hold_size());
  EXPECT_EQ(TFS_SUCCESS, replicate_task->handle_complete(&message, all_complete_flag));
  EXPECT_EQ(3, repl_block->get_hold_size());

  for(i = 0; i < 2; i++)
  {
    info.id_ ++;
    EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));
  }

  bool master = false;
  block_id = 0;
  BlockCollect* move_block = add_block(block_id); // replicate block, need move
  for(i = 0; i< 5; i++) 
  {
    server_collect = get_server(BASE_SERVER_ID + i + 1);
    move_block->add(server_collect, now, false, master);
    server_collect->add(move_block, false);
  }   

  info.id_ ++;
  EXPECT_EQ(TFS_SUCCESS, add_server(info, is_new, now));

  runner.clear();
  server_collect = get_server(BASE_SERVER_ID + 2);
  runner.push_back(server_collect);// source server
  server_collect = get_server(BASE_SERVER_ID + 6);
  runner.push_back(server_collect);// dest server

  // move task
  repl_block_info.block_id_ = block_id;
  repl_block_info.source_id_= BASE_SERVER_ID + 2;
  repl_block_info.destination_id_= BASE_SERVER_ID + 6;
  repl_block_info.is_move_ = REPLICATE_BLOCK_MOVE_FLAG_YES;
  message.set_repl_block(&repl_block_info);
  ReplicateTaskPtr move_task = new ReplicateTask(this, priority, block_id, now, now, runner);
  server_collect = get_server(BASE_SERVER_ID + 2);
  EXPECT_EQ(5, move_block->get_hold_size());
  EXPECT_EQ(2, server_collect->block_count()); 
  EXPECT_EQ(TFS_SUCCESS, move_task->handle_complete(&message, all_complete_flag));
  EXPECT_EQ(5, move_block->get_hold_size());
  EXPECT_EQ(1, server_collect->block_count()); 
}


TEST_F(TaskTest, redundant_task_handle)
{

}
TEST_F(TaskTest, redundant_task_handle_complete)
{

}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
