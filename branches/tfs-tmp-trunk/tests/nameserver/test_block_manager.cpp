/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_block_manager.cpp 5 2011-03-20 15:55:56Z
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
#include "block_manager.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    class BlockManagerTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {

        }
        BlockManagerTest():
          layout_manager_(ns_),
          block_manager_(layout_manager_)
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
        }

        ~BlockManagerTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {
          block_manager_.clear_();

        }
      protected:
        NameServer ns_;
        LayoutManager layout_manager_;
        BlockManager block_manager_;
    };

    TEST_F(BlockManagerTest, insert_remove_get_exist)
    {
      bool set = false;
      const uint32_t id = 100;
      time_t now = Func::get_monotonic_time();

      BlockCollect* block = block_manager_.insert_(id, now, set);
      EXPECT_TRUE(NULL != block);
      int32_t index = block_manager_.get_chunk_(id);
      BlockCollect* result = *block_manager_.blocks_[index]->find(block);
      EXPECT_TRUE(NULL != result);
      EXPECT_TRUE(result->id() == block->id());

      result = block_manager_.get_(id);
      EXPECT_TRUE(NULL != result);
      EXPECT_TRUE(result->id() == block->id());

      result = block_manager_.get_(101);
      EXPECT_TRUE(NULL == result);
      EXPECT_FALSE(block_manager_.exist(101));
      EXPECT_TRUE(block_manager_.exist(id));

      EXPECT_TRUE(block_manager_.remove_(block->id()));
      EXPECT_FALSE(block_manager_.exist(id));
    }

    TEST_F(BlockManagerTest, get_servers)
    {
      bool set = false;
      const uint32_t id = 100;
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      bool master = false, writable = false;

      BlockCollect* block = block_manager_.insert_(id, now, set);
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(id == block->id());
      uint64_t server = info.id_;
      EXPECT_EQ(TFS_SUCCESS, block->add(writable, master, server, now));
      EXPECT_EQ(1, block->get_servers_size());

      uint64_t server2 = ++info.id_;
      EXPECT_EQ(TFS_SUCCESS, block->add(writable, master, server2, now));
      EXPECT_EQ(2, block->get_servers_size());

      uint64_t result[SYSPARAM_NAMESERVER.max_replication_];
      ArrayHelper<uint64_t> helper(SYSPARAM_NAMESERVER.max_replication_, result);

      block_manager_.get_servers(helper, id);
      EXPECT_EQ(2, helper.get_array_index());

      helper.clear();
      block_manager_.get_servers(helper, block);
      EXPECT_EQ(2, helper.get_array_index());
    }

    TEST_F(BlockManagerTest, build_relation)
    {
      bool set = false;
      const uint32_t id = 100;
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;

      bool master = false, writable = false;

      BlockCollect* block = block_manager_.insert_(id, now, set);
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(id == block->id());
      uint64_t server = info.id_;

      EXPECT_EQ(EXIT_PARAMETER_ERROR, block_manager_.build_relation_(NULL, writable, master, server, now));
      EXPECT_EQ(EXIT_PARAMETER_ERROR, block_manager_.build_relation_(block, writable, master,INVALID_SERVER_ID, now));
      EXPECT_EQ(TFS_SUCCESS, block_manager_.build_relation_(block, writable, master, server, now));

      uint64_t result = block->get_server();
      EXPECT_TRUE(result == server);
    }

    TEST_F(BlockManagerTest, update_block_info)
    {
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      BlockInfoV2 blkinfo;
      memset(&blkinfo, 0, sizeof(blkinfo));
      blkinfo.block_id_ = 100;
      blkinfo.version_  = 10;
      ServerCollect server(layout_manager_, info, now);
      bool master = false, writable = false, isnew = false;

      BlockCollect* block = NULL;
      EXPECT_EQ(EXIT_PARAMETER_ERROR,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, NULL, now, false));
      EXPECT_EQ(EXIT_BLOCK_NOT_FOUND,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, false));

      EXPECT_EQ(TFS_SUCCESS,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, true));
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(blkinfo.block_id_ == block->id());
      EXPECT_TRUE(blkinfo.version_ == block->version());

      block = NULL;
      blkinfo.version_ = blkinfo.version_ / 2;
      EXPECT_EQ(EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, true));
      EXPECT_EQ(EXIT_UPDATE_BLOCK_INFO_VERSION_ERROR,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, false));
      EXPECT_TRUE(NULL == block);

      blkinfo.version_ = blkinfo.version_ * 2 + 1;
      EXPECT_EQ(TFS_SUCCESS,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, true));
      EXPECT_EQ(TFS_SUCCESS,block_manager_.update_block_info(block, isnew, writable, master, blkinfo, &server, now, false));
      EXPECT_TRUE(NULL != block);
      EXPECT_TRUE(blkinfo.block_id_ == block->id());
      EXPECT_TRUE(blkinfo.version_ == block->version());
    }

    TEST_F(BlockManagerTest, update_relation)
    {
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      bool isnew = false;
      ServerCollect server(layout_manager_, info, now);
      ServerManager server_manager(layout_manager_);
      EXPECT_EQ(TFS_SUCCESS, server_manager.add(info, now, isnew));
      info.id_++;
      ServerCollect server2(layout_manager_, info, now);
      EXPECT_EQ(TFS_SUCCESS, server_manager.add(info, now, isnew));

      BlockInfoV2 blocks[10000];
      common::ArrayHelper<BlockInfoV2> helper(100000, blocks);
      int32_t COUNT = random() %  10000;
      int32_t blk_start = random() % 100000000 + 100000;
      BlockInfoV2 tmp;
      for (int32_t i = 0; i < COUNT; i++, blk_start++)
      {
        memset(&tmp, 0, sizeof(tmp));
        tmp.block_id_ = blk_start;
        tmp.version_  = random() % COUNT + 1;
        tmp.file_count_   = random() % 10000 + 1;
        tmp.size_ = random() % 10000000 + 1;
        tmp.del_file_count_ = random() % 10000;
        tmp.del_size_ = random() % 100000;
        helper.push_back(tmp);
      }
      std::vector<uint64_t> expires;
      EXPECT_EQ(TFS_SUCCESS, block_manager_.update_relation(expires, &server, helper, now));

      BlockCollect* pblock = NULL;
      for (int32_t index = 0; index < helper.get_array_index(); ++index)
      {
        tmp = *helper.at(index);
        pblock = block_manager_.get(tmp.block_id_);
        EXPECT_TRUE(NULL != pblock);
        EXPECT_TRUE(pblock->id() == tmp.block_id_);
        EXPECT_TRUE(pblock->version() > 0);
        EXPECT_TRUE(pblock->size() > 0);
        EXPECT_EQ(1, pblock->get_servers_size());
      }

      EXPECT_EQ(TFS_SUCCESS, block_manager_.update_relation(expires, &server2, helper, now));
      for (int32_t index = 0; index < helper.get_array_index(); ++index)
      {
        tmp = *helper.at(index);
        pblock = block_manager_.get(tmp.block_id_);
        EXPECT_TRUE(NULL != pblock);
        EXPECT_TRUE(pblock->id() == tmp.block_id_);
        EXPECT_TRUE(pblock->version() > 0);
        EXPECT_TRUE(pblock->size() > 0);
        EXPECT_EQ(2, pblock->get_servers_size());
      }
      //TODO
      //其他的测试待server_manager&delete_queue测试完了再测试
    }

    TEST_F(BlockManagerTest, scan)
    {
      time_t now = Func::get_monotonic_time();
      DataServerStatInfo info;
      memset(&info, 0, sizeof(info));
      info.status_ = DATASERVER_STATUS_ALIVE;
      info.id_ = 0xfffffff0;
      bool set = false, writable = false, master = false;

      BlockCollect* block = NULL;
      int32_t COUNT = random() %  10000 + MAX_BLOCK_CHUNK_NUMS;
      uint32_t blk_start= random() % 100000000 + 100;
      for (int32_t i = 0; i < COUNT; i++, blk_start++, block = NULL)
      {
        block = block_manager_.insert_(blk_start, now, set);
        EXPECT_TRUE(NULL != block);
        EXPECT_EQ(TFS_SUCCESS, block_manager_.build_relation(block, writable, master, info.id_, now));
      }

      SSMScanParameter param;
      memset(&param, 0, sizeof(param));

      int32_t total = 0;
      int32_t should = 128;
      int32_t next = 0;
      int32_t actual = 0;

      bool all_over = false;
      bool cutover = true;
      while (!all_over)
      {
        actual = block_manager_.scan(param, next, all_over, cutover, should);
        if (!cutover)
            param.addition_param1_ = param.addition_param2_;
        total += actual;
      }
      EXPECT_EQ(total, COUNT);
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
