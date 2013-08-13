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
#include "ds_lease_message.h"
#include "array_helper.h"
#include "dataservice.h"
#include "writable_block_manager.h"

using namespace tbutil;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace dataserver
  {
    class TestWritableBlockManager: public ::testing::Test
    {
      public:
        TestWritableBlockManager()
        {
        }
        ~TestWritableBlockManager()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
    };

    TEST_F(TestWritableBlockManager, general)
    {
      DataService service;
      WritableBlockManager manager(service);
      uint64_t block_id = 100;
      uint64_t servers[] = {1001, 1002};
      ArrayHelper<uint64_t> helper(2, servers, 2);
      WritableBlock* block = NULL;

      // insert & get
      block = manager.insert(block_id, helper, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
      ASSERT_EQ(block_id, block->get_block_id());
      ASSERT_EQ(block, manager.get(block_id, BLOCK_WRITABLE));

      // insert & get
      block_id = 200;
      block = manager.insert(block_id, helper, BLOCK_UPDATE);
      ASSERT_TRUE(NULL != block);
      ASSERT_EQ(block_id, block->get_block_id());
      ASSERT_EQ(block, manager.get(block_id, BLOCK_UPDATE));

      // insert & get
      block_id = 300;
      block = manager.insert(block_id, helper, BLOCK_EXPIRED);
      ASSERT_TRUE(NULL != block);
      ASSERT_EQ(block_id, block->get_block_id());
      ASSERT_EQ(block, manager.get(block_id, BLOCK_EXPIRED));

      // remove from expired list & get
      block_id = 300;
      block = manager.remove(block_id, BLOCK_EXPIRED);
      ASSERT_TRUE(NULL != block);
      ASSERT_TRUE(manager.empty(BLOCK_EXPIRED));
      ASSERT_TRUE(NULL == manager.get(block_id, BLOCK_EXPIRED));

      // remove from writable list
      block_id = 100;
      block = manager.remove(block_id, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
      ASSERT_TRUE(manager.empty(BLOCK_WRITABLE));
      block = manager.get(block_id, BLOCK_EXPIRED); // shoule moved to expired list
      ASSERT_TRUE(NULL != block);

      // remove from updatable list
      block_id = 200;
      block = manager.remove(block_id, BLOCK_UPDATE);
      ASSERT_TRUE(NULL != block);
      ASSERT_TRUE(manager.empty(BLOCK_UPDATE));
      block = manager.get(block_id, BLOCK_EXPIRED); // should moved to expired list
      ASSERT_TRUE(NULL != block);

      ASSERT_EQ(2, manager.size(BLOCK_EXPIRED));
    }

    TEST_F(TestWritableBlockManager, alloc)
    {
      DataService service;
      WritableBlockManager manager(service);
      uint64_t block_id = 100;
      uint64_t servers[] = {1001, 1002};
      ArrayHelper<uint64_t> helper(2, servers, 2);
      WritableBlock* block = NULL;
      int ret = TFS_SUCCESS;

      // no writable block currently
      ret = manager.alloc_writable_block(block);
      ASSERT_NE(TFS_SUCCESS, ret);

      // add a writable block
      block = manager.insert(block_id, helper, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);

      // alloc again
      ret = manager.alloc_writable_block(block);
      ASSERT_EQ(TFS_SUCCESS, ret);
      ASSERT_TRUE(block->get_use_flag());

      // free it
      manager.free_writable_block(block_id);
      block = manager.get(block_id, BLOCK_WRITABLE);
      ASSERT_TRUE(!block->get_use_flag());
    }

    TEST_F(TestWritableBlockManager, apply_callback)
    {
      // consturct an apply block response packet
      DsApplyBlockResponseMessage response;
      BlockLease* leases = response.get_block_lease();
      response.set_size(2);
      leases[0].block_id_ = 100;
      leases[0].result_ = TFS_SUCCESS;
      leases[1].block_id_ = 200;
      leases[1].result_ = TFS_SUCCESS;

      // call apply_block_callback
      DataService service;
      WritableBlockManager manager(service);
      WritableBlock* block = NULL;
      manager.apply_block_callback(&response);

      ASSERT_EQ(2, (int)manager.size(BLOCK_WRITABLE));
      block = manager.get(100, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
      block = manager.get(200, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
    }

    TEST_F(TestWritableBlockManager, giveup_callback)
    {
      // consturct an apply block response packet
      DsGiveupBlockResponseMessage response;
      BlockLease* leases = response.get_block_lease();
      response.set_size(2);
      leases[0].block_id_ = 100;
      leases[0].result_ = TFS_SUCCESS;
      leases[1].block_id_ = 200;
      leases[1].result_ = TFS_SUCCESS;

      uint64_t servers[] = {1001, 1002};
      ArrayHelper<uint64_t> helper(2, servers, 2);

      // call giveup_block_callback
      DataService service;
      WritableBlockManager manager(service);
      manager.insert(100, helper, BLOCK_EXPIRED);
      manager.insert(200, helper, BLOCK_EXPIRED);
      ASSERT_EQ(2, manager.size(BLOCK_EXPIRED));
      manager.giveup_block_callback(&response); // expired block should be removed
      ASSERT_EQ(0, (int)manager.size(BLOCK_EXPIRED));
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

