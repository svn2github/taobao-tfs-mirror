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
#include "writable_block.h"

using namespace tbutil;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace dataserver
  {
    class TestLeaseManager: public ::testing::Test
    {
      public:
        TestLeaseManager()
        {
        }
        ~TestLeaseManager()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
    };

    TEST_F(TestLeaseManager, process_apply_response)
    {
      DsApplyLeaseResponseMessage response;
      LeaseMeta meta;
      meta.lease_id_ = 100;
      meta.lease_expire_time_ = 60;
      response.set_lease_meta(meta);

      DataService service;
      LeaseManager manager(service);
      manager.process_apply_response(&response);
      LeaseMeta& new_meta = manager.lease_meta_;
      int32_t last_renew_time = manager.last_renew_time_;
      ASSERT_EQ(new_meta.lease_id_, meta.lease_id_);
      ASSERT_EQ(new_meta.lease_expire_time_ , meta.lease_expire_time_);
      ASSERT_LT(Func::get_monotonic_time() - last_renew_time, 1);
    }

    TEST_F(TestLeaseManager, process_renew_response)
    {
      DsRenewLeaseResponseMessage response;
      BlockLease* leases = response.get_block_lease();
      response.set_size(2);

      // renew wrong
      leases[0].block_id_ = 100;
      leases[0].servers_[0] = 1001;
      leases[0].servers_[1] = 1002;
      leases[0].size_ = 2;
      leases[0].result_ = -1;

      // renew success
      leases[1].block_id_ = 200;
      leases[1].servers_[0] = 2001;
      leases[1].servers_[1] = 2002;
      leases[1].size_ = 2;
      leases[1].result_ = TFS_SUCCESS;

      uint64_t servers[] = {3001, 3002};
      ArrayHelper<uint64_t> helper(2, servers, 2);

      WritableBlock* block = NULL;
      DataService service;
      LeaseManager manager(service);
      block = manager.get_writable_block_manager().insert(100, helper, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
      block = manager.get_writable_block_manager().insert(200, helper, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);

      manager.process_renew_response(&response);
      block = manager.get_writable_block_manager().get(100, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL == block);
      block = manager.get_writable_block_manager().get(200, BLOCK_WRITABLE);
      ASSERT_TRUE(NULL != block);
      VUINT64 servers2;
      block->get_servers(servers2);
      ASSERT_EQ(2001, (int)servers2[0]);
      ASSERT_EQ(2002, (int)servers2[1]);

    }

  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

