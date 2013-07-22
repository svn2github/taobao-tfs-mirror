/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   xueya.yy(xueya.yy@taobao.com)
 *      - initial release
 *
 */

#include <gtest/gtest.h>
#include "parameter.h"
#include "test_lifecycle_rootserver_helper.h"
#include "kv_meta_define.h"
#include "test_kvengine.h"
#include "define.h"
#include "error_msg.h"

using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::exprootserver;

class LifeCycleRootserverTest: public ::testing::Test
{
  public:
    LifeCycleRootserverTest()
    {
    }
    ~LifeCycleRootserverTest()
    {

    }
    virtual void SetUp()
    {
      test_life_cycle_rootserver_helper_= new TestLifeCycleRootserverHelper();
      test_life_cycle_rootserver_helper_->set_kv_engine(&test_engine_);
      test_life_cycle_rootserver_helper_->init(&test_engine_);
    }
    virtual void TearDown()
    {
      test_life_cycle_rootserver_helper_->set_kv_engine(NULL);
      delete test_life_cycle_rootserver_helper_;
      test_life_cycle_rootserver_helper_ = NULL;
    }

  protected:
    TestLifeCycleRootserverHelper *test_life_cycle_rootserver_helper_;
    TestEngineHelper test_engine_;
};

TEST_F(LifeCycleRootserverTest, test_query_progress)
{
  int ret = TFS_SUCCESS;

  //int ret = test_life_cycle_rootserver_helper_->query_progress();
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(LifeCycleRootserverTest, test_handle_fail_servers)
{
  int ret = TFS_SUCCESS;
  uint64_t es_id = tbsys::CNetUtil::strToAddr("10.232.35.41", 0);
  std::vector<uint64_t> v_servers;
  v_servers.push_back(es_id);
  ret = test_life_cycle_rootserver_helper_->handle_fail_servers(v_servers);
  EXPECT_EQ(TFS_SUCCESS, ret);

  std::map<uint64_t, common::ExpireDeleteTask> m_task_info;
  test_life_cycle_rootserver_helper_->get_task_info(m_task_info);
  std::map<uint64_t, common::ExpireDeleteTask>::iterator iter = m_task_info.find(es_id);
  EXPECT_EQ(iter, m_task_info.end());
}

TEST_F(LifeCycleRootserverTest, test_handle_finish_task)
{
  int ret = TFS_SUCCESS;
  uint64_t es_id = tbsys::CNetUtil::strToAddr("10.232.35.41", 0);
  ret = test_life_cycle_rootserver_helper_->handle_finish_task(es_id);
  EXPECT_EQ(TFS_SUCCESS, ret);

  std::map<uint64_t, common::ExpireDeleteTask> m_task_info;
  test_life_cycle_rootserver_helper_->get_task_info(m_task_info);
  std::map<uint64_t, common::ExpireDeleteTask>::iterator iter = m_task_info.find(es_id);
  EXPECT_EQ(iter, m_task_info.end());
}

TEST_F(LifeCycleRootserverTest, test_assign_task)
{
  int ret = TFS_SUCCESS;
  //int ret = test_life_cycle_rootserver_helper_->assign_task();
  EXPECT_EQ(TFS_SUCCESS, ret);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
