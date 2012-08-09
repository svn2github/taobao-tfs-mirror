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
#include "server_collect.h"
#include "block_collect.h"
#include "nameserver.h"
#include "block_manager.h"
#include "layout_manager.h"
#include "task_manager.h"
#include "task.h"

#include "message/compact_block_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace nameserver
  {
    class TaskTest:public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }
        static void TearDownTestCase()
        {

        }

        TaskTest():
          manager_(ns_)
      {

      }
        virtual ~TaskTest()
        {

        }
        virtual void SetUp()
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
          SYSPARAM_NAMESERVER.heart_interval_ = 2;
          SYSPARAM_NAMESERVER.replicate_wait_time_ = 10;
          SYSPARAM_NAMESERVER.task_expired_time_ = 1;
          SYSPARAM_NAMESERVER.group_mask_= 0xffffffff;
        }

        virtual void TearDown()
        {

        }
        NameServer ns_;
        LayoutManager manager_;
    };

    TEST_F(TaskTest, compact_handle)
    {

    }

    TEST_F(TaskTest, replicate_task_handle_complete)
    {

    }

    TEST_F(TaskTest, marshalling_handle_complete)
    {
      const uint64_t BASE_SERVER_ID = 0xFFFFFFFF;
      const uint32_t BASE_BLOCK_ID  = 100;
      //int64_t family_id = 100;
      int32_t family_aid_info;
      const int32_t DATA_MEMBER_NUM = 5;
      const int32_t CHECK_MEMBER_NUM = 2;
      SET_DATA_MEMBER_NUM(family_aid_info,DATA_MEMBER_NUM);
      SET_CHECK_MEMBER_NUM(family_aid_info,CHECK_MEMBER_NUM);
      SET_MASTER_INDEX(family_aid_info, DATA_MEMBER_NUM + 1);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, DATA_MEMBER_NUM);
      FamilyMemberInfo fminfo[MAX_MARSHALLING_NUM];
      for (int32_t i = 0; i < DATA_MEMBER_NUM; ++i)
      {
        fminfo[i].server_ = BASE_SERVER_ID + i;
        fminfo[i].block_  = BASE_BLOCK_ID + i;
      }
    }
  }
}
int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
