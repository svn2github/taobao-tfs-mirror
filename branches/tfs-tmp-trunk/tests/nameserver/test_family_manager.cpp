/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_family_collect.cpp 5 2012-07-20 09:55:56Z
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
#include "nameserver.h"
#include "family_manager.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    class FamilyManagerTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        FamilyManagerTest():
          layout_manager_(ns_),
          family_manager_(layout_manager_)
        {

        }

        ~FamilyManagerTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
        NameServer ns_;
        LayoutManager layout_manager_;
        FamilyManager family_manager_;
    };

    TEST_F(FamilyManagerTest, MarshallingItem_get)
    {
      const uint64_t SERVER_ID = 0xfffffffff;
      const uint64_t BLOCK_ID  = 100;
      FamilyManager::MarshallingItem item;
      memset(&item , 0, sizeof(item));
      EXPECT_EQ(TFS_SUCCESS, item.insert(SERVER_ID, BLOCK_ID));
      std::pair<uint64_t, uint64_t> out;
      EXPECT_FALSE(item.get(out, 0xffff, 1));
      EXPECT_TRUE(item.get(out, SERVER_ID, BLOCK_ID));
      EXPECT_EQ(SERVER_ID, out.first);
      EXPECT_EQ(BLOCK_ID, out.second);
      EXPECT_EQ(1, item.slot_num_);
    }

    TEST_F(FamilyManagerTest, MarshallingItem_choose_item_random)
    {
      const uint64_t SERVER_ID = 0xfffffffff;
      const uint64_t BLOCK_ID  = 100;
      FamilyManager::MarshallingItem item;
      memset(&item , 0, sizeof(item));

      std::pair<uint64_t, uint64_t> out;
      EXPECT_EQ(EXIT_MARSHALLING_ITEM_QUEUE_EMPTY, item.choose_item_random(out));
      EXPECT_EQ(0, item.slot_num_);

      EXPECT_EQ(TFS_SUCCESS, item.insert(SERVER_ID, BLOCK_ID));
      EXPECT_TRUE(item.get(out, SERVER_ID, BLOCK_ID));
      EXPECT_EQ(SERVER_ID, out.first);
      EXPECT_EQ(BLOCK_ID, out.second);
      EXPECT_EQ(1, item.slot_num_);

      memset(&out, 0, sizeof(out));
      EXPECT_EQ(TFS_SUCCESS, item.choose_item_random(out));
      EXPECT_EQ(SERVER_ID, out.first);
      EXPECT_EQ(BLOCK_ID, out.second);
      EXPECT_EQ(EXIT_MARSHALLING_ITEM_QUEUE_EMPTY, item.choose_item_random(out));
    }

    TEST_F(FamilyManagerTest, insert_remove_get)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      const int32_t MEMBER_NUM = 7;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      std::pair<uint64_t, int32_t> members[MEMBER_NUM];
      common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MEMBER_NUM, members);
      std::pair<uint64_t, int32_t> item;
      for (int32_t i = 0; i <MEMBER_NUM; i++)
      {
        helper.push_back(std::make_pair(100 + i, i));
      }
      time_t now = Func::get_monotonic_time();
      EXPECT_EQ(TFS_SUCCESS, family_manager_.insert_(family_id, family_aid_info, helper, now));
      FamilyCollect* family = family_manager_.get_(family_id);
      EXPECT_TRUE(NULL != family);
      EXPECT_EQ(EXIT_ELEMENT_EXIST, family_manager_.insert_(family_id, family_aid_info, helper, now));
      FamilyCollect* family2 = family_manager_.get_(family_id);
      EXPECT_TRUE(NULL != family2);
      EXPECT_TRUE(family == family2);
      std::pair<uint64_t, int32_t> results[MEMBER_NUM];
      common::ArrayHelper<std::pair<uint64_t, int32_t> > helper2(MEMBER_NUM, results);
      EXPECT_EQ(EXIT_NO_FAMILY, family_manager_.get_members_(helper2, 101));
      EXPECT_EQ(TFS_SUCCESS, family_manager_.get_members_(helper2, family_id));
      EXPECT_EQ(MEMBER_NUM , helper2.get_array_index());
      for (int32_t i = 0; i < MEMBER_NUM; ++i)
      {
        std::pair<uint64_t, int32_t> item = *helper.at(i);
        std::pair<uint64_t, int32_t> item2 = *helper2.at(i);
        EXPECT_EQ(item.first, item2.first);
        EXPECT_EQ(item.second, item2.second);
      }

      GCObject* object = NULL;
      EXPECT_EQ(EXIT_NO_FAMILY, family_manager_.remove(object, 101));
      EXPECT_TRUE(NULL == object);
      EXPECT_EQ(TFS_SUCCESS, family_manager_.remove(object, family_id));
      EXPECT_TRUE(NULL != object);
      tbsys::gDelete(object);
    }

    TEST_F(FamilyManagerTest, scan)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      const int32_t MEMBER_NUM = 7;
      const int32_t MAX_FAMILY_NUM = 100;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      for (int32_t j = 0; j < MAX_FAMILY_NUM; ++j)
      {
        std::pair<uint64_t, int32_t> members[MEMBER_NUM];
        common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MEMBER_NUM, members);
        std::pair<uint64_t, int32_t> item;
        for (int32_t i = 0; i <MEMBER_NUM; i++)
        {
          helper.push_back(std::make_pair(family_id + i, i));
        }
        time_t now = Func::get_monotonic_time();
        EXPECT_EQ(TFS_SUCCESS, family_manager_.insert_(family_id + j, family_aid_info, helper, now));
        FamilyCollect* family = family_manager_.get_(family_id);
        EXPECT_TRUE(NULL != family);
      }

      int64_t begin = 0;
      int32_t total = 0;
      const int32_t MAX_QUERY_NUM = 20;
      FamilyCollect* families[MAX_QUERY_NUM];
      common::ArrayHelper<FamilyCollect*> result(MAX_QUERY_NUM, families);
      for (int32_t k = 0; k < MAX_FAMILY_NUM / MAX_QUERY_NUM; k++)
      {
        result.clear();
        family_manager_.scan(result, begin, MAX_QUERY_NUM);
        total += result.get_array_index();
        for (int32_t m = 0; m < result.get_array_index(); ++m)
        {
          FamilyCollect* family = *result.at(m);
          EXPECT_TRUE(NULL != family);
          EXPECT_EQ(((k * MAX_QUERY_NUM + m) + family_id), family->get_family_id());
          std::pair<uint64_t, int32_t> results2[MEMBER_NUM];
          common::ArrayHelper<std::pair<uint64_t, int32_t> > helper2(MEMBER_NUM, results2);
          family->get_members(helper2);
          for (int32_t n = 0; n < helper2.get_array_index(); ++n)
          {
            std::pair<uint64_t, int32_t> item2 = *helper2.at(n);
            EXPECT_EQ((uint64_t)(family_id + n), item2.first);
            EXPECT_EQ(n, item2.second);
          }
        }
      }
      EXPECT_EQ(MAX_FAMILY_NUM, total);
    }

    TEST_F(FamilyManagerTest, create_family_choose_data_members)
    {
      std::pair<uint64_t, uint64_t> members[MAX_MARSHALLING_NUM];
      common::ArrayHelper<std::pair<uint64_t, uint64_t> > helper(MAX_MARSHALLING_NUM, members);
      int32_t data_member = MAX_DATA_MEMBER_NUM;
      EXPECT_EQ(TFS_SUCCESS, family_manager_.create_family_choose_data_members(helper, data_member));
      EXPECT_EQ(0, helper.get_array_index());

      const uint64_t SERVER_ID = 0xfffffff;
      const uint32_t BLOCK_ID  = 100;
      const uint32_t RACK_ID   = 0xffff;
      time_t now = Func::get_monotonic_time();

      for (int32_t i = 0; i < MAX_RACK_NUM; i++)
      {
        int32_t random_index = random() % (MAX_SINGLE_RACK_SERVER_NUM - 10) + 10;
        int32_t rack_id = RACK_ID + i;
        for (int32_t j = 0; j  < random_index; ++j)
        {
          layout_manager_.get_block_manager().insert( BLOCK_ID + i + j, now);
          EXPECT_EQ(TFS_SUCCESS, family_manager_.push_block_to_marshalling_queues(rack_id, SERVER_ID + i + j , BLOCK_ID + i + j));
        }
      }
      EXPECT_EQ(MAX_RACK_NUM, family_manager_.marshalling_queue_.size());
      EXPECT_EQ(TFS_SUCCESS, family_manager_.create_family_choose_data_members(helper, data_member));
      EXPECT_EQ(data_member, helper.get_array_index());
      for (int32_t i = 0; i < helper.get_array_index(); ++i)
      {
        std::pair<uint64_t, uint64_t> item = *helper.at(i);
        for (int32_t j = i + 1; j < helper.get_array_index(); j++)
        {
          std::pair<uint64_t, uint64_t> item2 = *helper.at(j);
          EXPECT_TRUE(item.first != item2.first);
          EXPECT_TRUE(item.second != item2.second);
        }
      }
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
