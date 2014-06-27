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
#include "family_collect.h"
#include "layout_manager.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {

    class FamilyCollectTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        FamilyCollectTest()
        {

        }

        ~FamilyCollectTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(FamilyCollectTest, add)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      FamilyCollect family(family_id, family_aid_info, 0);
      for (int32_t i = 0; i <7; i++)
      {
        EXPECT_EQ(family.add((100+i), 1), TFS_SUCCESS);
      }
      EXPECT_EQ(family.add((100+8), 1), EXIT_OUT_OF_RANGE);
      family.dump(TBSYS_LOG_LEVEL_DEBUG);
    }

    TEST_F(FamilyCollectTest, update)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      FamilyCollect family(family_id, family_aid_info, 0);
      for (int32_t i = 0; i <7; i++)
      {
        EXPECT_EQ(family.add((100+i), 1), TFS_SUCCESS);
      }
      family.dump(TBSYS_LOG_LEVEL_DEBUG);
      EXPECT_EQ(family.update(101, 2), TFS_SUCCESS);
      EXPECT_EQ(family.update(1000, 2), EXIT_NO_BLOCK);
      int32_t version = 0;
      EXPECT_EQ(family.get_version(version, 101), TFS_SUCCESS);
      EXPECT_EQ(version, 2);
    }

    TEST_F(FamilyCollectTest, exit)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      FamilyCollect family(family_id, family_aid_info, 0);
      for (int32_t i = 0; i <7; i++)
      {
        EXPECT_EQ(family.add((100+i), 1), TFS_SUCCESS);
      }
      EXPECT_TRUE(family.exist(100));
      EXPECT_TRUE(family.exist(1000));
    }

    TEST_F(FamilyCollectTest, get_members)
    {
      int64_t family_id = 100;
      int32_t family_aid_info = 0;
      SET_DATA_MEMBER_NUM(family_aid_info, 5);
      SET_CHECK_MEMBER_NUM(family_aid_info, 2);
      SET_MARSHALLING_TYPE(family_aid_info, 1);
      SET_MASTER_INDEX(family_aid_info, 5);
      FamilyCollect family(family_id, family_aid_info, 0);
      for (int32_t i = 0; i <7; i++)
      {
        EXPECT_EQ(family.add((100+i), 1), TFS_SUCCESS);
      }
      family.dump(TBSYS_LOG_LEVEL_DEBUG);
      std::pair<uint64_t , int32_t> members[7];
      ArrayHelper<std::pair<uint64_t , int32_t> > helper(7, members);

      family.get_members(helper);
      std::ostringstream str;
      str <<"family_id: " << family.family_id_ <<",data_member_num: " << family.get_data_member_num() << ",check_member_num: "<<
          family.get_check_member_num() << ",code_type:" << family.get_code_type() << ",master_index: " << family.get_master_index();
      str << ", data_members: ";
      int32_t i = 0;
      for (i = 0; i < family.get_data_member_num(); ++i)
      {
        str <<members[i].first <<":" << members[i].second<< ",";
      }
      str << ", check_members: ";
      const int32_t MEMBER_NUM = family.get_data_member_num() + family.get_check_member_num();
      for (; i < MEMBER_NUM; ++i)
      {
          str <<members[i].first <<":" << members[i].second<< ",";
      }
      TBSYS_LOG(DEBUG, "%s", str.str().c_str());
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
