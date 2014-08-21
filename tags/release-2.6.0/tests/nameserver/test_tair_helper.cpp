/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_database_helper.cpp 5 2012-07-20 09:55:56Z
 *
 * Authors:
 *  duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <iostream>
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Memory.hpp>
#include <time.h>
#include "tair_helper.h"

using namespace tfs::common;
using namespace tair;

namespace tfs
{
  namespace nameserver
  {
    class TairHelperTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        TairHelperTest()
        {

        }

        ~TairHelperTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
        static std::string master_ipaddr;
        static std::string slave_ipaddr;
        static std::string group_name;
        static std::string key_prefix;
        static int32_t area;
    };

    std::string TairHelperTest::master_ipaddr= "10.235.145.80:5198";
    std::string TairHelperTest::slave_ipaddr = "10.235.145.82:5198";
    std::string TairHelperTest::group_name= "group_ldbcommon";
    std::string TairHelperTest::key_prefix= "t1m";
    int32_t TairHelperTest::area= 1011;

    TEST_F(TairHelperTest, create_family)
    {
      TairHelper dbhelper(key_prefix, master_ipaddr, slave_ipaddr, group_name, area);
      EXPECT_EQ(TFS_SUCCESS, dbhelper.initialize());
      FamilyInfo info, query_info;
      EXPECT_EQ(TFS_SUCCESS, dbhelper.create_family_id(info.family_id_));
      info.family_id_ = 2;
      info.family_aid_info_ = 100;
      for (int32_t i = 0; i < 10; i++)
      {
        info.family_member_.push_back(std::pair<uint32_t, int32_t>(1000000 + i , 10+ i));
      }
      int32_t ret = dbhelper.create_family(info);
      EXPECT_EQ(TFS_SUCCESS, ret);
      if (TFS_SUCCESS == ret)
      {
        query_info.family_id_ = info.family_id_;
        ret = dbhelper.query_family(query_info);
        EXPECT_EQ(TFS_SUCCESS, ret);
      }
    }

    TEST_F(TairHelperTest, del_family)
    {
      TairHelper dbhelper(key_prefix, master_ipaddr, slave_ipaddr, group_name, area);
      EXPECT_EQ(TFS_SUCCESS, dbhelper.initialize());
      int64_t family_id = 2;
      int32_t ret = dbhelper.del_family(family_id);
      EXPECT_EQ(TFS_SUCCESS, ret);
    }

    TEST_F(TairHelperTest, scan)
    {
      TairHelper dbhelper(key_prefix, master_ipaddr, slave_ipaddr, group_name, area);
      EXPECT_EQ(TFS_SUCCESS, dbhelper.initialize());
      int64_t family_id = 0, ret = TFS_SUCCESS;
      for (int32_t i =10000; i < 10010; i++)
      {
        FamilyInfo cinfo;
        cinfo.family_id_ = i;
        cinfo.family_aid_info_ = 100;
        for (int32_t j = 0; j < 10; j++)
        {
          cinfo.family_member_.push_back(std::pair<uint32_t, int32_t>(1000000 + i  + j, 10+ i));
        }
        ret = dbhelper.create_family(cinfo);
        EXPECT_EQ(TFS_SUCCESS, ret);
      }

      std::vector<common::FamilyInfo> info;
      do
      {
        info.clear();
        ret = dbhelper.scan(info, family_id);
        EXPECT_TRUE(ret == TAIR_RETURN_SUCCESS || ret == TAIR_HAS_MORE_DATA);
        std::vector<common::FamilyInfo>::const_iterator iter = info.begin();
        for (; iter != info.end(); ++iter)
        {
          std::ostringstream str;
          std::vector<std::pair<uint64_t, int32_t> >::const_iterator it = (*iter).family_member_.begin();
          for (; it != (*iter).family_member_.end(); ++it)
            str << " " << (*it).first << ":" << (*it).second << " ";
          TBSYS_LOG(DEBUG, "family id : %ld, family_aid_info: %d, pair: %s",
            (*iter).family_id_, (*iter).family_aid_info_, str.str().c_str());
          family_id = (*iter).family_id_;
        }
      }
      while (info.size() > 0 && TAIR_HAS_MORE_DATA == ret);

      for (int32_t i =10000; i < 10010; i++)
      {
        ret = dbhelper.del_family(i);
      }
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
