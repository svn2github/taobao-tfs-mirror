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
#include "database_helper.h"

using namespace tfs::common;

namespace tfs
{
  namespace nameserver
  {
    class DataBaseHelperTest: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
          TBSYS_LOGGER.setLogLevel("debug");
        }

        static void TearDownTestCase()
        {

        }

        DataBaseHelperTest()
        {

        }

        ~DataBaseHelperTest()
        {

        }
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
        static std::string connstr;
        static std::string username;
        static std::string password;
    };

    std::string DataBaseHelperTest::connstr = "10.232.35.41:3306:tfs_name_db";
    std::string DataBaseHelperTest::username= "root";
    std::string DataBaseHelperTest::password= "root";

    TEST_F(DataBaseHelperTest, create_family)
    {
      DataBaseHelper dbhelper(connstr, username, password);
      FamilyInfo info;
      EXPECT_EQ(TFS_SUCCESS, dbhelper.create_family_id(info.family_id_));
      info.family_aid_info_ = 100;
      for (int32_t i = 0; i < 10; i++)
      {
        info.family_member_.push_back(std::pair<uint32_t, int32_t>(1000000 + i , 10+ i));
      }
      int64_t mysql_ret = 0;
      int32_t ret = dbhelper.create_family(info, mysql_ret);
      EXPECT_EQ(TFS_SUCCESS, ret);
    }

    TEST_F(DataBaseHelperTest, del_family)
    {
      DataBaseHelper dbhelper(connstr, username, password);
      int64_t family_id = 2;
      int64_t mysql_ret = 0;
      int32_t ret = dbhelper.del_family(mysql_ret, family_id);
      EXPECT_EQ(TFS_SUCCESS, ret);
    }

    TEST_F(DataBaseHelperTest, scan)
    {
      DataBaseHelper dbhelper(connstr, username, password);
      int64_t family_id = 0;
      std::vector<common::FamilyInfo> info;
      do
      {
        info.clear();
        int32_t ret = dbhelper.scan(info, family_id);
        EXPECT_EQ(TFS_SUCCESS, ret);
        std::vector<common::FamilyInfo>::const_iterator iter = info.begin();
        for (; iter != info.end(); ++iter)
        {
          std::ostringstream str;
          std::vector<std::pair<uint32_t, int32_t> >::const_iterator it = (*iter).family_member_.begin();
          for (; it != (*iter).family_member_.end(); ++it)
            str << " " << (*it).first << ":" << (*it).second << " ";
          TBSYS_LOG(DEBUG, "family id : %ld, family_aid_info: %d, pair: %s",
            (*iter).family_id_, (*iter).family_aid_info_, str.str().c_str());
          family_id = (*iter).family_id_;
        }
      }
      while (info.size() > 0);
    }
  }/** end namespace nameserver **/
}/** end namespace tfs **/


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
