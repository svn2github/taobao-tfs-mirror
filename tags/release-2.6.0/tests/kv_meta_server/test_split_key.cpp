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
 *   daoan(daoan.taobao.com)
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "tairengine_helper.h"
#include "define.h"
using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::kvmetaserver;

class SplitKeyTest: public ::testing::Test
{
  public:
    SplitKeyTest()
    {
    }
    ~SplitKeyTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(SplitKeyTest, testsplit)
{
  KvKey key;
  string test_key1("bucketname");
  test_key1 += KvKey::DELIMITER;
  test_key1 += "objectname111";
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_OBJECT;
  tair::data_entry prefix_key;
  tair::data_entry second_key;
  string str_real_prefix("bucketname");
  str_real_prefix += KvKey::DELIMITER;

  tair::data_entry real_prefix;
  real_prefix.set_data(str_real_prefix.c_str(), str_real_prefix.length());
  tair::data_entry real_second("objectname111");
  EXPECT_EQ(TFS_SUCCESS ,TairEngineHelper::split_key_for_tair(key, &prefix_key, &second_key));
  EXPECT_EQ(real_prefix, prefix_key);
  EXPECT_EQ(real_second, second_key);

}

TEST_F(SplitKeyTest, test_no_delimite)
{
  KvKey key;
  string test_key1("bucketnameobjectname111");
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_OBJECT;
  tair::data_entry prefix_key;
  tair::data_entry second_key;
  tair::data_entry real_prefix("bucketname");
  tair::data_entry real_second("objectname111");
  EXPECT_NE(TFS_SUCCESS ,TairEngineHelper::split_key_for_tair(key, &prefix_key, &second_key));

}

TEST_F(SplitKeyTest, test_0_inkey)
{
  KvKey key;
  string test_key1("buc.,*");
  test_key1 += KvKey::DELIMITER;
  test_key1 += "ketnameobjectname111";
  test_key1[2] = 0;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_OBJECT;
  tair::data_entry prefix_key;
  tair::data_entry second_key;
  tair::data_entry real_prefix;
  tair::data_entry real_second("ketnameobjectname111");
  string real_prefix_str("buc.,*");
  real_prefix_str += KvKey::DELIMITER;
  real_prefix_str[2] = 0;
  real_prefix.set_data(real_prefix_str.c_str(), real_prefix_str.length());
  EXPECT_EQ(TFS_SUCCESS ,TairEngineHelper::split_key_for_tair(key, &prefix_key, &second_key));
  EXPECT_EQ(real_prefix, prefix_key);
  EXPECT_EQ(real_second, second_key);

}
TEST_F(SplitKeyTest, test_only_prefix)
{
  KvKey key;
  string test_key1("buc.,*ketnameobjectname111");
  test_key1+= KvKey::DELIMITER;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_OBJECT;
  tair::data_entry prefix_key;
  tair::data_entry second_key;
  tair::data_entry real_prefix;
  string real_prefix_str("buc.,*ketnameobjectname111");
  real_prefix_str += KvKey::DELIMITER;
  real_prefix.set_data(real_prefix_str.c_str(), real_prefix_str.length());
  EXPECT_EQ(TFS_SUCCESS ,TairEngineHelper::split_key_for_tair(key, &prefix_key, &second_key));
  EXPECT_EQ(real_prefix, prefix_key);
  EXPECT_EQ(0, second_key.get_size());


}


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
