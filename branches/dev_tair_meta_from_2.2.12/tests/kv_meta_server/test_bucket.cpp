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
#include "common/parameter.h"
#include "kvengine_helper.h"
#include "tairengine_helper.h"
#include "define.h"
#include "common/error_msg.h"

using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::metawithkv;

class BucketTest: public ::testing::Test
{
  public:
    BucketTest()
    {
      SYSPARAM_KVMETA.tair_master_ = "10.235.145.80:5198";
      SYSPARAM_KVMETA.tair_slave_ = "10.235.145.82:5198";
      SYSPARAM_KVMETA.tair_group_ = "group_ldbcommon";
      SYSPARAM_KVMETA.tair_object_area_ = 630;
    }
    ~BucketTest()
    {

    }
    virtual void SetUp()
    {
      kv_engine_helper_ = new TairEngineHelper();
      kv_engine_helper_->init();
    }
    virtual void TearDown()
    {
      delete kv_engine_helper_;
      kv_engine_helper_ = NULL;
    }

  protected:
    KvEngineHelper* kv_engine_helper_;
};

TEST_F(BucketTest, test_put)
{
  int ret = TFS_SUCCESS;
  string test_key1("bucketname");
  string now_time("2012-01-06");
  string value(now_time);

  KvKey key;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_BUCKET;

  ret = kv_engine_helper_->put_key(key, value, 0);

  string* tvalue = &value;

  int64_t version = -1;
  ret = kv_engine_helper_->get_key(key, tvalue, &version);
  EXPECT_EQ(*tvalue, now_time);

  ret = kv_engine_helper_->delete_key(key);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(BucketTest, test_del_with_no_object)
{
  int ret = TFS_SUCCESS;
  string test_key1("bucketname");
  string now_time("2012-01-06");
  string value(now_time);

  KvKey key;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_BUCKET;

  ret = kv_engine_helper_->put_key(key, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  ret = kv_engine_helper_->delete_key(key);
  EXPECT_EQ(ret, TFS_SUCCESS);

  string* tvalue = &value;
  int64_t version = -1;
  ret = kv_engine_helper_->get_key(key, tvalue, &version);
  EXPECT_NE(ret, TFS_SUCCESS);

}

TEST_F(BucketTest, test_del_with_object)
{
  int ret = TFS_SUCCESS;
  string test_key1("bucketname");
  string now_time("2012-01-06");
  string value(now_time);

  KvKey key;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_BUCKET;

  ret = kv_engine_helper_->put_key(key, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  //put obj
  KvKey obj_key;
  string test_key2("bucketname");
  test_key2 += KvKey::DELIMITER;
  test_key2 += "objectname";
  obj_key.key_ = test_key2.c_str();
  obj_key.key_size_ = test_key2.length();
  obj_key.key_type_ = KvKey::KEY_TYPE_OBJECT;

  ret = kv_engine_helper_->put_key(obj_key, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  ret = kv_engine_helper_->delete_key(key);
  EXPECT_EQ(ret, EXIT_DELETE_DIR_WITH_FILE_ERROR);

  ret = kv_engine_helper_->delete_key(obj_key);
  EXPECT_EQ(ret, TFS_SUCCESS);
  ret = kv_engine_helper_->delete_key(key);
  EXPECT_EQ(ret, TFS_SUCCESS);
}


TEST_F(BucketTest, test_get)
{
  int ret = TFS_SUCCESS;
  string test_key1("bucketname");
  string now_time("2012-01-06");
  string value(now_time);

  KvKey key;
  key.key_ = test_key1.c_str();
  key.key_size_ = test_key1.length();
  key.key_type_ = KvKey::KEY_TYPE_BUCKET;

  ret = kv_engine_helper_->put_key(key, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  KvKey obj_key;
  string test_key2("bucketname");
  test_key2 += KvKey::DELIMITER;
  test_key2 += "objectname";
  obj_key.key_ = test_key2.c_str();
  obj_key.key_size_ = test_key2.length();
  obj_key.key_type_ = KvKey::KEY_TYPE_OBJECT;

  ret = kv_engine_helper_->put_key(obj_key, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  KvKey obj_key1;
  string test_key3("object");
  test_key3 += KvKey::DELIMITER;
  test_key3 += "object";
  obj_key1.key_ = test_key3.c_str();
  obj_key1.key_size_ = test_key3.length();
  obj_key1.key_type_ = KvKey::KEY_TYPE_OBJECT;

  ret = kv_engine_helper_->put_key(obj_key1, value, 0);
  EXPECT_EQ(ret, TFS_SUCCESS);

  vector<string> v_object_name;
  string prefix("object");
  string start_key("objectname");
  int32_t limit = 10;
  ret = kv_engine_helper_->list_skeys(key, prefix, start_key, limit, v_object_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(v_object_name[0], start_key);

  ret = kv_engine_helper_->delete_key(obj_key);
  EXPECT_EQ(ret, TFS_SUCCESS);
  ret = kv_engine_helper_->delete_key(obj_key1);
  EXPECT_EQ(ret, TFS_SUCCESS);
  ret = kv_engine_helper_->delete_key(key);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
