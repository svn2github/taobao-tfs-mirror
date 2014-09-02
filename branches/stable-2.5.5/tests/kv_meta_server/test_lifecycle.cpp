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
 *   daoan(daoan@taobao.com)
 *      - initial release
 *
 */

#include <gtest/gtest.h>
#include "parameter.h"
#include "test_life_cycle_helper.h"
#include "kv_meta_define.h"
#include "test_kvengine.h"
#include "define.h"
#include "error_msg.h"

using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::kvmetaserver;

class LifeCycleMetaTest: public ::testing::Test
{
  public:
    LifeCycleMetaTest()
    {
    }
    ~LifeCycleMetaTest()
    {

    }
    virtual void SetUp()
    {
      test_life_cycle_helper_= new TestLifeCycleHelper();
      test_life_cycle_helper_->set_kv_engine(&test_engine_);
      test_life_cycle_helper_->init(&test_engine_);
    }
    virtual void TearDown()
    {
      test_life_cycle_helper_->set_kv_engine(NULL);
      delete test_life_cycle_helper_;
      test_life_cycle_helper_ = NULL;
    }

  protected:
    TestLifeCycleHelper *test_life_cycle_helper_;
    TestEngineHelper test_engine_;
};

TEST_F(LifeCycleMetaTest, test_serialize_name_expiretime_key)
{
  common::KvKey key;
  char data[100];
  int ret = 0;
  ret = ExpireDefine::serialize_name_expiretime_key(ExpireDefine::FILE_TYPE_RAW_TFS,
      "addds", &key, data, 100);
  EXPECT_EQ(TFS_SUCCESS, ret);
  int8_t key_type;
  int32_t file_type;
  string filename;
  ret = ExpireDefine::deserialize_name_expiretime_key(data,100,
      &key_type, &file_type, &filename);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(KvKey::KEY_TYPE_NAME_EXPTIME, key_type);
  EXPECT_EQ(ExpireDefine::FILE_TYPE_RAW_TFS, file_type);
  EXPECT_EQ("addds", filename);
}

TEST_F(LifeCycleMetaTest, test_serialize_es_stat_key)
{
  common::KvKey key;
  char data[100];
  int ret = 0;
  uint64_t es_id = tbsys::CNetUtil::strToAddr("10.232.35.41", 0);
  int32_t num_es = 10;
  int32_t task_time = 111000;
  int32_t hash_bucket_num = 200;
  int64_t sum_file_num = 0x7fffffffffffffffL;
  ret = ExpireDefine::serialize_es_stat_key(es_id, num_es, task_time, hash_bucket_num, sum_file_num, &key, data, 100);
  EXPECT_EQ(TFS_SUCCESS, ret);
  uint64_t des_es_id;
  int32_t des_num_es;
  int32_t des_task_time;
  int32_t des_hash_bucket_num;
  int64_t des_sum_file_num;
  ret = ExpireDefine::deserialize_es_stat_key(data, 100, &des_es_id, &des_num_es, &des_task_time, &des_hash_bucket_num, &des_sum_file_num);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(des_es_id, es_id);
  EXPECT_EQ(des_num_es, num_es);
  EXPECT_EQ(des_task_time, task_time);
  EXPECT_EQ(des_hash_bucket_num, hash_bucket_num);
  EXPECT_EQ(des_sum_file_num, sum_file_num);
}

TEST_F(LifeCycleMetaTest, test_serialize_name_expiretime_value)
{
  common::KvMemValue value;
  char data[100];
  int ret = 0;
  int invlaid_time = 500;
  ret = ExpireDefine::serialize_name_expiretime_value(invlaid_time,
      &value, data, 100);
  EXPECT_EQ(TFS_SUCCESS, ret);
  int32_t invlaid_time2 = 0;
  ret = ExpireDefine::deserialize_name_expiretime_value(data,100,
      &invlaid_time2);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(invlaid_time, invlaid_time2);
}
TEST_F(LifeCycleMetaTest, serialize_exptime_app_key)
{
  common::KvKey key;
  char data[100];
  int ret = 0;
  ret = ExpireDefine::serialize_exptime_app_key(10, 20, 30, ExpireDefine::FILE_TYPE_RAW_TFS,
      "file_name", &key, data, 100);
  EXPECT_EQ(TFS_SUCCESS, ret);
  int32_t  day =0;
  int32_t  hour =0;
  int32_t mod=0;
  int32_t type=0;
  string file_name;
  ret = ExpireDefine::deserialize_exptime_app_key(data,100,
      &day, &hour, &mod, &type, &file_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(10, day);
  EXPECT_EQ(20, hour);
  EXPECT_EQ(30, mod);
  EXPECT_EQ(ExpireDefine::FILE_TYPE_RAW_TFS, type);
  EXPECT_EQ("file_name", file_name);
}

TEST_F(LifeCycleMetaTest, test_set_life_cycle)
{
  int ret = 0;
  ret = test_life_cycle_helper_->set_file_lifecycle(1, "adbs",50,"appkey");
  EXPECT_EQ(TFS_SUCCESS, ret);
  int32_t invalide_time;
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "adbs", &invalide_time);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(50, invalide_time);
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "adbsf", &invalide_time);
  EXPECT_EQ(EXIT_KV_RETURN_DATA_NOT_EXIST, ret);

}
TEST_F(LifeCycleMetaTest, rm_life_cycle)
{
  int ret = 0;
  ret = test_life_cycle_helper_->set_file_lifecycle(1, "adbs",50,"appkey");
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_life_cycle_helper_->rm_life_cycle(1, "adbs");
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_life_cycle_helper_->rm_life_cycle(1, "adbs");
  EXPECT_EQ(TFS_SUCCESS, ret);
  int32_t invalide_time;
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "adbs", &invalide_time);
  EXPECT_EQ(EXIT_KV_RETURN_DATA_NOT_EXIST, ret);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
