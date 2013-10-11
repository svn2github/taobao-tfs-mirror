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
 *   qixiao(qixiao@alibaba-inc.com)
 *      - initial release
 *
 */

#include <gtest/gtest.h>
#include "parameter.h"
#include "test_lifecycle_expireserver_helper.h"
#include "kv_meta_define.h"
#include "test_kvengine.h"
#include "define.h"
#include "error_msg.h"

using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::expireserver;
using namespace tfs::kvmetaserver;

int TestCleanTaskHelper::get_note(const uint64_t locak_ip, const int32_t num_es, const int32_t task_time,
                                  const int32_t hash_bucket_num, const int64_t sum_file_num)
{
    int ret = TFS_SUCCESS;
    char *key_buff = NULL;
    KvKey key;
    key_buff= (char*) malloc(512);
    KvValue *kv_value = NULL;

    if (NULL == key_buff)
    {
      ret = TFS_ERROR;
    }
    if (TFS_SUCCESS == ret)
    {
      ret = ExpireDefine::serialize_es_stat_key(locak_ip, num_es,
                                task_time, hash_bucket_num,
                                sum_file_num, &key,
                                key_buff, 512);
    }
    if (TFS_SUCCESS == ret)
    {
      ret = kv_engine_helper_->get_key(911, key, &kv_value, 0);
    }

    if (NULL != kv_value)
    {
      free(kv_value);
    }
    if (NULL != key_buff)
    {
      free(key_buff);
      key_buff = NULL;
    }
    return ret;
}

class LifeCycleExpireTest: public ::testing::Test
{
  public:
    LifeCycleExpireTest()
    {
    }
    ~LifeCycleExpireTest()
    {

    }
    virtual void SetUp()
    {
      test_lifecycle_expire_helper_= new TestCleanTaskHelper();
      test_lifecycle_expire_helper_->set_kv_engine(&test_engine_);
      test_lifecycle_expire_helper_->init(&test_engine_);

      test_life_cycle_helper_= new TestLifeCycleHelper();
      test_life_cycle_helper_->set_kv_engine(&test_engine_);
      test_life_cycle_helper_->init(&test_engine_);
    }
    virtual void TearDown()
    {

      test_life_cycle_helper_->set_kv_engine(NULL);
      delete test_life_cycle_helper_;
      test_life_cycle_helper_ = NULL;

      test_lifecycle_expire_helper_->set_kv_engine(NULL);
      delete test_lifecycle_expire_helper_;
      test_lifecycle_expire_helper_ = NULL;
    }

  protected:
    TestCleanTaskHelper *test_lifecycle_expire_helper_;
    TestLifeCycleHelper *test_life_cycle_helper_;
    TestEngineHelper test_engine_;
};


TEST_F(LifeCycleExpireTest, take_note)
{
  /* set life cycle */
  int ret = TFS_SUCCESS;
  int32_t ONE_DAY = 24 * 60 * 60;
  int32_t TWO_HOURS = 60 * 60 * 2;
  //std::string file_name = "file111";//111-5889,222-6698,333-10218
  ret = test_life_cycle_helper_->set_file_lifecycle(1, "file111", 1374805530,"tfscom");
  EXPECT_EQ(TFS_SUCCESS, ret);
  int32_t invalide_time;
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "file111", &invalide_time);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(1374805530, invalide_time);

  ret = test_life_cycle_helper_->set_file_lifecycle(1, "file222", 1374805533 - TWO_HOURS ,"tfscom");
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "file222", &invalide_time);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(1374805533 - TWO_HOURS, invalide_time);

  ret = test_life_cycle_helper_->set_file_lifecycle(1, "file333", 1374805577 - ONE_DAY, "tfscom");
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_life_cycle_helper_->get_file_lifecycle(1, "file333", &invalide_time);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(1374805577 - ONE_DAY, invalide_time);

  uint64_t local_id = tbsys::CNetUtil::strToAddr("10.232.35.41", 0);
  int32_t total_es = 10243;
  int32_t num_es = 0;
  int32_t note_interval = 0;
  SYSPARAM_EXPIRESERVER.re_clean_days_ = 2;

  /*CASE1*/
  num_es = 5889;
  ret = test_lifecycle_expire_helper_->clean_task(local_id, total_es, num_es, note_interval, 1374809888);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_lifecycle_expire_helper_->get_note(local_id, num_es, 1374809888, 5889, 1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  /*CASE2*/
  num_es = 6698;
  ret = test_lifecycle_expire_helper_->clean_task(local_id, total_es, num_es, note_interval, 1374809888);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_lifecycle_expire_helper_->get_note(local_id, num_es, 1374809888, 6698, 1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  /*CASE3*/
  num_es = 10218;
  ret = test_lifecycle_expire_helper_->clean_task(local_id, total_es, num_es, note_interval, 1374809888);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_lifecycle_expire_helper_->get_note(local_id, num_es, 1374809888, 10218, 1);
  EXPECT_EQ(TFS_SUCCESS, ret);

}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
