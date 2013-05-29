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
#include "test_meta_info_helper.h"
#include "kv_meta_define.h"
#include "test_kvengine.h"
#include "define.h"
#include "error_msg.h"

using namespace std;
using namespace tfs;
using namespace tfs::common;
using namespace tfs::kvmetaserver;

class BucketTest: public ::testing::Test
{
  public:
    BucketTest()
    {
    }
    ~BucketTest()
    {

    }
    virtual void SetUp()
    {
      test_meta_info_helper_ = new TestMetaInfoHelper();
      test_meta_info_helper_->set_kv_engine(&test_engine_);
      test_meta_info_helper_->init();
    }
    virtual void TearDown()
    {
      test_meta_info_helper_->set_kv_engine(NULL);
      delete test_meta_info_helper_;
      test_meta_info_helper_ = NULL;
    }

  protected:
    TestMetaInfoHelper *test_meta_info_helper_;
    TestEngineHelper test_engine_;
};

TEST_F(BucketTest, test_put)
{
  int ret = TFS_SUCCESS;
  string bucket_name("bucket");
  int64_t now_time = static_cast<int64_t>(time(NULL));

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 222;

  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  BucketMetaInfo new_meta_info;
  ret = test_meta_info_helper_->head_bucket(bucket_name, &new_meta_info, NULL);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(now_time, new_meta_info.create_time_);
  EXPECT_EQ(user_info.owner_id_, new_meta_info.owner_id_);
  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(BucketTest, test_del_with_no_object)
{
  int ret = TFS_SUCCESS;
  string bucket_name("bucketname");
  int64_t now_time = static_cast<int64_t>(time(NULL));;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.set_create_time(now_time);
  UserInfo user_info;
  user_info.owner_id_ = 222;

  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  string file_name("objectname");
  ObjectInfo object_info;
  ret = test_meta_info_helper_->put_object(bucket_name, file_name, object_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo object_info1;
  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, file_name, &object_info1, &still_have, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(BucketTest, test_del_with_object)
{
  int ret = TFS_SUCCESS;
  string bucket_name("bucketname");
  int64_t now_time = static_cast<int64_t>(time(NULL));;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;

  UserInfo user_info;
  user_info.owner_id_ = 212;

  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //put obj
  string file_name("objectname");
  ObjectInfo object_info;
  ret = test_meta_info_helper_->put_object(bucket_name, file_name, object_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, EXIT_DELETE_DIR_WITH_FILE_ERROR);

  ObjectInfo object_info1;
  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, file_name, &object_info1, &still_have, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(BucketTest, test_with_prefix)
{
  const char *s = "objectname/aa/";
  const string prefix = "objectname/aa";
  char delimiter = '/';

  bool prefix_flag = false;
  bool common_flag = false;
  int pos = -1;

  MetaInfoHelper::get_common_prefix(s, prefix, delimiter, &prefix_flag, &common_flag, &pos);

  EXPECT_EQ(prefix_flag, true);
  EXPECT_EQ(common_flag, true);

  EXPECT_EQ(pos, 13);
}

TEST_F(BucketTest, test_no_prefix)
{
  const char *s = "photos/2006/feb/aaa";
  char delimiter = '/';
  const string prefix;

  bool prefix_flag = false;
  bool common_flag = false;
  int pos = -1;

  MetaInfoHelper::get_common_prefix(s, prefix, delimiter, &prefix_flag, &common_flag, &pos);

  EXPECT_EQ(prefix_flag, true);
  EXPECT_EQ(common_flag, true);

  EXPECT_EQ(pos, 6);
}

TEST_F(BucketTest, test_prefix_match_delimiter_dismatch)
{
  const char *s = "photos/2006/feb/aaa";
  const string prefix = "photos/2006/";
  char delimiter = '*';

  bool prefix_flag = false;
  bool common_flag = false;
  int pos = -1;

  MetaInfoHelper::get_common_prefix(s, prefix, delimiter, &prefix_flag, &common_flag, &pos);

  EXPECT_EQ(prefix_flag, true);
  EXPECT_EQ(common_flag, false);

  EXPECT_EQ(pos, -1);
}

TEST_F(BucketTest, test_prefix_delimiter_dismatch)
{
  const char *s = "photos/2006/feb/aaa";
  const string prefix = "2006/";
  char delimiter = '*';

  bool prefix_flag = false;
  bool common_flag = false;
  int pos = -1;

  MetaInfoHelper::get_common_prefix(s, prefix, delimiter, &prefix_flag, &common_flag, &pos);

  EXPECT_EQ(prefix_flag, false);
  EXPECT_EQ(common_flag, false);

  EXPECT_EQ(pos, -1);
}

TEST_F(BucketTest, test_get)
{
  int ret = TFS_SUCCESS;
  string bucket_name("bucketname");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;

  UserInfo user_info;
  user_info.owner_id_ = 212;

  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //put obj
  string file_name("objectname/aa/");
  ObjectInfo object_info;
  ret = test_meta_info_helper_->put_object(bucket_name, file_name, object_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  // get bucket -> list obj
  string prefix("objectname/aa");
  string start_key("objectname/a");
  char delimiter = '/';
  vector<ObjectMetaInfo> v_object_meta_info;
  vector<string> v_object_name;
  set<string> s_common_prefix;
  int32_t limit = 10;
  int8_t is_truncated = -1;

  ret = test_meta_info_helper_->get_bucket(bucket_name, prefix, start_key, delimiter, &limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(0, is_truncated);
  EXPECT_EQ(1, static_cast<int32_t>(s_common_prefix.size()));
  EXPECT_EQ(0, static_cast<int32_t>(v_object_name.size()));


  ObjectInfo object_info1;
  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, file_name, &object_info1, &still_have, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(BucketTest, test_acl)
{
  int ret = TFS_SUCCESS;
  string bucket_name("bucketname");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;

  UserInfo user_info;
  user_info.owner_id_ = 212;

  //212 has full_control, 200 has nothing
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //impact on get_bucket, put_object, del_object, put_bucket_acl, get_bucket_acl

  // get bucket
  string prefix("objectname/aa");
  string start_key("objectname/a");
  char delimiter = '/';
  vector<ObjectMetaInfo> v_object_meta_info;
  vector<string> v_object_name;
  set<string> s_common_prefix;
  int32_t limit = 10;
  int8_t is_truncated = -1;

  ret = test_meta_info_helper_->get_bucket(bucket_name, prefix, start_key, delimiter, &limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  user_info.owner_id_ = 200;
  ret = test_meta_info_helper_->get_bucket(bucket_name, prefix, start_key, delimiter, &limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);
  EXPECT_EQ(EXIT_BUCKET_PERMISSION_DENY, ret);

  //put_object
  string file_name("objectname/aa/");
  ObjectInfo object_info;
  ret = test_meta_info_helper_->put_object(bucket_name, file_name, object_info, user_info);
  EXPECT_EQ(EXIT_BUCKET_PERMISSION_DENY, ret);
  user_info.owner_id_ = 212;
  ret = test_meta_info_helper_->put_object(bucket_name, file_name, object_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo object_info1;
  bool still_have = false;
  //del object
  user_info.owner_id_ = 200;
  ret = test_meta_info_helper_->del_object(bucket_name, file_name, &object_info1, &still_have, user_info);
  EXPECT_EQ(EXIT_BUCKET_PERMISSION_DENY, ret);
  user_info.owner_id_ = 212;
  ret = test_meta_info_helper_->del_object(bucket_name, file_name, &object_info1, &still_have, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //put bucket acl
  MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.insert(std::make_pair(212, FULL_CONTROL));
  user_info.owner_id_ = 200;
  ret = test_meta_info_helper_->put_bucket_acl(bucket_name, bucket_acl_map, user_info);
  EXPECT_EQ(EXIT_BUCKET_PERMISSION_DENY, ret);
  user_info.owner_id_ = 212;
  ret = test_meta_info_helper_->put_bucket_acl(bucket_name, bucket_acl_map, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //get bucket acl
  user_info.owner_id_ = 200;
  ret = test_meta_info_helper_->get_bucket_acl(bucket_name, &bucket_acl_map, user_info);
  EXPECT_EQ(EXIT_BUCKET_PERMISSION_DENY, ret);
  user_info.owner_id_ = 212;
  ret = test_meta_info_helper_->get_bucket_acl(bucket_name, &bucket_acl_map, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(BucketTest, test_get_service)
{
  int ret = TFS_SUCCESS;

  string bucket_name("bucket");
  int64_t now_time = static_cast<int64_t>(time(NULL));
  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 222;

  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  string bucket_name1("bucket1");
  int64_t now_time1 = static_cast<int64_t>(time(NULL));
  bucket_meta_info.create_time_ = now_time1;
  user_info.owner_id_ = 222;
  ret = test_meta_info_helper_->put_bucket(bucket_name1, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  BucketsResult buckets_result;
  ret = test_meta_info_helper_->list_buckets(&buckets_result, user_info);

  EXPECT_EQ(222, buckets_result.owner_id_);
  map<string, BucketMetaInfo>::const_iterator iter = buckets_result.bucket_info_map_.find(bucket_name);
  EXPECT_EQ(bucket_name, iter->first);
  EXPECT_EQ(now_time, iter->second.create_time_);

  iter = buckets_result.bucket_info_map_.find(bucket_name1);
  EXPECT_EQ(bucket_name1, iter->first);
  EXPECT_EQ(now_time1, iter->second.create_time_);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->del_bucket(bucket_name1);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
