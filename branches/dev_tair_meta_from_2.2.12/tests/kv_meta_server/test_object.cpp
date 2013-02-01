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

class ObjectTest: public ::testing::Test
{
  public:
    ObjectTest()
    {
    }
    ~ObjectTest()
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

TEST_F(ObjectTest, test_pwrite_head)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname1");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 666;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);
  obj_info.v_tfs_file_info_[0].offset_ = 0;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 0, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &new_obj_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(10, new_obj_info.meta_info_.big_file_size_);


  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_middle)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname2");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 555;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 0, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 20;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 20, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo ret_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 10, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_overlap)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abdc");
  string object_name("objectname3");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 222;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 0, 10, obj_info ,user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 20, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo ret_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 15, 10, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_error_param)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname4");
  int64_t now_time = 11111;

  BucketMetaInfo bucket_meta_info;
  bucket_meta_info.create_time_ = now_time;
  UserInfo user_info;
  user_info.owner_id_ = 222;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, -1, 10, obj_info, user_info);
  EXPECT_EQ(TFS_ERROR, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_no_bucket)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname5");
  UserInfo user_info;
  user_info.owner_id_ = 222;
  ObjectInfo obj_info;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, 0, 10, obj_info, user_info);

  EXPECT_EQ(EXIT_BUCKET_NOT_EXIST, ret);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
