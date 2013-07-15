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
      test_meta_info_helper_->init(&test_engine_);
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

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 666;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &new_obj_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(10, new_obj_info.meta_info_.big_file_size_);

  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_middle)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname2");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 555;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 20;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo ret_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = 10;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &ret_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_overlap)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abdc");
  string object_name("objectname3");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 222;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info ,user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 20;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo ret_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = 15;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &ret_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(TFS_SUCCESS, ret);
}

TEST_F(ObjectTest, test_pwrite_append)
{
  int ret = TFS_SUCCESS;
  string bucket_name("abcd");
  string object_name("objectname4");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 222;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = -1;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo ret_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(10, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = 20;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(30, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = -1;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(40, ret_obj_info.meta_info_.big_file_size_);

  obj_info.v_tfs_file_info_[0].offset_ = 10;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &ret_obj_info);
  EXPECT_EQ(40, ret_obj_info.meta_info_.big_file_size_);

  bool still_have = false;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &ret_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);

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
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(EXIT_BUCKET_NOT_EXIST, ret);
}

TEST_F(ObjectTest, test_pread_head)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testabcd");
  string object_name("testobjectname1");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 10;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  ret = test_meta_info_helper_->head_object(bucket_name, object_name, &new_obj_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(10, new_obj_info.meta_info_.big_file_size_);

  bool still_have1 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
}

TEST_F(ObjectTest, test_pread_smallfile)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testabcd");
  string object_name("testobjectsmallfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 131;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  bool still_have = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 2097152, &new_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(2097152, new_obj_info.meta_info_.big_file_size_);
  EXPECT_EQ(131, obj_info.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(false, still_have);

  bool still_have1 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(ObjectTest, test_pread_bigfile)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testabcd");
  string object_name("testobjectsmallfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 131;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 2097152;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 132;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 4194304;
  obj_info.v_tfs_file_info_[0].file_size_ = 1048576;
  obj_info.v_tfs_file_info_[0].block_id_ = 133;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  bool still_have = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 5242888, &new_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(5242880, new_obj_info.meta_info_.big_file_size_);
  EXPECT_EQ(131, new_obj_info.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(132, new_obj_info.v_tfs_file_info_[1].block_id_);
  EXPECT_EQ(133, new_obj_info.v_tfs_file_info_[2].block_id_);
  EXPECT_EQ(false, still_have);

  bool still_have1 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(ObjectTest, test_pread_out_of_order)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testyoyo");
  string object_name("testobjectoutorderfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;
  //002
  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 2097152;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 232;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);
  //004
  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_[0].offset_ = 6291456;
  obj_info.v_tfs_file_info_[0].file_size_ = 1048576;
  obj_info.v_tfs_file_info_[0].block_id_ = 234;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info1;
  bool still_have1 = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 7340032, &new_obj_info1, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(7340032, new_obj_info1.meta_info_.big_file_size_);
  ASSERT_TRUE(!new_obj_info1.v_tfs_file_info_.empty());
  EXPECT_EQ(232, new_obj_info1.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(234, new_obj_info1.v_tfs_file_info_[1].block_id_);
  EXPECT_EQ(false, still_have1);

  //003
  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_[0].offset_ = 4194304;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 233;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  //001
  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_ = 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 231;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info2;
  bool still_have2 = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 7340032, &new_obj_info2, &still_have2);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(7340032, new_obj_info2.meta_info_.big_file_size_);
  EXPECT_EQ(231, new_obj_info2.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(232, new_obj_info2.v_tfs_file_info_[1].block_id_);
  EXPECT_EQ(233, new_obj_info2.v_tfs_file_info_[2].block_id_);
  EXPECT_EQ(234, new_obj_info2.v_tfs_file_info_[3].block_id_);
  EXPECT_EQ(false, still_have2);

  ObjectInfo new_obj_info3;
  bool still_have3 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info3, &still_have3);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(ObjectTest, test_del_smallfile)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testabcddel");
  string object_name("testobjectdelsmallfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);

  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_= 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 131;
  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  bool still_have = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 2097152, &new_obj_info, &still_have);

  EXPECT_EQ(131, new_obj_info.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(2097152, new_obj_info.v_tfs_file_info_[0].file_size_);
  EXPECT_EQ(2097152, new_obj_info.meta_info_.big_file_size_);
  EXPECT_EQ(false, still_have);

  ObjectInfo new_obj_info1;
  bool still_have1 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info1, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);

  EXPECT_EQ(131, new_obj_info1.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(2097152, new_obj_info1.v_tfs_file_info_[0].file_size_);
  EXPECT_EQ(false, still_have1);

  bool still_have2 = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 2097152, &new_obj_info1, &still_have2);
  EXPECT_EQ(EXIT_OBJECT_NOT_EXIST, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(ObjectTest, test_del_bigfile)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testbigdel");
  string object_name("testobjectdelbigfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  obj_info.meta_info_.big_file_size_ = 0;
  TfsFileInfo tfs_file_info;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);
  obj_info.v_tfs_file_info_[0].offset_ = 0;
  obj_info.v_tfs_file_info_[0].file_size_= 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 131;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);
  obj_info.v_tfs_file_info_[0].offset_ = 2097152;
  obj_info.v_tfs_file_info_[0].file_size_= 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 132;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);
  obj_info.v_tfs_file_info_[0].offset_ = 4194304;
  obj_info.v_tfs_file_info_[0].file_size_= 1048576;
  obj_info.v_tfs_file_info_[0].block_id_ = 133;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  bool still_have = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(131, new_obj_info.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(132, new_obj_info.v_tfs_file_info_[1].block_id_);
  EXPECT_EQ(133, new_obj_info.v_tfs_file_info_[2].block_id_);
  EXPECT_EQ(false, still_have);

  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 5300000, &new_obj_info, &still_have);
  EXPECT_EQ(EXIT_OBJECT_NOT_EXIST, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);

}

TEST_F(ObjectTest, test_del_bigfile_no_head)
{
  int ret = TFS_SUCCESS;
  string bucket_name("testbigdel");
  string object_name("testobjectdelbigfile");

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = 1688;
  ret = test_meta_info_helper_->put_bucket(bucket_name, bucket_meta_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo obj_info;
  TfsFileInfo tfs_file_info;

  obj_info.meta_info_.big_file_size_ = 0;
  obj_info.v_tfs_file_info_.push_back(tfs_file_info);
  obj_info.v_tfs_file_info_[0].offset_ = 2097152;
  obj_info.v_tfs_file_info_[0].file_size_= 2097152;
  obj_info.v_tfs_file_info_[0].block_id_ = 132;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 5242880;
  obj_info.v_tfs_file_info_[0].file_size_= 1048576;
  obj_info.v_tfs_file_info_[0].block_id_ = 134;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  obj_info.v_tfs_file_info_[0].offset_ = 4194304;
  obj_info.v_tfs_file_info_[0].file_size_= 1048576;
  obj_info.v_tfs_file_info_[0].block_id_ = 133;

  ret = test_meta_info_helper_->put_object(bucket_name, object_name, obj_info, user_info);
  EXPECT_EQ(TFS_SUCCESS, ret);

  ObjectInfo new_obj_info;
  bool still_have1 = true;
  ret = test_meta_info_helper_->del_object(bucket_name, object_name, &new_obj_info, &still_have1);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(132, new_obj_info.v_tfs_file_info_[0].block_id_);
  EXPECT_EQ(133, new_obj_info.v_tfs_file_info_[1].block_id_);
  EXPECT_EQ(134, new_obj_info.v_tfs_file_info_[2].block_id_);
  EXPECT_EQ(false, still_have1);

  bool still_have = true;
  ret = test_meta_info_helper_->get_object(bucket_name, object_name, 0, 5300000, &new_obj_info, &still_have);
  EXPECT_EQ(EXIT_OBJECT_NOT_EXIST, ret);

  ret = test_meta_info_helper_->del_bucket(bucket_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
