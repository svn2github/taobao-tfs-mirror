/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   diqing <diqing@taobao.com>
*      - initial release
*
*/

#include"kv_meta_test_init.h"

TEST_F(TFS_Init,01_put_bucket_acl)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,02_put_bucket_null)
{
  int Ret ;
  const char* bucket_name = NULL;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_NE(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,03_put_bucket_empty)
{
  int Ret ;
  const char* bucket_name = "";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,04_put_bucket_double)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,05_put_bucket_acl_userinfo_is_null)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}



int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
