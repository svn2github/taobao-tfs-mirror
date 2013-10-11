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
using namespace tfs::common;
TEST_F(TFS_Init,01_put_bucket_acl_private)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB2";
  const char* local_file = RC_2M;
  const char* local_file_rcv = "Temp";

  tfs::common::CustomizeInfo customize_info;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::UserInfo user_info_2;
  user_info_2.owner_id_ = 2;

  tfs::common::CANNED_ACL acl=PRIVATE;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
 EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info_2,customize_info);
 EXPECT_NE(Ret,0);


  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info_2);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info_2);  
  EXPECT_NE(Ret,0); 
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);                   
  EXPECT_EQ(Ret,0); 
   
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,01_put_bucket_acl_public_read)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB2";
  const char* local_file = RC_2M;
  const char* local_file_rcv = "Temp";

  tfs::common::CustomizeInfo customize_info;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::UserInfo user_info_2;
  user_info_2.owner_id_ = 2;

  tfs::common::CANNED_ACL acl=PUBLIC_READ;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info_2,customize_info);
 EXPECT_NE(Ret,0);

  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info_2);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info_2);  
  EXPECT_NE(Ret,0); 
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);                   
  EXPECT_EQ(Ret,0); 
   
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
}
//TEST_F(TFS_Init,01_put_bucket_acl_public_read_write)
//{
//  int Ret ;
//  const char* bucket_name = "a1a";
//  const char* object_name = "BBB2";
//  const char* object_name_1 = "BBB3";
//  const char* local_file = RC_2M;
//  const char* local_file_rcv = "Temp";
//
//  tfs::common::CustomizeInfo customize_info;
//  tfs::common::UserInfo user_info;
//  user_info.owner_id_ = 1;
//
//  tfs::common::UserInfo user_info_2;
//  user_info_2.owner_id_ = 2;
//
//  tfs::common::CANNED_ACL acl=PUBLIC_READ_WRITE;
//  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.put_bucket_acl(bucket_name,acl,user_info);
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info_2,customize_info);
//  EXPECT_EQ(Ret,0);
//
//  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info_2);
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info_2);  
//  EXPECT_EQ(Ret,0);
// 
//  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);                   
//  EXPECT_EQ(Ret,0);
//  Ret = kv_meta_client.del_object(bucket_name,object_name_1,user_info_2);                   
//  EXPECT_EQ(Ret,0);
//  
//  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
//  EXPECT_EQ(Ret,0);
//  
//}


TEST_F(TFS_Init,02_put_bucket_null)
{
  int Ret ;
  const char* bucket_name = NULL;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CANNED_ACL acl=PRIVATE;

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
  tfs::common::CANNED_ACL acl=PRIVATE;

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
  tfs::common::CANNED_ACL acl=PRIVATE;
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
