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
*    diqing<diqing@taobao.com>
*      - initial release
*
*/

#include"kv_meta_test_init.h"
#define RC_2M    "/home/admin/workspace/diqing/resource/2m"
TEST_F(TFS_Init,01_get_service)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* local_file=RC_2M; 
  tfs::common::BucketsResult buckets_result;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(0,Ret);
  Ret = kv_meta_client.put_object(bucket_name,"abc/",local_file,user_info,customize_info);  
  EXPECT_EQ(0,Ret);
  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(0,Ret);
  Ret = kv_meta_client.del_object(bucket_name,"abc/",user_info); 
  EXPECT_EQ(0,Ret);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);


}
TEST_F(TFS_Init,test_1)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(0,Ret);


}
TEST_F(TFS_Init,test_2)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  cout<<"###################"<<endl;
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);


}
TEST_F(TFS_Init,02_get_service)
{
  int Ret ;
  const char* bucket_name = "alal";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::BucketsResult buckets_result;
  tfs::common::BucketMetaInfo bucket_meta_info;
  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(Ret,0);
  
  //Ret = kv_meta_client.head_bucket(bucket_name, &bucket_meta_info, user_info);
  //EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(Ret,0);
 
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret= kv_meta_client.head_bucket(bucket_name, &bucket_meta_info, user_info);
  EXPECT_NE(Ret,0);
  
  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(Ret,0);
 
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);  
  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,03_get_service_no_bucket)
{ 
  int Ret ;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::BucketsResult buckets_result;
  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(Ret,0);

}

TEST_F(TFS_Init,04_get_service_2_bucket)
{
  
  int Ret ;
  const char* bucket_name = "alas";
  const char* bucket_name1 = "alasl";
  
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::BucketsResult buckets_result;
  
  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket(bucket_name1, user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.get_service(&buckets_result,  user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name1,user_info);
  EXPECT_EQ(Ret,0);

}
TEST_F(TFS_Init,05_get_service_2_userinfo)
{
  
  int Ret ;
  const char* bucket_name = "alald";
  
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::UserInfo user_info1;
  user_info1.owner_id_ = 10;

  tfs::common::BucketsResult buckets_result;
  tfs::common::BucketMetaInfo bucket_meta_info;
  Ret = kv_meta_client.put_bucket(bucket_name, user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.get_service(&buckets_result,  user_info1);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
