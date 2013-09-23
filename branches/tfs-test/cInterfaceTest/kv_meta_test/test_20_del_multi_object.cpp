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
TEST_F(TFS_Init,01_del_multi_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1="CCC";
  const char* local_file = RC_2M;
  const bool  quiet = true; 

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,02_del_multi_object_2K)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const char* local_file = RC_2K;
  const bool  quiet = true; 


  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::DeleteResult delete_result;
  tfs::common::CustomizeInfo customize_info;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,03_del_object_5M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const char* local_file = RC_5M;
  const bool  quiet = true; 


  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,04_del_object_100M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const char* local_file = RC_100M;
  const bool  quiet = true; 

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;
  tfs::common::ObjectInfo object_info;
  tfs::common::ObjectInfo object_info_1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.head_object(bucket_name,object_name_1,&object_info_1,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.head_object(bucket_name,object_name_1,&object_info_1,user_info);
  EXPECT_NE(Ret,0);


  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}


TEST_F(TFS_Init,05_del_object_non_exist_bucket)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const bool  quiet = true; 
  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;
  s_object_name.insert(object_name_1);



  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,06_del_object_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const bool  quiet = true; 
  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);



  tfs::common::UserInfo user_info;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;
  user_info.owner_id_ = 1;
  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,07_del_object_empty_bucket)
{
  int Ret ;
  const char* object_name = "BBB";
  const char* object_name_1 = "CCC";
  const char* bucket_name = "";
  const bool  quiet = true; 
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);


}

TEST_F(TFS_Init,08_del_object_non_exsit_object_2)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1="CCC";
  const bool  quiet = true; 

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}
TEST_F(TFS_Init,08_del_object_non_exsit_object_1)
{
int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1="CCC";
  const char* local_file = RC_2M;
  const bool  quiet = true; 

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);
  
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,09_del_object_null_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = NULL;
  const char* local_file = RC_2M;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,10_del_object_empty_object)
{ int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}

TEST_F(TFS_Init,11_del_object_double_del)
{ 
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* object_name_1="CCC";
  const char* local_file = RC_2M;
  const bool  quiet = true; 

  std::set<std::string> s_object_name;
  s_object_name.insert(object_name);
  s_object_name.insert(object_name_1);
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;
  tfs::common::DeleteResult delete_result;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name_1,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_multi_objects(bucket_name,s_object_name, quiet, &delete_result, user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
