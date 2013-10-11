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
TEST_F(TFS_Init,01_put_bucket_acl_read_write_acp)
{
  int Ret ;
  const char* bucket_name = "ala";
  const char* object_name = "BBB2";
  const char* local_file = RC_2M;

  tfs::common::CustomizeInfo customize_info;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
  
  bucket_acl_map.clear();  
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP|WRITE|READ_ACP));
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret =kv_meta_client.get_bucket_acl(bucket_name,&bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}
TEST_F(TFS_Init,01_put_bucket_acl_write)
{
  int Ret ;
  const char* bucket_name = "ala";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,WRITE|WRITE_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  bucket_acl_map.clear();  
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP|WRITE|READ_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret =kv_meta_client.get_bucket_acl(bucket_name,&bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}
TEST_F(TFS_Init,01_put_bucket_acl_read_write_read_acp)
{
  int Ret ;
  const char* bucket_name = "ala";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  
  bucket_acl_map.clear();  
  bucket_acl_map.insert(std::make_pair(2,WRITE|READ_ACP));
  tfs::common::UserInfo user_info2;
  user_info2.owner_id_=2;
  Ret =kv_meta_client.get_bucket_acl(bucket_name,&bucket_acl_map,user_info2);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info2);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info2);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,WRITE|READ_ACP));
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
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
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,FULL_CONTROL));
 
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
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
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,FULL_CONTROL));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,04_put_bucket_acl_double)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,FULL_CONTROL));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,05_put_bucket_acl_write_write_acp)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,WRITE|WRITE_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  tfs::common::MAP_INT64_INT bucket_acl_map_get;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,WRITE|WRITE_ACP|READ));
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_acl(bucket_name,&bucket_acl_map_get,user_info);  
  cout<<"the bucket_acl_map_get is  "<<bucket_acl_map_get[1];
  
  EXPECT_EQ(bucket_acl_map_get[1],11); 
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}



int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
