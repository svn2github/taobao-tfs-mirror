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
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  bucket_acl_map.clear();
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP));
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  
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
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  bucket_acl_map.clear();  
  bucket_acl_map.insert(std::make_pair(1,READ|WRITE_ACP|WRITE|READ_ACP));
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
  const char* bucket_name = "alaali";
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
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  Ret =kv_meta_client.get_bucket_acl(bucket_name,&bucket_acl_map,user_info2);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info2);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.del_bucket(bucket_name,user_info2);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,02_put_bucket_null)
{
  int Ret ;
  const char* bucket_name = NULL;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;

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

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
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
  tfs::common::MAP_INT64_INT bucket_acl_map;

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
TEST_F(TFS_Init,05_put_bucket_acl_userinfo_is_null)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::MAP_INT64_INT bucket_acl_map;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.put_bucket_acl(bucket_name,bucket_acl_map,user_info);
  EXPECT_EQ(Ret,0);
  
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}



int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
