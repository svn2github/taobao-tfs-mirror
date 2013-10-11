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

TEST_F(TFS_Init,01_put_bucket_tag)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("aaa","bbb"));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,02_put_bucket_tag_bucket_name_is_null)
{
  int Ret ;
  const char* bucket_name = NULL;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("aaa","bbb"));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,03_put_bucket_tag_bucket_is_empty)
{
  int Ret ;
  const char* bucket_name = "";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("aaa","bbb"));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_NE(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_NE(Ret,0);

}

TEST_F(TFS_Init,04_put_bucket_tag_double)
{  
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("aaa","bbb"));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
 
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);


  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

}
TEST_F(TFS_Init,05_put_bucket_tag_is_null)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("NULL",""));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,06_put_bucket_tag_is_empty)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("",""));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
TEST_F(TFS_Init,07_put_bucket_tag_one_tag)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("onetag","onetag"));
  

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,09_put_bucket_tag_two_tag)
{
  int Ret ;
  const char* bucket_name = "a1a";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  
  MAP_STRING bucket_tag_map;
  bucket_tag_map.clear();
  bucket_tag_map.insert(std::make_pair("onetag","onetag"));
  bucket_tag_map.insert(std::make_pair("twotag","twotag")); 

  MAP_STRING bucket_tag_map_get;
  bucket_tag_map_get.clear();  
  
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_bucket_tag(bucket_name,bucket_tag_map);
  EXPECT_EQ(Ret,0);
  Ret = kv_meta_client.get_bucket_tag(bucket_name,&bucket_tag_map_get);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket_tag(bucket_name);
  EXPECT_EQ(Ret,0); 
  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}
int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
