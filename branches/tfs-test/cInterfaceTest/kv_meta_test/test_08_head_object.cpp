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
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/

#include"kv_meta_test_init.h"

TEST_F(TFS_Init,01_head_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(object_info.has_meta_info_,true);
  EXPECT_EQ(object_info.meta_info_.big_file_size_,2*(1<<20));
  EXPECT_EQ(object_info.meta_info_.owner_id_,user_info.owner_id_);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,02_head_object_2K)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(object_info.has_meta_info_,true);
  EXPECT_EQ(object_info.meta_info_.big_file_size_,2*(1<<10));
  EXPECT_EQ(object_info.meta_info_.owner_id_,user_info.owner_id_);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,03_head_object_5M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(object_info.has_meta_info_,true);
  EXPECT_EQ(object_info.meta_info_.big_file_size_,5*(1<<20));
  EXPECT_EQ(object_info.meta_info_.owner_id_,user_info.owner_id_);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,04_head_object_100M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_100M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info,customize_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(object_info.has_meta_info_,true);
  EXPECT_EQ(object_info.meta_info_.big_file_size_,100*(1<<20));
  EXPECT_EQ(object_info.meta_info_.owner_id_,user_info.owner_id_);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,05_head_object_non_exsit_bucket)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;

  tfs::common::ObjectInfo object_info;

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,06_head_object_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,07_head_object_empty_bucket)
{
  int Ret ;
  const char* bucket_name = "";
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,08_head_object_non_exist_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,09_head_object_null_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = NULL;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,10_head_object_empty_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
