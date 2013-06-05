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

TEST_F(TFS_Init,01_put_object)
{
  int Ret ;
  const char* bucket_name = "a1a1";
  const char* object_name = "BBB2";
  const char* local_file = RC_2M;
  int64_t req_length = 2*(1<<20);

  const char* local_file_rcv = "Temp";
  int64_t length = 2*(1<<20);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);

  uint32_t rcv_crc = Get_crc(local_file_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,02_put_object_2K)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;
  int64_t req_length = 2*(1<<10);

  const char* local_file_rcv = "Temp";
  int64_t length = 2*(1<<10);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);

  uint32_t rcv_crc = Get_crc(local_file_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,03_put_object_5M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;
  int64_t req_length = 5*(1<<20);

  const char* local_file_rcv = "Temp";
  int64_t length = 5*(1<<20);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);

  uint32_t rcv_crc = Get_crc(local_file_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,04_put_object_100M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_100M;
  int64_t req_length = 100*(1<<20);

  const char* local_file_rcv = "Temp";
  int64_t length = 100*(1<<20);

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.get_object(bucket_name,object_name,local_file_rcv,user_info);
  EXPECT_EQ(Ret,0);

  uint32_t rcv_crc = Get_crc(local_file_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,05_put_object_without_bucket)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,06_put_object_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,07_put_object_empty_bucket)
{
  int Ret ;
  const char* bucket_name = "";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,08_put_object_double)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,09_put_object_null_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = NULL;
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,10_put_object_empty_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "";
  const char* local_file = RC_2M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,11_put_object_null_local_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = NULL;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,12_put_object_empty_local_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = "";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,13_put_object_non_exist_local_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = "ABC";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,14_put_object_0_local_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_00;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(object_info.meta_info_.big_file_size_,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
