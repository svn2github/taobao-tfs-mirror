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

TEST_F(TFS_Init,01_pread_object)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int req_length = 2*(1<<20);
  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);
  EXPECT_EQ(object_meta_info.big_file_size_,length);

  uint32_t rcv_crc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf_rcv;
  buf_rcv = NULL;
}

TEST_F(TFS_Init,02_pread_object_2K)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  int req_length = 2*(1<<10);
  int64_t offset = 0;
  int64_t length = 2*(1<<10);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);
  EXPECT_EQ(object_meta_info.big_file_size_,length);

  uint32_t rcv_crc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,03_pread_object_5M)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  int req_length = 5*(1<<20);
  int64_t offset = 0;
  int64_t length = 5*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);
  EXPECT_EQ(object_meta_info.big_file_size_,length);

  uint32_t rcv_crc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,04_pread_object_100M)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_100M;

  int req_length = 100*(1<<20);
  int64_t offset = 0;
  int64_t length = 100*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  uint32_t local_crc = Get_crc(local_file,req_length);
  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);
  EXPECT_EQ(object_meta_info.big_file_size_,length);

  uint32_t rcv_crc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(local_crc,rcv_crc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,05_pread_object_with_offset)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 1024;
  int64_t length = 1*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,06_pread_object_with_length)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 0;
  int64_t length = 1*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);
  EXPECT_EQ(object_meta_info.big_file_size_,2*(1<<20));

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,07_pread_object_non_exist_bucket)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";


  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,08_pread_object_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* object_name = "BBB";

  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,09_pread_object_empty_bucket)
{
  int Ret ;
  const char* bucket_name = "";
  const char* object_name = "BBB";

  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);
}

TEST_F(TFS_Init,10_pread_object_non_exist)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";

  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,11_pread_object_null_object)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = NULL;


  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,12_pread_object_empty_object)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "";


  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,13_pread_object_wrong_offset)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = -1;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,14_pread_object_more_offset)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 2*(1<<20)+1;
  int64_t length = 0;
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,15_pread_object_wrong_length)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 0;
  int64_t length = -1;
  char*buf_rcv = new char[1];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_NE(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,16_pread_object_more_length)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 0;
  int64_t length = 3*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,2*(1<<20));
  EXPECT_EQ(object_meta_info.big_file_size_,2*(1<<20));

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,17_pread_object_more_length_and_offset)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 1024 ;
  int64_t length = 2*(1<<20)-1;
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,2*(1<<20)-1024);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,18_pread_object_zore_length_and_offset)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2M;

  int64_t offset = 0;
  int64_t length = 0;
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);
}

TEST_F(TFS_Init,19_pread_object_with_hollow)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 0;

  int64_t length_1 = 4*(1<<20);
  char*buf_rcv_1   = new char[length_1];

  int64_t length_2 = 10*(1<<20);
  char*buf_rcv_2   = new char[length_2];

  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20);
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);


  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_1,offset,length_1,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length_1);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_2,offset,length_2,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,7*(1<<20));

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
  delete buf_rcv_1;
  buf_rcv_1 = NULL;
  delete buf_rcv_2;
  buf_rcv_2 = NULL;
}

TEST_F(TFS_Init,20_pread_object_with_hollow_offset_in_hollow)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 1<<20;

  int64_t length_1 = 3*(1<<20);
  char*buf_rcv_1   = new char[length_1];

  int64_t length_2 = 5*(1<<20);
  char*buf_rcv_2   = new char[length_2];

  int64_t length_3 = 8*(1<<20);
  char*buf_rcv_3   = new char[length_3];

  int64_t length_4 = 20*(1<<20);
  char*buf_rcv_4   = new char[length_4];

  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);


  int64_t object_offset = 5*(1<<20);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  object_offset = 10*(1<<20);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_1,offset,length_1,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length_1);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_2,offset,length_2,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length_2);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_3,offset,length_3,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length_3);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv_4,offset,length_4,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,11*(1<<20));

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
  delete buf_rcv_1;
  buf_rcv_1 = NULL;
  delete buf_rcv_2;
  buf_rcv_2 = NULL;
  delete buf_rcv_3;
  buf_rcv_3 = NULL;
  delete buf_rcv_4;
  buf_rcv_4 = NULL;
}

TEST_F(TFS_Init,21_pread_object_with_hollow_sample)
{
  int Ret ;
  const char* bucket_name = "AAA";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 6*(1<<20);


  int64_t length = 10*(1<<20);
  char*buf_rcv   = new char[length];

  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20);
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);


  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,1*(1<<20));


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
  delete buf_rcv;
  buf_rcv = NULL;
}

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
