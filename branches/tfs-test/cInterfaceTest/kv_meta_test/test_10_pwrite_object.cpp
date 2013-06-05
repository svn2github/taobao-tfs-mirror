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

TEST_F(TFS_Init,01_pwrite_object_2M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t length = 2*(1<<20);
  int64_t offset = 5*(1<<20)+1;
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20)+1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_2M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,02_pwrite_object_2M_offset_0)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 5*(1<<20);
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20);
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_2M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,03_pwrite_object_20M)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 5*(1<<20)+1;
  int64_t length = 20*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_20M;
  int64_t object_offset = 5*(1<<20)+1;
  int64_t pwrite_length = 20*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_20M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,04_pwrite_object_20M_offset_0)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 5*(1<<20);
  int64_t length = 20*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_20M;
  int64_t object_offset = 5*(1<<20);
  int64_t pwrite_length = 20*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_20M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,05_pwrite_object_with_offset)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 5*(1<<20)+1024;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20)+1024;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_2M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,06_pwrite_object_with_length)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 5*(1<<20);
  int64_t length = 1*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 5*(1<<20);
  int64_t pwrite_length = 1*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_2M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,07_pwrite_object_non_exist_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  tfs::common::ObjectInfo object_info ;
  int64_t offset = 1;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  Ret = kv_meta_client.head_object(bucket_name,object_name,&object_info,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,08_pwrite_object_non_exist_object_offset_0)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 0;
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 0;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,09_pwrite_object_null_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = NULL;
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,10_pwrite_object_empty_object)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "";
  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,11_pwrite_object_non_exist_bucket)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,12_pwrite_object_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,13_pwrite_object_empty_bucket)
{
  int Ret ;
  const char* bucket_name = "";
  const char* object_name = "BBB";

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,14_pwrite_object_null_buf)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t object_offset = 0;
  int64_t pwrite_length = 0;
  char*buf = NULL;

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,15_pwrite_object_empty_buf)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t object_offset = 0;
  int64_t pwrite_length = 0;
  char*buf = new char [1];
  buf[0]='\0';

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,16_pwrite_object_wrong_offset)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = -2;
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,17_pwrite_object_more_offset)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  int64_t offset = 6*(1<<20);
  int64_t length = 2*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 6*(1<<20);
  int64_t pwrite_length = 2*(1<<20);
  char*buf = new char[pwrite_length];
  ReadData(pwrite_file,buf,pwrite_length,0);

  uint32_t ExpectCrc = Get_crc(RC_2M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_EQ(Ret,pwrite_length);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);


  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,18_pwrite_object_wrong_length)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_5M;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  const char* pwrite_file = RC_2M;
  int64_t object_offset = 1;
  int64_t pwrite_length = -1;
  char*buf = new char[1];
  ReadData(pwrite_file,buf,pwrite_length,0);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf,object_offset,pwrite_length,user_info);
  EXPECT_LT(Ret,0);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf;
  buf = NULL;
}

TEST_F(TFS_Init,19_pwrite_object_small_hollow_in_small_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  int req_length = 2*(1<<10);

  //const char*a1a="/home/yiming.czw/kv_meta_test/resouce/aaa";
  int64_t offset = 2*(1<<10);
  int64_t length = 5*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file_1 = RC_5M;
  //const char* pwrite_file_1 = a1a;
  int64_t object_offset_1 = 2*(1<<10);
  int64_t pwrite_length_1 = 1<<20;
  char*buf_1 = new char[pwrite_length_1];
  ReadData(pwrite_file_1,buf_1,pwrite_length_1,object_offset_1-req_length);

  const char* pwrite_file_2 = RC_5M;
  //const char* pwrite_file_2 = a1a;
  int64_t object_offset_2= (1<<20)+2*(1<<10);
  int64_t pwrite_length_2 = 3*(1<<20);
  char*buf_2 = new char[pwrite_length_2];
  ReadData(pwrite_file_2,buf_2,pwrite_length_2,object_offset_2-req_length);

  const char* pwrite_file_3 = RC_5M;
  //const char* pwrite_file_3 = a1a;
  int64_t object_offset_3 = 4*(1<<20)+2*(1<<10);
  int64_t pwrite_length_3 = 1<<20;
  char*buf_3 = new char[pwrite_length_3];
  ReadData(pwrite_file_3,buf_3,pwrite_length_3,object_offset_3-req_length);

  uint32_t ExpectCrc = Get_crc(RC_5M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_1,object_offset_1,pwrite_length_1,user_info);
  EXPECT_EQ(Ret,pwrite_length_1);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_3,object_offset_3,pwrite_length_3,user_info);
  EXPECT_EQ(Ret,pwrite_length_3);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_2,object_offset_2,pwrite_length_2,user_info);
  EXPECT_EQ(Ret,pwrite_length_2);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf_1;
  buf_1 = NULL;
  delete buf_2;
  buf_2 = NULL;
  delete buf_3;
  buf_3 = NULL;
}

TEST_F(TFS_Init,20_pwrite_object_large_hollow_in_small_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  int req_length = 2*(1<<10);

  int64_t offset = 2*(1<<10);
  int64_t length = 20*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file_1 = RC_20M;
  int64_t object_offset_1 = 2*(1<<10);
  int64_t pwrite_length_1 = 1<<20;
  char*buf_1 = new char[pwrite_length_1];
  ReadData(pwrite_file_1,buf_1,pwrite_length_1,object_offset_1-req_length);

  const char* pwrite_file_2 = RC_20M;
  int64_t object_offset_2= (1<<20)+2*(1<<10);
  int64_t pwrite_length_2 = 18*(1<<20);
  char*buf_2 = new char[pwrite_length_2];
  ReadData(pwrite_file_2,buf_2,pwrite_length_2,object_offset_2-req_length);

  const char* pwrite_file_3 = RC_20M;
  int64_t object_offset_3 = 19*(1<<20)+2*(1<<10);
  int64_t pwrite_length_3 = 1<<20;
  char*buf_3 = new char[pwrite_length_3];
  ReadData(pwrite_file_3,buf_3,pwrite_length_3,object_offset_3-req_length);

  uint32_t ExpectCrc = Get_crc(RC_20M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_1,object_offset_1,pwrite_length_1,user_info);
  EXPECT_EQ(Ret,pwrite_length_1);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_3,object_offset_3,pwrite_length_3,user_info);
  EXPECT_EQ(Ret,pwrite_length_3);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_2,object_offset_2,pwrite_length_2,user_info);
  EXPECT_EQ(Ret,pwrite_length_2);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf_1;
  buf_1 = NULL;
  delete buf_2;
  buf_2 = NULL;
  delete buf_3;
  buf_3 = NULL;
}

TEST_F(TFS_Init,21_pwrite_object_small_hollow_in_large_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  int req_length = 2*(1<<10);

  int64_t offset = 2*(1<<10);
  int64_t length = 20*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file_1 = RC_20M;
  int64_t object_offset_1 = 2*(1<<10);
  int64_t pwrite_length_1 = 9*(1<<20);
  char*buf_1 = new char[pwrite_length_1];
  ReadData(pwrite_file_1,buf_1,pwrite_length_1,object_offset_1-req_length);

  const char* pwrite_file_2 = RC_20M;
  int64_t object_offset_2= 9*(1<<20)+2*(1<<10);
  int64_t pwrite_length_2 = 2*(1<<20);
  char*buf_2 = new char[pwrite_length_2];
  ReadData(pwrite_file_2,buf_2,pwrite_length_2,object_offset_2-req_length);

  const char* pwrite_file_3 = RC_20M;
  int64_t object_offset_3 = 11*(1<<20)+2*(1<<10);
  int64_t pwrite_length_3 = 9*(1<<20);
  char*buf_3 = new char[pwrite_length_3];
  ReadData(pwrite_file_3,buf_3,pwrite_length_3,object_offset_3-req_length);

  uint32_t ExpectCrc = Get_crc(RC_20M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_1,object_offset_1,pwrite_length_1,user_info);
  EXPECT_EQ(Ret,pwrite_length_1);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_3,object_offset_3,pwrite_length_3,user_info);
  EXPECT_EQ(Ret,pwrite_length_3);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_2,object_offset_2,pwrite_length_2,user_info);
  EXPECT_EQ(Ret,pwrite_length_2);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf_1;
  buf_1 = NULL;
  delete buf_2;
  buf_2 = NULL;
  delete buf_3;
  buf_3 = NULL;
}

TEST_F(TFS_Init,22_pwrite_object_large_hollow_in_large_file)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* object_name = "BBB";
  const char* local_file = RC_2K;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;
  int req_length = 2*(1<<10);

  int64_t offset = 2*(1<<10);
  int64_t length = 100*(1<<20);
  char*buf_rcv = new char[length];
  tfs::common::ObjectMetaInfo object_meta_info;
  tfs::common::CustomizeInfo customize_info;

  const char* pwrite_file_1 = RC_100M;
  int64_t object_offset_1 = 2*(1<<10);
  int64_t pwrite_length_1 = 40*(1<<20);
  char*buf_1 = new char[pwrite_length_1];
  ReadData(pwrite_file_1,buf_1,pwrite_length_1,object_offset_1-req_length);

  const char* pwrite_file_2 = RC_100M;
  int64_t object_offset_2= 40*(1<<20)+2*(1<<10);
  int64_t pwrite_length_2 = 20*(1<<20);
  char*buf_2 = new char[pwrite_length_2];
  ReadData(pwrite_file_2,buf_2,pwrite_length_2,object_offset_2-req_length);

  const char* pwrite_file_3 = RC_100M;
  int64_t object_offset_3 = 60*(1<<20)+2*(1<<10);
  int64_t pwrite_length_3 = 40*(1<<20);
  char*buf_3 = new char[pwrite_length_3];
  ReadData(pwrite_file_3,buf_3,pwrite_length_3,object_offset_3-req_length);

  uint32_t ExpectCrc = Get_crc(RC_100M,length);

  Ret = kv_meta_client.put_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.put_object(bucket_name,object_name,local_file,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_1,object_offset_1,pwrite_length_1,user_info);
  EXPECT_EQ(Ret,pwrite_length_1);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_3,object_offset_3,pwrite_length_3,user_info);
  EXPECT_EQ(Ret,pwrite_length_3);

  Ret = kv_meta_client.pwrite_object(bucket_name,object_name,buf_2,object_offset_2,pwrite_length_2,user_info);
  EXPECT_EQ(Ret,pwrite_length_2);

  Ret = kv_meta_client.pread_object(bucket_name,object_name,buf_rcv,offset,length,&object_meta_info,&customize_info,user_info);
  EXPECT_EQ(Ret,length);

  uint32_t ActualCrc = tfs::common::Func::crc(0,buf_rcv,length);
  EXPECT_EQ(ExpectCrc,ActualCrc);

  Ret = kv_meta_client.del_object(bucket_name,object_name,user_info);
  EXPECT_EQ(Ret,0);

  Ret = kv_meta_client.del_bucket(bucket_name,user_info);
  EXPECT_EQ(Ret,0);

  delete buf_1;
  buf_1 = NULL;
  delete buf_2;
  buf_2 = NULL;
  delete buf_3;
  buf_3 = NULL;
}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
