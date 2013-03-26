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
#include"func.h"

//TEST_F(TFS_Without_Init,01_initialize)
//{
//  const char*kms_addr="10.232.36.210:7201";
//  const char*rc_addr="10.232.36.202:9202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_EQ(Ret,0);
//}
//

TEST_F(TFS_Without_Init,02_initialize_int64)
{
  const char*kms_addr="10.232.36.210:7201";
  const char*rc_addr="10.232.36.202:5202";
  int Ret ;

  int64_t kms_addr_int64 = tfs::common::Func::get_host_ip(kms_addr);
  std::cout<<kms_addr_int64<<endl;

//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr_int64,rc_addr);
//  EXPECT_EQ(Ret,0);
}

//TEST_F(TFS_Without_Init,03_initialize_wrong_rc_addr_port)
//{
//  const char*kms_addr="10.232.36.210:7201";
//  const char*rc_addr="10.232.36.202:7202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,04_initialize_wrong_rc_addr_ip)
//{
//  const char*kms_addr="10.232.36.210:7201";
//  const char*rc_addr="10.232.36.213:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,05_initialize_wrong_rc_addr_null)
//{
//  const char*kms_addr="10.232.36.210:7201";
//  const char*rc_addr=NULL;
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,06_initialize_wrong_rc_addr_empty)
//{
//  const char*kms_addr="10.232.36.210:7201";
//  const char*rc_addr="";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,07_initialize_wrong_kms_addr_port)
//{
//  const char*kms_addr="10.232.36.210:7202";
//  const char*rc_addr="10.232.36.202:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,08_initialize_wrong_kms_addr_ip)
//{
//  const char*kms_addr="10.232.36.213:7201";
//  const char*rc_addr="10.232.36.202:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,09_initialize_wrong_kms_addr_null)
//{
//  const char*kms_addr=NULL;
//  const char*rc_addr="10.232.36.202:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,10_initialize_wrong_kms_addr_empty)
//{
//  const char*kms_addr="";
//  const char*rc_addr="10.232.36.202:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//
//TEST_F(TFS_Without_Init,11_initialize_wrong_kms_addr_int64)
//{
//  int64_t kms_addr= 1234567890;
//  const char*rc_addr="10.232.36.202:5202";
//  int Ret ;
//
//  KvMetaClient*kv_meta_client = new KvMetaClient();
//  Ret = kv_meta_client->initialize(kms_addr,rc_addr);
//  EXPECT_NE(Ret,0);
//}
//

int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
