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
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#include "tfs_client_impl_init.h"

TEST_F(TfsInit, 01_fetch_file_right_small)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  char local_file[10] = "abc";
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int32_t data_len = 100*(1<<10);

  //save file and generate src crc
  data_in(a100K, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch file and generate dest crc
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_EQ(data_len, Ret);
  data_in(local_file, buf, 0, data_len);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink localfile and rm tfsfile
  Ret = unlink(local_file);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 02_fetch_file_null_local_file)
{
  char tfs_name[19] = "/test";
  const char* local_file = NULL;
  int32_t data_len = 100*(1<<10);
  int64_t Ret = -1;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_GT(0, Ret);

  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 03_fetch_file_empty_local_file)
{
  char tfs_name[19] = "/test";
  int32_t data_len = 100*(1<<10);
  int64_t Ret = -1;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_file(appId, uid, "", tfs_name);
  EXPECT_GT(0, Ret);

  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 05_fetch_file_null_tfs_file)
{
  const char* tfs_name = NULL;
  char local_file[10] = "abc";
  int64_t Ret = -1;

  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 06_fetch_file_empty_tfs_file)
{
  const char* tfs_name = "";
  char local_file[10] = "abc";
  int64_t Ret = -1;
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 07_fetch_file_not_exist_tfs_file)
{
  char tfs_name[19] = "test";
  char local_file[10] = "abc";
  int64_t Ret = -1;
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_GT(0, Ret);
}

/*
TEST_F(TfsInit, 08_fetch_file_wrong2_tfs_file)
{
  char tfs_name[19] = "/test/";
  int64_t Ret = -1;
  Ret = tfsclient->fetch_file(appId, uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
}*/

TEST_F(TfsInit, 09_fetch_file_empty_create_file)
{
  char local_file[10] = "abc";
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->create_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_EQ(0, Ret);

  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = unlink(local_file);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 11_fetch_file_with_same_dir)
{
  char tfs_name[19] = "/test";
  char local_file[10] = "abc";
  int64_t Ret = -1;
  Ret = tfsclient->create_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_GT(0, Ret);
  Ret = tfsclient->rm_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 12_fetch_file_many_times)
{
  char tfs_name[19] = "/test";
  char local_file[10] = "abc";
  char buf[100*(1<<10)];
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int32_t data_len = 100*(1<<10);

  //save file and generate src crc
  data_in(a100K, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch file and generate dest crc
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_EQ(data_len, Ret);
  data_in(local_file, buf, 0, data_len);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink localfile and rm tfsfile
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = unlink(local_file);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 13_fetch_file_large_local_file) //6M
{
  char tfs_name[19] = "/test";
  char local_file[10] = "abc";
  static char buf[1<<30];
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int32_t data_len =1<<30;

  //save file and generate src crc
  data_in(a1G, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a1G, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch file and generate dest crc
  Ret = tfsclient->fetch_file(appId, uid, local_file, tfs_name);
  EXPECT_EQ(data_len, Ret);
  data_in(local_file, buf, 0, data_len);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink localfile and rm tfsfile
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = unlink(local_file);
  EXPECT_EQ(0, Ret);
}


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
