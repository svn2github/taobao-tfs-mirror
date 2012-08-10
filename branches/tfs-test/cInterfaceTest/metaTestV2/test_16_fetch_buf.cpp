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

TEST_F(TfsInit, 01_fetch_buf_right_small)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);

  //save file and generate src crc
  data_in(a100K, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch buf and generate dest crc
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_EQ(data_len, Ret);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink rm tfsfile
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 02_fetch_buf_zero_length)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int32_t data_len = 100*(1<<10);
  int64_t Ret = -1;
  int64_t offset = 0;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, 0, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 03_fetch_buf_wrong_length)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int32_t data_len = 100*(1<<10);
  int64_t offset = 0;
  int64_t Ret = -1;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, -1, tfs_name);
  EXPECT_GT(0, Ret);

  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 04_fetch_buf_less_length)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int32_t data_len = 100*(1<<10);
  int64_t offset = 0;
  int64_t Ret = -1;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len-1, tfs_name);
  EXPECT_EQ(data_len-1, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 05_fetch_buf_more_length)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int32_t data_len = 100*(1<<10);
  int64_t Ret = -1;
  int64_t offset = 0;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len+1, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 06_fetch_buf_wrong_offset)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int32_t data_len = 100*(1<<10);
  int64_t offset = -1;
  int64_t Ret = -1;

  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_GT(0, Ret);

  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 07_fetch_buf_not_exist_tfs_file)
{
  char tfs_name[19] = "test";
  char buf[100*(1<<10)];
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 08_fetch_buf_null_tfs_file)
{
  char buf[100*(1<<10)];
  int64_t Ret = -1;
  int32_t data_len = 100*(1<<10);
  int64_t offset = 0;
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, NULL);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 09_fetch_buf_empty_tfs_file)
{
  char buf[100*(1<<10)];
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, "");
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 10_fetch_buf_empty_create_file)
{
  char buf[100*(1<<10)];
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);
  Ret = tfsclient->create_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 11_fetch_buf_with_same_dir)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);

  Ret = tfsclient->create_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_GT(0, Ret);
  Ret = tfsclient->rm_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 12_fetch_buf_many_times)
{
  char tfs_name[19] = "/test";
  char buf[100*(1<<10)];
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 100*(1<<10);

  //save file and generate src crc
  data_in(a100K, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch file and generate dest crc
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_EQ(data_len, Ret);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink rm tfsfile
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 13_fetch_buf_large_local_file) //6M
{
  char tfs_name[19] = "/test";
  static char buf[1<<30];
  uint32_t src_crc = -1;
  uint32_t dest_crc = -1;
  int64_t Ret = -1;
  int64_t offset = 0;
  int32_t data_len = 1<<30;

  //save file and generate src crc
  data_in(a6M, buf, 0, data_len);
  src_crc = Func::crc(0, buf, data_len);
  Ret = tfsclient->save_file(uid, a1G, tfs_name);
  EXPECT_EQ(data_len, Ret);

  //fetch file and generate dest crc
  Ret = tfsclient->fetch_buf(appId, uid, buf, offset, data_len, tfs_name);
  EXPECT_EQ(data_len, Ret);
  dest_crc = Func::crc(0, buf, data_len);
  EXPECT_EQ(src_crc, dest_crc);

  //unlink localfile and rm tfsfile
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
