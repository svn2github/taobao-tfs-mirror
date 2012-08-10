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

TEST_F(TfsInit, 01_save_file_right_small)
{
  const char* tfs_name = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(100*(1<<10), Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 02_save_file_null_local_file)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, NULL, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 03_save_file_empty_local_file)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, "", tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 04_save_file_wrong_local_file)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, "aabbcc", tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 05_save_file_null_tfs_file)
{
  const char* tfs_name = NULL;
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 06_save_file_empty_tfs_file)
{
  char tfs_name[1] = "";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
}

TEST_F(TfsInit, 07_save_file_wrong1_tfs_file)
{
  char tfs_name[19] = "test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
}

/*
TEST_F(TfsInit, 08_save_file_wrong2_tfs_file)
{
  char tfs_name[19] = "/test/";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
}*/

TEST_F(TfsInit, 09_save_file_wrong3_tfs_file)
{
  char tfs_name[19] = "/";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, 10_save_file_not_exist_parent_file)
{
  char tfs_name[19] = "/test/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(100*(1<<10), Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, "/test");
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 11_save_file_with_same_dir)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->create_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(100*(1<<10), Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 12_save_file_many_times)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_EQ(100*(1<<10), Ret);
  Ret = tfsclient->save_file(uid, a100K, tfs_name);
  EXPECT_GT(0, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, 13_save_file_large_local_file)
{
  char tfs_name[19] = "/test";
  int64_t Ret = -1;
  Ret = tfsclient->save_file(uid, a1G, tfs_name);
  EXPECT_EQ(1<<30, Ret);
  Ret = tfsclient->rm_file(uid, tfs_name);
  EXPECT_EQ(0, Ret);
}


int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
