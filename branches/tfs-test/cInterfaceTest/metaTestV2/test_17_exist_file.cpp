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

TEST_F(TfsInit, Test_01_exist_file)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, file_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_02_not_exist_file)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_03_wrong_file)
{
  int64_t Ret = -1;
  char file_path[20] = "abcdefg";
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_04_exist_file_wrong_uid)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid+1, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->rm_file(uid, file_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_05_exist_same_dir1)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->rm_dir(uid, file_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_06_exist_same_dir2)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->create_file(uid, file_path);
  EXPECT_EQ(0, Ret);

  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(0, Ret);

  Ret = tfsclient->rm_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, file_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_07_rm_exist_file)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_08_mv_exist_file)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_file(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->mv_file(uid, file_path,"/test1");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, file_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, "/test1") ? 0 : 1;
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, "/test1");
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_09_exist_file_com)
{
  int64_t Ret = -1;
  char file_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, file_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->create_file(uid, "/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_file_exist(appId, uid, "/test/test") ? 0 : 1;
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, "/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, "/test");
  EXPECT_EQ(0, Ret);
}
int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
