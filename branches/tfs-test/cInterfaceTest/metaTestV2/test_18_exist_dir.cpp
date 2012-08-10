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

TEST_F(TfsInit, Test_01_exist_dir)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_02_not_exist_dir)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_03_wrong_dir)
{
  int64_t Ret = -1;
  char dir_path[20] = "abcdefg";
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_04_exist_dir_wrong_uid)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid+1, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->rm_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_05_exist_same_file1)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_file(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->rm_file(uid, dir_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_06_exist_same_file2)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_file(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);

  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(0, Ret);

  Ret = tfsclient->rm_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_file(uid, dir_path);
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_07_rm_exist_dir)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
}

TEST_F(TfsInit, Test_08_mv_exist_dir)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->mv_dir(uid, dir_path,"/test1");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, dir_path) ? 0 : 1;
  EXPECT_EQ(1, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, "/test1") ? 0 : 1;
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, "/test1");
  EXPECT_EQ(0, Ret);
}

TEST_F(TfsInit, Test_09_exist_dir_com)
{
  int64_t Ret = -1;
  char dir_path[20] = "/test";
  Ret = tfsclient->create_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->create_dir(uid, "/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->create_dir(uid, "/test/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->is_dir_exist(appId, uid, "/test/test/test") ? 0 : 1;
  EXPECT_EQ(0, Ret);
  
  Ret = tfsclient->rm_dir(uid, "/test/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, "/test/test");
  EXPECT_EQ(0, Ret);
  Ret = tfsclient->rm_dir(uid, dir_path);
  EXPECT_EQ(0, Ret);
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
