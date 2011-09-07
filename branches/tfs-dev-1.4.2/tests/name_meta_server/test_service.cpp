/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_bit_map.cpp 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *    daoan(daoan@taobao.com)
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "meta_cache_info.h"
#include "meta_cache_helper.h"
#include "meta_server_service.h"
#include "common/parameter.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;

class ServiceTest: public ::testing::Test
{
  public:
    ServiceTest()
    {
    }
    ~ServiceTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

const int32_t FRAG_LEN = 65535;
void dump_meta_info(const MetaInfo& metainfo)
{
  int size = metainfo.file_info_.size_;
  int nlen = metainfo.file_info_.name_.length();

  TBSYS_LOG(INFO, "size = %d, name_len = %d", size, nlen);
}
void dump_frag_meta(const FragMeta& fm)
{
  TBSYS_LOG(INFO, "offset_ %ld file_id_ %lu size_ %d block_id_ %u",
      fm.offset_, fm.file_id_, fm.size_, fm.block_id_);
}
tfs::namemetaserver::MetaServerService service;
NameMeatServerParameter::DbInfo dbinfo;
int64_t app_id = 18;
int64_t uid = 5;

TEST_F(ServiceTest, create_dir)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test1");
  sprintf(new_dir_path, "/test1");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->dump();

  TBSYS_LOG(INFO, "create /test1/");
  sprintf(new_dir_path, "/test1/");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_ERROR, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test1/ff/");
  sprintf(new_dir_path, "/test1/ff/");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_ERROR, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();
  //now we have  /test1/   /test1/ff/  /test2/
}
TEST_F(ServiceTest, create_file)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->dump();

  TBSYS_LOG(INFO, "create /test3/f1");
  sprintf(new_dir_path, "/test3/f1");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_ERROR, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test1/ff/");
  sprintf(new_dir_path, "/test1/ff/");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_ERROR, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test2");
  sprintf(new_dir_path, "/test2");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_ERROR, ret);
  p->dump();
  //now we have  /test1/   /test1/ff/  /test2/ff
}
TEST_F(ServiceTest, rm_dir)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "rm /test1/ff/f1");
  sprintf(new_dir_path, "/test1/ff/f1");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->dump();

  TBSYS_LOG(INFO, "rm /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "create /test2/");
  sprintf(new_dir_path, "/test2/");
  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  //now we have  /test1/    /test2/ff
}
TEST_F(ServiceTest, rm_file)
{
  char new_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "rm /test1/ff");
  sprintf(new_dir_path, "/test1/ff");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->dump();

  TBSYS_LOG(INFO, "rm /test3/f1");
  sprintf(new_dir_path, "/test3/f1");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "rm /test1");
  sprintf(new_dir_path, "/test1");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "rm /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.rm(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();

  //now we have  /test1/   /test2/
} 
TEST_F(ServiceTest, mv_dir)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;
  TBSYS_LOG(INFO, "create /test2/ff");
  sprintf(new_dir_path, "/test2/ff");
  ret = service.create(app_id, uid, new_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->dump();

  TBSYS_LOG(INFO, "mv /test2/ /test1/test3/t2");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1/test3/t2");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "mv /test2/ /test1");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "mv /test2/ /test1/test3");
  sprintf(new_dir_path, "/test2");
  sprintf(dest_dir_path, "/test1/test3");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();
  //now we have  /test1/test3/ff 
} 

TEST_F(ServiceTest, mv_file)
{
  char new_dir_path[1024];
  char dest_dir_path[1024];
  int ret = 0;

  CacheRootNode* p = service.get_root_node(app_id, uid);

  TBSYS_LOG(INFO, "mv /test1/test3/ff /test2");
  sprintf(new_dir_path, "/test1/test3/ff");
  sprintf(dest_dir_path, "/test2");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, DIRECTORY);
  EXPECT_NE(TFS_SUCCESS, ret);
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, NORMAL_FILE);
  EXPECT_EQ(TFS_SUCCESS, ret);
  p->dump();

  TBSYS_LOG(INFO, "mv /test1/test3 /test4");
  sprintf(new_dir_path, "/test1/test3");
  sprintf(dest_dir_path, "/test4");
  ret = service.mv(app_id, uid, new_dir_path, dest_dir_path, NORMAL_FILE);
  EXPECT_NE(TFS_SUCCESS, ret);
  p->dump();

  //now we have  /test1/test3/ /test2
} 

int main(int argc, char* argv[])
{
  MemHelper::init(5,5,5);
  dbinfo.conn_str_ = "10.232.36.205:3306:tfs_name_db";
  dbinfo.user_ = "root";
  dbinfo.passwd_ = "root";

  SYSPARAM_NAMEMETASERVER.db_infos_.push_back(dbinfo);
  SYSPARAM_NAMEMETASERVER.max_pool_size_ = 5;
  service.initialize(0, NULL);
  CacheRootNode* p = service.get_root_node(app_id, uid);
  p->app_id_ = app_id;
  p->user_id_ = uid;
  printf("app_id %lu, uid: %lu\n", app_id, uid);
  //TBSYS_LOGGER.setLogLevel("debug");
  PROFILER_SET_STATUS(0);


  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
