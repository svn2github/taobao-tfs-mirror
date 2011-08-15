/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "meta_server_service.h"

using namespace tfs::namemetaserver;
using namespace tfs::common;

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

int main()
{
  int64_t app_id = 5;
  int64_t uid = 5;

  char dir_path[512], new_dir_path[512], wrong_dir_path[512];
  char file_path[512], new_file_path[512], wrong_file_path[512];
  int ret = 1;

  tfs::namemetaserver::MetaServerService service;

  // create file or dir test
  // TODO read file to test if the file or dir is exist
  printf("-----------------------------------------------\n");
  printf("app_id %lu, uid: %lu\n", app_id, uid);

  sprintf(new_dir_path, "/taat");

  ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  printf("create dir %s, ret: %d\n", new_dir_path, ret);

  sprintf(dir_path, "/test");

  ret = service.create(app_id, uid, dir_path, DIRECTORY);
  printf("create dir %s, ret: %d\n", dir_path, ret);


  sprintf(new_file_path, "/taat/that");

  ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", new_file_path, ret);

  sprintf(new_file_path, "/taat/1");

  ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", new_file_path, ret);

  sprintf(new_file_path, "/taat/zz");

  ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", new_file_path, ret);

  sprintf(new_file_path, "/taat/zzzz");

  ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", new_file_path, ret);

  sprintf(file_path, "/test/that");

  ret = service.create(app_id, uid, file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", file_path, ret);


  printf("---------------------------\n");

  //FragInfo tfi;
  //tfi.cluster_id_ = 1;
  //for (int i = 0; i < MAX_FRAG_INFO_COUNT + 100; i++)
  //{
  //  FragMeta tmp;
  //  tmp.offset_ = 10 * i;
  //  tmp.file_id_ = 1;
  //  tmp.size_ = 9;
  //  tmp.block_id_ = 5;
  //  tfi.v_frag_meta_.push_back(tmp);
  //}

  //ret = service.write(app_id, uid, file_path, tfi);
  //printf("write file %s, ret: %d\n", file_path, ret);

  //bool sh;
  //ret = service.read(app_id, uid, file_path, MAX_FRAG_INFO_COUNT * 10, 40, tfi, sh);
  //printf("read ret = %d, cid = %d sh = %d \n", ret, tfi.cluster_id_, sh);
  //for (size_t i = 0; i < tfi.v_frag_meta_.size(); i++)
  //{
  //  dump_frag_meta(tfi.v_frag_meta_[i]);
  //}

  //tfi.v_frag_meta_.clear();
  //FragMeta tmp;
  //tmp.offset_ = 10 * (MAX_FRAG_INFO_COUNT+101);
  //tmp.file_id_ = 1;
  //tmp.size_ = 9;
  //tmp.block_id_ = 5;
  //tfi.v_frag_meta_.push_back(tmp);

  //ret = service.write(app_id, uid, file_path, tfi);
  //printf("write file %s, ret: %d\n", file_path, ret);

  //ret = service.read(app_id, uid, file_path, (MAX_FRAG_INFO_COUNT+101) * 10, 40, tfi, sh);
  //printf("read ret = %d, cid = %d sh = %d \n", ret, tfi.cluster_id_, sh);
  //for (size_t i = 0; i < tfi.v_frag_meta_.size(); i++)
  //{
  //  dump_frag_meta(tfi.v_frag_meta_[i]);
  //}
  //return 0;

  sprintf(wrong_dir_path, "/admin/test");
  ret = service.create(app_id, uid, wrong_dir_path, DIRECTORY);
  printf("create dir %s, ret: %d\n", wrong_dir_path, ret);

  sprintf(wrong_file_path, "/admin/test/1.txt");
  ret = service.create(app_id, uid, wrong_file_path, NORMAL_FILE);
  printf("create file %s, ret: %d\n", wrong_file_path, ret);

  //// rm file or dir test
  //// TODO read file to test if the file or dir is exist
  //uid = uid + 1;
  //printf("-----------------------------------------------\n");
  //printf("app_id %lu, uid: %lu\n", app_id, uid);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //printf("create dir %s, ret: %d\n", dir_path, ret);
  //ret = service.rm(app_id, uid, dir_path, DIRECTORY);
  //printf("rm dir %s, ret: %d\n", dir_path, ret);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //sprintf(file_path, "/test/1.txt");
  //ret = service.create(app_id, uid, file_path, NORMAL_FILE);
  //printf("create file %s, ret: %d\n", file_path, ret);
  //ret = service.rm(app_id, uid, dir_path, DIRECTORY);
  //printf("rm dir %s, ret: %d\n", dir_path, ret);
  //ret = service.rm(app_id, uid, file_path, NORMAL_FILE);
  //printf("rm file %s, ret: %d\n", file_path, ret);
  //ret = service.rm(app_id, uid, dir_path, DIRECTORY);
  //printf("rm dir %s, ret: %d\n", dir_path, ret);

  //sprintf(wrong_dir_path, "/admin/test");
  //ret = service.rm(app_id, uid, wrong_dir_path, DIRECTORY);
  //printf("rm dir %s, ret: %d\n", wrong_file_path, ret);

  //sprintf(wrong_file_path, "/admin/test/1.txt");
  //ret = service.rm(app_id, uid, wrong_file_path, NORMAL_FILE);
  //printf("rm file %s, ret: %d\n", wrong_file_path, ret);

  //// mv file or dir test

  //// --directory to directory
  //uid = uid + 1;
  //printf("-----------------------------------------------\n");
  //printf("app_id %lu, uid: %lu\n", app_id, uid);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", dir_path, ret);

  sprintf(new_dir_path, "/admin");
  //ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", new_dir_path, ret);

  //ret = service.mv(app_id, uid, dir_path, new_dir_path, DIRECTORY);
  //printf("mv file %s->%s, %d\n", dir_path, new_dir_path, ret);

  //ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  //printf("rm dir %s, %d\n", new_dir_path, ret);

  //ret = service.mv(app_id, uid, dir_path, new_dir_path, DIRECTORY);
  //printf("mv file %s->%s, %d\n", dir_path, new_dir_path, ret);

  //// --file to file
  //uid = uid + 1;
  //printf("app_id %lu, uid: %lu\n", app_id, uid);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", dir_path, ret);
  //sprintf(file_path, "/test/old.txt");
  //ret = service.create(app_id, uid, file_path, NORMAL_FILE);
  //printf("create file %s, %d\n", file_path, ret);

  //sprintf(new_dir_path, "/admin");
  //ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", new_dir_path, ret);
  sprintf(new_file_path, "/admin/new.txt");
  //ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  //printf("create file %s, %d\n", new_file_path, ret);

  //ret = service.mv(app_id, uid, file_path, new_file_path, NORMAL_FILE);
  //printf("mv file %s->%s, %d\n", file_path, new_file_path, ret);

  //ret = service.rm(app_id, uid, new_file_path, NORMAL_FILE);
  //printf("rm dir %s, %d\n", new_file_path, ret);

  //ret = service.mv(app_id, uid, file_path, new_file_path, NORMAL_FILE);
  //printf("mv dir %s->%s, %d\n", file_path, new_file_path, ret);

  //// --file to directory
  //uid = uid + 1;
  //printf("app_id %lu, uid: %lu\n", app_id, uid);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", dir_path, ret);
  //sprintf(file_path, "/test/old.txt");
  //ret = service.create(app_id, uid, file_path, NORMAL_FILE);
  //printf("create file %s, %d\n", file_path, ret);

  //sprintf(new_dir_path, "/admin");
  //ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", new_file_path, ret);

  //ret = service.mv(app_id, uid, file_path, new_dir_path, NORMAL_FILE);
  //printf("mv file %s->%s, %d\n", file_path, new_dir_path, ret);

  //ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  //printf("rm dir %s, %d\n", new_dir_path, ret);

  //ret = service.mv(app_id, uid, file_path, new_dir_path, NORMAL_FILE);
  //printf("mv file %s->%s, %d\n", file_path, new_dir_path, ret);

  //// --directory to file
  //uid = uid + 1;
  //printf("app_id %lu, uid: %lu\n", app_id, uid);

  //sprintf(dir_path, "/test");
  //ret = service.create(app_id, uid, dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", dir_path, ret);

  //sprintf(new_dir_path, "/admin");
  //ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  //printf("create dir %s, %d\n", new_file_path, ret);
  //sprintf(new_file_path, "/admin/new.txt");
  //ret = service.create(app_id, uid, new_file_path, NORMAL_FILE);
  //printf("create file %s, %d\n", new_file_path, ret);

  //ret = service.mv(app_id, uid, dir_path, new_file_path, DIRECTORY);
  //printf("mv file %s->%s, %d\n", dir_path, new_file_path, ret);

  //ret = service.rm(app_id, uid, new_file_path, NORMAL_FILE);
  //printf("rm dir %s, %d\n", new_file_path, ret);

  //ret = service.mv(app_id, uid, dir_path, new_file_path, DIRECTORY);
  //printf("mv file %s->%s, %d\n", dir_path, new_file_path, ret);

  return 0;
}
