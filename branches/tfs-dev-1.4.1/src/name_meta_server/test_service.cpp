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
#include "common/parameter.h"

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
  int64_t app_id = 8;
  int64_t uid = 5;

  char new_dir_path[512];
  //, wrong_dir_path[512];
  //char file_path[512], new_file_path[512], wrong_file_path[512];
  int ret = 1;

  tfs::namemetaserver::MetaServerService service;
  NameMeatServerParameter::DbInfo dbinfo;
  dbinfo.conn_str_ = "10.232.36.205:3306:tfs_name_db";
  dbinfo.user_ = "root";
  dbinfo.passwd_ = "root";


  SYSPARAM_NAMEMETASERVER.db_infos_.push_back(dbinfo);
  SYSPARAM_NAMEMETASERVER.max_pool_size_ = 5;
  service.initialize(0, NULL);

  // create file or dir test
  // TODO read file to test if the file or dir is exist
  printf("app_id %lu, uid: %lu\n", app_id, uid);
  TBSYS_LOGGER.setLogLevel("warn");
  PROFILER_SET_THRESHOLD(100);
  PROFILER_SET_STATUS(1);

  TBSYS_LOG(ERROR, "start create");
  int times = 1;
  for(int i = 0; i < 1000 * times; i++)
  {
    sprintf(new_dir_path, "/test%d", i);
    ret = service.create(app_id, uid, new_dir_path, DIRECTORY);
  }
  TBSYS_LOG(ERROR, "end create");
  TBSYS_LOG(ERROR, "start rm");
  //for(int i = 0; i < 1000 * times; i++)
  //{
  //  sprintf(new_dir_path, "/test%d", i);
  //  ret = service.rm(app_id, uid, new_dir_path, DIRECTORY);
  //}
  TBSYS_LOG(ERROR, "end rm");

  return 0;
}
