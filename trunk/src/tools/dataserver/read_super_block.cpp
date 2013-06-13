/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: clear_file_system.cpp 380 2011-05-30 08:04:16Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-01-10
 *
 */
#include <stdio.h>
#include <vector>
#include <algorithm>
#include "common/parameter.h"
#include "dataserver/block_manager.h"
#include "common/version.h"

using namespace tfs::dataserver;
using namespace tfs::common;
using namespace std;

void dump_super_block(const SuperBlockInfo& info)
{
  printf("%-25s%s\n", "mount_tag:", info.mount_tag_);
  printf("%-25s%s\n", "mount_path:", info.mount_point_);
  printf("%-25s%"PRI64_PREFIX"d\n", "mount_space:", info.mount_point_use_space_);
  printf("%-25s%d\n", "mount fs type:", info.mount_fs_type_);
  printf("%-25s%d\n", "sb reserve offset:", info.superblock_reserve_offset_);
  printf("%-25s%d\n", "index reserve offset:", info.block_index_offset_);
  printf("%-25s%d\n", "max block element:", info.max_block_index_element_count_);
  printf("%-25s%d\n", "total main block count:", info.total_main_block_count_);
  printf("%-25s%d\n", "used main block count:", info.used_main_block_count_);
  printf("%-25s%d\n", "max main block size:", info.max_main_block_size_);
  printf("%-25s%d\n", "max extend block size:", info.max_extend_block_size_);
  printf("%-25s%d\n", "max hash bucket count:", info.max_hash_bucket_count_);
  printf("%-25s%d\n", "hash bucket count:", info.hash_bucket_count_);
  printf("%-25s%lf\n", "max use block ratio:", info.max_use_block_ratio_);
  printf("%-25s%lf\n", "max use hash ratio:", info.max_use_hash_bucket_ratio_);
}


void dump_block_index_header()
{
  printf("%-22s%-10s%-10s%-10s%-10s%-8s%-6s%-6s%-6s\n",
      "LOGIC_ID",
      "PHY_ID",
      "NAME_ID",
      "NEXT",
      "PREV",
      "INDEX",
      "STAT",
      "FLAG",
      "OVER");
}

void dump_block_index(const BlockIndex& index)
{
  printf("%-22"PRI64_PREFIX"u%-10d%-10d%-10d%-10d%-8d%-6d%-6d%-6d\n",
      index.logic_block_id_,
      index.physical_block_id_,
      index.physical_file_name_id_,
      index.next_index_,
      index.prev_index_,
      index.index_,
      index.status_,
      index.split_flag_,
      index.split_status_);
}

bool compare_by_logic_id(const BlockIndex& left, const BlockIndex& right)
{
  bool res = false;
  if (left.logic_block_id_ < right.logic_block_id_)
  {
    res = true;
  }
  else if (left.logic_block_id_ > right.logic_block_id_)
  {
    res = false;
  }
  else
  {
    res = left.physical_block_id_ < right.physical_block_id_;
  }
  return res;
}

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  std::string server_index;

  while ((i = getopt(argc, argv, "f:i:vh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'v':
      fprintf(stderr, "create tfs file system tool, version: %s\n", Version::get_build_description());
      return 0;
    case 'h':
    default:
      help_info = 1;
      break;
    }
  }

  if ((conf_file == NULL) || (server_index.size() == 0) || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  int ret = 0;
  if (EXIT_SUCCESS != TBSYS_CONFIG.load(conf_file))
  {
    cerr << "load config error conf_file is " << conf_file;
    return TFS_ERROR;
  }
  if ((ret = SYSPARAM_DATASERVER.initialize(conf_file, server_index)) != TFS_SUCCESS)
  {
    cerr << "SysParam::load file system param failed:" << conf_file << endl;
    return ret;
  }

  string super_block_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
  BlockManager block_manager(super_block_path);
  {
    SuperBlockInfo* info = NULL;
    ret = block_manager.get_super_block_manager().load();
    if (TFS_SUCCESS == ret)
    {
      ret =  block_manager.get_super_block_manager().get_super_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        dump_super_block(*info);
      }
    }

    printf("\n\n");

    dump_block_index_header();

    vector<BlockIndex> index_vecs;
    for (int i = 1; i < info->max_block_index_element_count_; i++)
    {
      BlockIndex index;
      block_manager.get_super_block_manager().get_block_index(index, i);
      if (0 != index.physical_block_id_)
      {
        index_vecs.push_back(index);
      }
    }

    sort(index_vecs.begin(), index_vecs.end(), compare_by_logic_id);
    for (unsigned i = 1; i < index_vecs.size(); i++)
    {
      dump_block_index(index_vecs[i]);
    }
  }

  return 0;
}
