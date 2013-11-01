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
#include "tbsys.h"
#include <stdio.h>
#include <vector>
#include <algorithm>
#include <sstream>
#include <map>
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

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  std::string server_index;
  std::stringstream log_name;
  std::string log_level;

  while ((i = getopt(argc, argv, "f:i:l:vh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'l':
      log_level = optarg;
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
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -l log_level -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -l log_level debug | info | warn | error\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  log_name << argv[0] << server_index << ".log";
  TBSYS_LOGGER.setLogLevel(log_level.c_str());
  TBSYS_LOGGER.setFileName(log_name.str().c_str());
  TBSYS_LOGGER.rotateLog(log_name.str().c_str());

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

  TBSYS_LOG(INFO, "check super block start");

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
        // dump_super_block(*info);
      }
    }

    vector<BlockIndex> index_vecs;
    index_vecs.push_back(BlockIndex()); // to make the index start at 1
    for (int i = 1; i <= info->max_block_index_element_count_; i++)
    {
      BlockIndex index;
      block_manager.get_super_block_manager().get_block_index(index, i);
      index_vecs.push_back(index);
    }

    std::map<uint64_t, uint64_t> extend_used_map;

    for (int i = 1; i <= info->total_main_block_count_; i++)
    {
      BlockIndex current = index_vecs[i];
      if (0 == current.logic_block_id_)
      {
        continue;
      }
      if (0 != current.physical_block_id_)  // allocated block
      {
        uint64_t curr_logic_block_id = current.logic_block_id_;
        while (0 != current.next_index_)
        {
          current = index_vecs[current.next_index_];
          if (current.physical_block_id_ == 0)
          {
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> extend block %d no allocated.",
                curr_logic_block_id, current.physical_block_id_);
            continue;
          }

          if (current.logic_block_id_ != curr_logic_block_id)
          {
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> extend block %d belongs to"
                "another logic block %"PRI64_PREFIX"u.",
                curr_logic_block_id, current.physical_block_id_, current.logic_block_id_);
            continue;
          }

          BlockIndex& alloc = index_vecs[current.physical_file_name_id_];
          if (alloc.physical_block_id_ == 0)
          {
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d not allocated, extend block: %d",
                curr_logic_block_id, current.physical_file_name_id_, current.physical_block_id_);
            continue;
          }

          if (alloc.split_flag_ != BLOCK_SPLIT_FLAG_YES)
          {
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d not splited, extend block: %d",
                curr_logic_block_id, current.physical_file_name_id_, current.physical_block_id_);
            continue;
          }

          uint64_t extend_key = (((uint64_t)alloc.physical_file_name_id_) << 32) + current.index_;
          std::map<uint64_t, uint64_t>::iterator iter = extend_used_map.find(extend_key);
          if (iter != extend_used_map.end())
          {
            TBSYS_LOG(WARN, "logic_block %"PRI64_PREFIX"u ==> alloc block %d index %d is linked to "
                "another logic block %"PRI64_PREFIX"u",
                curr_logic_block_id, alloc.physical_file_name_id_, current.index_, iter->second);
          }
          else
          {
            extend_used_map.insert(make_pair(extend_key, curr_logic_block_id));
          }
        }
      }
    }
  }

  TBSYS_LOG(INFO, "check super block end");

  return 0;
}
