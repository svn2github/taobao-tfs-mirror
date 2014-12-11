/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
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

const int32_t MAX_REMOVE_COUNT = 200; // 4T disk, avoid remove too many blocks by mistake

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  std::string server_index;
  int32_t remove_count = 0;

  while ((i = getopt(argc, argv, "f:i:c:vh")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'c':
      remove_count = atoi(optarg);
      break;
    case 'v':
      fprintf(stderr, "remove block tool, version: %s\n", Version::get_build_description());
      return 0;
    case 'h':
    default:
      help_info = 1;
      break;
    }
  }

  if ((conf_file == NULL) || (server_index.size() == 0) ||
      (remove_count <= 0) || (remove_count > MAX_REMOVE_COUNT) || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i server_index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -c remove_count (1-200)\n");
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
    }

    int32_t counter = 0;
    for (int i = 1; i <= info->total_main_block_count_ && counter < remove_count; i++)
    {
      BlockIndex index;
      ret = block_manager.get_super_block_manager().get_block_index(index, i);
      if (TFS_SUCCESS == ret && 0 == index.physical_block_id_)
      {
        std::stringstream block_file_path;
        block_file_path << info->mount_point_ << "/" << i;
        if (0 == access(block_file_path.str().c_str(), F_OK))
        {
          if (0 == unlink(block_file_path.str().c_str()))
          {
            printf("remove %s success\n", block_file_path.str().c_str());
            counter++;
          }
        }
      }
    }
    printf("Total remove %d blocks\n", counter);
  }

  return 0;
}
