/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: format_file_system.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2012-12-20
 *
 */
#include <stdio.h>
#include "common/parameter.h"
#include "dataserver/block_manager.h"
#include "common/version.h"

using namespace tfs::common;
using namespace tfs::dataserver;
using namespace std;

int main(int argc, char* argv[])
{
  char* conf_file = NULL;
  int help_info = 0;
  int i;
  std::string server_index;
  bool speedup = false;

  while ((i = getopt(argc, argv, "f:i:vhn")) != EOF)
  {
    switch (i)
    {
    case 'f':
      conf_file = optarg;
      break;
    case 'i':
      server_index = optarg;
      break;
    case 'n':
      speedup = true;
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

  if (conf_file == NULL || server_index.size() == 0 || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i index -h -v\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number\n");
    fprintf(stderr, "  -n speedup ds start up\n");
    fprintf(stderr, "  -v show version info\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return -1;
  }

  int ret = 0;
  TBSYS_CONFIG.load(conf_file);
  if ((ret = SYSPARAM_FILESYSPARAM.initialize(server_index)) != TFS_SUCCESS)
  {
    fprintf(stderr, "SysParam::load filesystemparam failed: %s\n", conf_file);
    return ret;
  }

  string super_block_path = string(SYSPARAM_FILESYSPARAM.mount_name_) + SUPERBLOCK_NAME;
  BlockManager block_manager(super_block_path);
  ret = block_manager.format(SYSPARAM_FILESYSPARAM);
  printf("format filesystem %s!\n", TFS_SUCCESS == ret ? "successfully" : "failed");

  return 0;
}
