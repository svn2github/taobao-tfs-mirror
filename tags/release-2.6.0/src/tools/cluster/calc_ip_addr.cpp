/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: blocktool.cpp 432 2011-06-08 07:06:11Z nayan@taobao.com $
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>
#include <set>
#include <iostream>

#include "tbsys.h"
#include "common/internal.h"
#include "common/func.h"


using namespace std;
using namespace tfs::common;

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -m [-h]\n", name);
  fprintf(stderr, "       -s ip list\n");
  fprintf(stderr, "       -m mask\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

int main(int argc, char* argv[])
{
  std::string mask("");
  std::string ip_list_file("");
  int i = 0;
  while ((i = getopt(argc, argv, "s:m:h")) != EOF)
  {
    switch (i)
    {
    case 's':
      ip_list_file= optarg;
      break;
    case 'm':
      mask= optarg;
      break;
    case 'h':
    default:
      usage(argv[0]);
    }
  }

  if ((ip_list_file.empty())
      || (ip_list_file.compare(" ") == 0)
      || mask.empty()
      || (mask.compare(" ") == 0))
  {
    usage(argv[0]);
  }

  FILE* file= NULL;
  int ret = TFS_SUCCESS;
  if (access(ip_list_file.c_str(), R_OK) < 0)
  {
    printf("[ERROR] access input ip addr list file: %s, error: %s\n", ip_list_file.c_str(), strerror(errno));
  }
  else if ((file= fopen(ip_list_file.c_str(), "r")) == NULL)
  {
    printf("[ERROR] open input ip addr list file: %s, error: %s\n", ip_list_file.c_str(), strerror(errno));
  }
  else
  {
    char ip[64];
    uint32_t mask_value = Func::get_addr(mask.c_str());
    while (fscanf(file, "%s", ip) != EOF)
    {
      uint64_t addr = Func::str_to_addr(ip, 3200);
      uint32_t lan = Func::get_lan(addr, mask_value);
      printf("[INFO] ipaddr: %s, lan: %u\n", ip, lan);
    }
  }
  return ret;
}
