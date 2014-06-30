/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: convert_name.cpp 413 2011-06-03 00:52:46Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "tbsys.h"
#include "clientv2/fsname.h"

using namespace tfs::clientv2;
using namespace tfs::common;

int main(int argc, char** argv)
{
  if (argc < 3 || argc > 6)
  {
    printf("Usgae: %s blockid fileid [ clusterid version suffix ]\n", argv[0]);
    printf("\tparameter invalid, default: suffix is NULL, clusterid is 0, version is 2 (27 characters).\n");
    return -1;
  }

  TfsFileNameVersion version_id = TFS_FILE_NAME_V2;
  uint64_t block_id = strtoull(argv[1], NULL, 10);
  uint64_t file_id = strtoull(argv[2], NULL, 10);
  int32_t cluster_id = 0;
  int32_t version = 2;
  if (argc >= 4)
  {
    cluster_id = atoi(argv[3]);
  }
  if (argc >= 5)
  {
    version = atoi(argv[4]);
  }
  std::string suffix;
  if (argc >= 6)
  {
    suffix = std::string(argv[5]);
  }
  if (cluster_id < 0 || cluster_id > 9 || version < 1 || version > 2)
  {
    printf("parameter invalid, clusterid must be 0 ~ 9, version must be 1 or 2\n");
    return -1;
  }

  if (1 == version)
  {
    version_id = TFS_FILE_NAME_V1;
  }
  FSName fs_name(block_id, file_id, cluster_id, version_id);
  fs_name.set_suffix(suffix.c_str());
  printf("blockid: %"PRI64_PREFIX "u, fileid: %"PRI64_PREFIX"u, name: %s\n", block_id, file_id, fs_name.get_name());
  return 0;
}
