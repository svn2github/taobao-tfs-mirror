/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: read_index_tool.cpp 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <iostream>
#include <string>
#include <dirent.h>
#include "common/internal.h"
#include "dataserver/ds_define.h"
#include "common/file_opv2.h"
#include "common/version.h"
#include "clientv2/fsname.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::dataserver;
using namespace tfs::clientv2;


// exclude "."
static int index_filter(const struct dirent* entry)
{
  return ::index(entry->d_name, '.') ? 0 : 1;
}

bool in_period(const time_t time, const time_t start_time, const time_t end_time)
{
  return (time >= start_time && time < end_time);
}

void dump_file_name(const int32_t cluster_id, const uint64_t block_id, const uint64_t file_id)
{
  FSName fs_name(block_id, file_id, cluster_id, TFS_FILE_NAME_V2);
  //printf("blockid: %"PRI64_PREFIX "u, fileid: %"PRI64_PREFIX"u, name: %s\n", block_id, file_id, fs_name.get_name());
  printf("%s\n", fs_name.get_name());
}


void dump_all_file_names(const int32_t cluster_id, const uint64_t block_id,
    FileOperation& file_op, const int32_t offset, const int32_t items,
    const time_t start_time, const time_t end_time)
{
  int ret = TFS_SUCCESS;
  FileInfoV2 file_info;
  for (int i = 0; i < items; i++)
  {
    ret = file_op.pread((char*)(&file_info), FILE_INFO_V2_LENGTH,
        offset + i * FILE_INFO_V2_LENGTH);
    if (FILE_INFO_V2_LENGTH == ret)
    {
      if (INVALID_FILE_ID != file_info.id_ && in_period(file_info.modify_time_, start_time, end_time))
      {
        dump_file_name(cluster_id, block_id, file_info.id_);
        while (file_info.next_ != 0)
        {
          int32_t slot = file_info.next_;
          ret = file_op.pread((char*)(&file_info), FILE_INFO_V2_LENGTH,
              offset + slot * FILE_INFO_V2_LENGTH);
          if (FILE_INFO_V2_LENGTH == ret && in_period(file_info.modify_time_, start_time, end_time))
          {
            dump_file_name(cluster_id, block_id, file_info.id_);
          }
          else
          {
            break;
          }
        }
      }
    }
    else
    {
      break;
    }
  }
}

void dump_index_file(const char *index_file, const int32_t cluster_id, const time_t start_time, const time_t end_time)
{
  FileOperation* file_op = new FileOperation(index_file);

  uint64_t block_id = 0;
  IndexHeaderV2 header;
  int ret = file_op->pread((char*)(&header), INDEX_HEADER_V2_LENGTH, 0);
  if (INDEX_HEADER_V2_LENGTH == ret)
  {
    block_id = header.info_.block_id_;
  }

  // normal block
  if (!IS_VERFIFY_BLOCK(block_id))
  {
    dump_all_file_names(cluster_id,
        block_id, *file_op,
        INDEX_HEADER_V2_LENGTH,
        header.file_info_bucket_size_, start_time, end_time);
  }
}

int main(int argc, char* argv[])
{
  char *conf_file = NULL;
  time_t start_time = 0;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  time_t end_time = tv.tv_sec;
  char *disk_index = NULL;
  int32_t cluster_id = 0;

  int32_t i = 0;
  int32_t help_info = 0;
  while ((i = getopt(argc, argv, "f:s:e:i:c:h")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 's':
        start_time = tbsys::CTimeUtil::strToTime(optarg);
        break;
      case 'e':
        end_time = tbsys::CTimeUtil::strToTime(optarg);
        break;
      case 'i':
        disk_index = optarg;
        break;
      case 'c':
        cluster_id = atoi(optarg);
        break;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || NULL == disk_index || 0 == cluster_id || help_info)
  {
    fprintf(stderr, "version: %s\n", Version::get_build_description());
    fprintf(stderr, "Usage: %s -f conf_file -i disk_index -c cluster_id [-s start_time -e end_tiem -h]\n", argv[0]);
    fprintf(stderr, "\n");
    return -1;
  }

  int32_t ret = 0;
  TBSYS_CONFIG.load(conf_file);
  if (TFS_SUCCESS != (ret = SYSPARAM_FILESYSPARAM.initialize(disk_index)))
  {
    fprintf(stderr, "SysParam::loadFileSystemParam failed: %s", conf_file);
    return ret;
  }

  // loop all index
  int32_t index = 0;
  struct dirent** index_names= NULL;
  std::string path(SYSPARAM_FILESYSPARAM.mount_name_ + INDEX_DIR_PREFIX);
  int32_t num = scandir(path.c_str(), &index_names, index_filter, NULL);
  for (index = 0; index < num; ++index)
  {
    std::string file_path = path + index_names[index]->d_name;
    fprintf(stderr, "will dump index file: %s from time: %d to %d \n", file_path.c_str(), (int32_t)start_time, (int32_t)end_time);
    dump_index_file(file_path.c_str(), cluster_id, start_time, end_time);
    free(index_names[index]);
  }
  free(index_names);

  return 0;
}
