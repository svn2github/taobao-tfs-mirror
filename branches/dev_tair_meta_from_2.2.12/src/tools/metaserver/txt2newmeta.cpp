/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: showssm.cpp 199 2011-04-12 08:49:55Z duanfei@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <vector>
#include <string>
#include <string.h>
#include <signal.h>
#include <Memory.hpp>

#include "common/parameter.h"
#include "common/config_item.h"
#include "name_meta_server/meta_store_manager.h"
#include "name_meta_server/meta_server_service.h"

using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;
using namespace tfs::namemetaserver;

const int MAX_OBJECT_NAME_LEN = 256;

const int MAGIC_NUMBER0 = 0x4f264975;
const int MAGIC_NUMBER1 = 0x5f375a86;
const int MAGIC_NUMBER2 = 0x60486b97;
const int MAGIC_NUMBER3 = 0x71597ca8;
const int MAGIC_NUMBER4 = 0x826a8db9;

int stop = 0;
void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
      stop = 1;
      break;
  }
}
int usage (const char* n)
{
  printf("%s -f ource_file -d S3_server -l log_file\n", n);
  return 0;
}


void transfer(const string &source_file_name, const string &s3_server)
{
  UNUSED(s3_server);
  int ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  s_fd = fopen(source_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", source_file_name.c_str());
    ret =  TFS_ERROR;
  }
  char buff[MAX_OBJECT_NAME_LEN * 2];
  int magic_number;
  string object_name;
  int full_name_len;
  MetaInfo meta_info;
  int read_len = 0;
  int64_t app_id = 0;
  int64_t uid = 0;

  while(TFS_SUCCESS == ret && 1 != stop)
  {
    read_len = fread(&magic_number, sizeof(magic_number), 1, s_fd);
    if (read_len <= 0) break;
    if (MAGIC_NUMBER0 == magic_number)
    {
      fread(&app_id, sizeof(app_id), 1, s_fd);
      fread(&uid, sizeof(uid), 1, s_fd);
      TBSYS_LOG(INFO, "deal appid:%ld uid:%ld", app_id, uid);
      continue;
    }else if (magic_number != MAGIC_NUMBER1)
    {
      TBSYS_LOG(INFO, "magic number error");
      break;
    }
    fread(&full_name_len, sizeof(full_name_len), 1, s_fd);
    if (full_name_len > MAX_OBJECT_NAME_LEN || full_name_len < 0)
    {
      TBSYS_LOG(INFO, "full name len error %d", full_name_len);
      break;
    }
    fread(buff, full_name_len, 1, s_fd);
    object_name.assign(buff, full_name_len);
    fread(&(meta_info.file_info_.create_time_), sizeof(meta_info.file_info_.create_time_), 1, s_fd);//ct
    fread(&(meta_info.file_info_.modify_time_), sizeof(meta_info.file_info_.modify_time_), 1, s_fd);//mt
    fread(&(meta_info.file_info_.size_), sizeof(meta_info.file_info_.size_), 1, s_fd);//size
    fread(&magic_number, sizeof(MAGIC_NUMBER2), 1, s_fd); //magic number
    if (magic_number != MAGIC_NUMBER2)
    {
      TBSYS_LOG(INFO, "magic number error");
      break;
    }
      TBSYS_LOG(INFO, "full_name %*s ct %d, mt %d, sz %lu",
          object_name.length(), object_name.c_str(),
          meta_info.file_info_.create_time_,
          meta_info.file_info_.modify_time_,
          meta_info.file_info_.size_);
    FragMeta frag_it;
    int32_t cluster_id;

    while(1)
    {
      fread(&magic_number, sizeof(MAGIC_NUMBER2), 1, s_fd); //magic number
      if (magic_number == MAGIC_NUMBER4) break;
      if (magic_number != MAGIC_NUMBER3)
      {
        TBSYS_LOG(INFO, "magic number error");
        break;
      }
      fread(&(frag_it.offset_), sizeof(frag_it.offset_), 1, s_fd);
      fread(&(cluster_id), sizeof(cluster_id), 1, s_fd);
      fread(&(frag_it.block_id_), sizeof(frag_it.block_id_), 1, s_fd);
      fread(&(frag_it.file_id_), sizeof(frag_it.file_id_), 1, s_fd);
      fread(&(frag_it.size_), sizeof(frag_it.size_), 1, s_fd);
        TBSYS_LOG(INFO, "off_set %ld cluster_id %d lock_id %d file_id %ld size %d",
            frag_it.offset_,
            cluster_id,
            frag_it.block_id_, frag_it.file_id_, frag_it.size_);
    }


  }
  fclose(s_fd);
  s_fd = NULL;
  return ;
}

int main(int argc, char *argv[])
{
  int i;
  std::string source_file_name;
  std::string s3_server;
  std::string log_file_name;
  while ((i = getopt(argc, argv, "f:d:l:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        source_file_name = optarg;
        break;
      case 'd':
        s3_server = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if (source_file_name.empty() || log_file_name.empty())
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);
  transfer(source_file_name, s3_server);

  return 0;
}


