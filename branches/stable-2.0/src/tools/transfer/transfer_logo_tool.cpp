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
*   duanfei <duanfei@taobao.com>
*      -modify-2011/12/29
*   linqing <linqing.zyd@taobao.com>
*      -modify-2012/02/24
*
*/
#include <tbsys.h>
#include <unistd.h>
#include "new_client/tfs_client_api.h"
#include "new_client/tfs_meta_client_api.h"
#include "new_client/tfs_rc_client_api.h"

#define buff_size  5 * 1024L * 1024L

using namespace tfs::common;
tfs::client::RcClient rc_client;
static char source_ns_addr[32];
static int64_t app_id = 0;
static int64_t hash_count = 0;
//iput_file
//source_file;/dest_file

static int64_t get_source(const char* source_name, char* buff)
{
  int64_t source_count  = -1;
  if (*source_name == '/')
  {
    //get data from local file
    int fd = ::open(source_name, O_RDONLY);
    if (fd < 0)
    {
      TBSYS_LOG(ERROR, " open file %s error", source_name);
    }
    else
    {
      source_count = ::read(fd, buff, buff_size);
      if (source_count < 0 || source_count >= buff_size)
      {
        TBSYS_LOG(ERROR, "read file %s error %ld", source_name, source_count);
        source_count = -1;
      }
      ::close(fd);
    }
  }
  else if(*source_name == 'T' || *source_name == 'L')
  {
    //get data from tfs
    int fd = tfs::client::TfsClient::Instance()->open(source_name, NULL, source_ns_addr, T_READ);
    if (fd < 0)
    {
      TBSYS_LOG(ERROR, " open file %s error", source_name);
    }
    else
    {
      source_count = tfs::client::TfsClient::Instance()->read(fd, buff, buff_size);
      if (source_count < 0 || source_count >= buff_size)
      {
        TBSYS_LOG(ERROR, "read file %s error %ld", source_name, source_count);
        source_count = -1;
      }
      tfs::client::TfsClient::Instance()->close(fd);
    }
  }
  else
  {
   TBSYS_LOG(ERROR, "know_source %s", source_name);
   source_count = -1;
  }
  return source_count;
}

static int64_t write_dest(const char* dest_name, char* buff, const int64_t size)
{
  int64_t write_count = -1;
  uint32_t  hash_value = tbsys::CStringUtil::murMurHash((const void*)(dest_name), strlen(dest_name));
  int32_t   uid = (hash_value % hash_count + 1);
  write_count = rc_client.save_buf(app_id, uid, buff, size, dest_name);
  if (write_count > 0 || write_count == EXIT_TARGET_EXIST_ERROR) // exist or success
  {
    printf("save file %s ok\n", dest_name);
  }
  else
  {
    printf("save file %s error\n", dest_name);
  }
  return write_count;
}

int main(int argc ,char* argv[])
{
  if (argc != 6)
  {
    printf("usage %s input_text source_ns_addr rcaddr app_key user_count\n", argv[0]);
    return 0;
  }

  TBSYS_LOGGER.setLogLevel("debug");
  tfs::client::TfsClient::Instance()->initialize();
  int ret = rc_client.initialize(argv[3], argv[4], "10.246.123.3");
  if (ret != TFS_SUCCESS)
  {
    printf("rc_client initialize error %s\n", argv[3]);
    return 0;
  }

  FILE* fd = ::fopen(argv[1], "r");
  if (fd < 0)
  {
    printf("open local file %s error\n", argv[1]);
    return 0;
  }
  snprintf(source_ns_addr, 32, "%s", argv[2]);
  app_id = rc_client.get_app_id();
  hash_count = atoll(argv[5]);
  char* buff = (char*)::malloc(buff_size);
  char line_buff[4096];
  char* p_source = NULL;
  char* p_dest = NULL;
  while(fgets(line_buff, 4096, fd)!= NULL)
  {
    // TBSYS_LOG(WARN, "deal %s", line_buff);
    p_source = line_buff;
    p_dest = strstr(line_buff, ":");
    if (NULL != p_dest)
    {
      *(p_dest++) = '\0';
    }
    else
    {
      TBSYS_LOG(ERROR, "deal error %s", line_buff);
      continue;
    }
    size_t count = strlen(p_dest);
    char *end = p_dest + count - 1;
    while (*end == '\n' || *end == '\r' || *end ==' ')
    {
      *end = 0;
      end--;
    }

    int64_t source_count = get_source(p_source, buff);
    if (source_count >= 0)
    {
      write_dest(p_dest, buff, source_count);
    }
  }
  ::fclose(fd);
  free (buff);
  return 0;
}
