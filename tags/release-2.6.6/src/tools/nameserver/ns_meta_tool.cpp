/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 */
#include "tbsys.h"
#include "common/serialization.h"
#include "common/error_msg.h"

using namespace std;
using namespace tfs::common;

int write_ns_meta(const int fd, const uint64_t id)
{
  int ret = TFS_SUCCESS;
  assert(fd != -1);
  char data[INT64_SIZE];
  int64_t pos = 0;
  ret = Serialization::set_int64(data, INT64_SIZE, pos, id);
  if (TFS_SUCCESS == ret)
  {
    int32_t offset = 0;
    int32_t length = 0;
    int32_t count  = 0;
    ::lseek(fd, 0, SEEK_SET);
    do
    {
      ++count;
      length = ::write(fd, (data + offset), (INT64_SIZE - offset));
      if (length > 0)
      {
        offset += length;
      }
    }
    while (count < 3 && offset < INT64_SIZE);
    ret = INT64_SIZE == offset ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
    if (TFS_SUCCESS == ret)
      fsync(fd);
  }
  return ret;
}

int read_ns_meta(const int fd, uint64_t &base_global_id)
{
  assert(fd != -1);
  int ret = TFS_SUCCESS;
  char data[INT64_SIZE];
  int32_t length = ::read(fd, data, INT64_SIZE);
  if (length == INT64_SIZE)//read successful
  {
    int64_t pos = 0;
    ret = Serialization::get_int64(data, INT64_SIZE, pos, reinterpret_cast<int64_t*>(&base_global_id));
  }
  else
  {
    ret = EXIT_GENERAL_ERROR;
  }
  return ret;
}


int main(int argc, char** argv)
{
  if (argc != 3 && argc != 4)
  {
    fprintf(stderr, "Usgae: %s  meta_file <get | set base_global_blockid>\n", argv[0]);
    return -1;
  }

  int fd = ::open(argv[1], O_RDWR | O_CREAT, 0600);
  if (fd < 0)
  {
    fprintf(stderr, "open file %s failed, errors: %s\n", argv[1], strerror(errno));
    return -1;
  }

  if (3 == argc && 0 == strcmp(argv[2], "get"))
  {
    uint64_t read_gloabal_blockid = 0;
    if (TFS_SUCCESS != read_ns_meta(fd, read_gloabal_blockid))
    {
      fprintf(stderr, "read fail from meta file: %s\n", argv[1]);
    }
    else
    {
      printf("global block id: %"PRI64_PREFIX"u\n", read_gloabal_blockid);
    }
  }
  else if (4 == argc && 0 == strcmp(argv[2], "set"))
  {
    uint64_t write_global_blockid = strtoull(argv[3], NULL, 10);
    if (TFS_SUCCESS == write_ns_meta(fd, write_global_blockid))
    {
      printf("set meta file: %s to %"PRI64_PREFIX"u success\n", argv[1], write_global_blockid);
    }
    else
    {
      fprintf(stderr, "set fail to meta file: %s\n", argv[1]);
    }
  }
  else
  {
    fprintf(stderr, "Usgae: %s  meta_file <get | set base_global_blockid>\n", argv[0]);
  }

  close(fd);
  return 0;
}
