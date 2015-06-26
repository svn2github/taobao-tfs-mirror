/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */

#include <set>
#include <stdio.h>
#include <error.h>
#include <string>
#include <vector>
#include "common/internal.h"
#include "common/error_msg.h"

using namespace std;
using namespace tfs::common;

void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -i input_item_path -s input_server_path -p output_prefix -o output_dir\n", name);
  fprintf(stderr, "       -i input item path\n");
  fprintf(stderr, "       -s input server path\n");
  fprintf(stderr, "       -p output prefix\n");
  fprintf(stderr, "       -o output dir\n");
  fprintf(stderr, "       -h help\n");
  exit(-1);
}

int main(int argc, char* argv[])
{
  int32_t i= 0;
  char* input_items_path = NULL;
  char* input_server_path= NULL;
  char* output_prefix    = NULL;
  char* output_dir_path  = NULL;
  FILE* input_item = NULL;
  FILE* input_server = NULL;
  std::vector<FILE*> output_files;
  std::set<std::string> input_server_paths;
  while ((i = getopt(argc, argv, "i:s:p:o:h")) != EOF)
  {
    switch (i)
    {
      case 'i':
        input_items_path = optarg;
        break;
      case 's':
        input_server_path= optarg;
        break;
      case 'p':
        output_dir_path = optarg;
        break;
      case 'o':
        output_prefix = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
        break;
     }
  }
  int32_t ret = (NULL != input_server_path && NULL != input_items_path
                && NULL != output_prefix && NULL != output_dir_path) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
  if (TFS_SUCCESS != ret)
  {
    usage(argv[0]);
  }

  if (TFS_SUCCESS == ret)
  {
    input_server = fopen(input_server_path, "r");
    ret = (NULL != input_server) ? TFS_SUCCESS : EXIT_OPEN_FILE_ERROR;
    if (TFS_SUCCESS != ret)
      fprintf(stderr, "[ERROR] open %s failed, error: %s", input_server_path, strerror(errno));
  }

  if (TFS_SUCCESS == ret)
  {
    input_item = fopen(input_items_path, "r");
    ret = (NULL != input_item) ? TFS_SUCCESS : EXIT_OPEN_FILE_ERROR;
    if (TFS_SUCCESS != ret)
      fprintf(stderr, "[ERROR] open %s failed, error: %s", input_items_path, strerror(errno));
  }
  const int32_t BUF_LEN = 128;
  char buf[BUF_LEN] = {'\0'};
  if (TFS_SUCCESS == ret)
  {
    while (EOF != fscanf(input_server, "%s", buf) && TFS_SUCCESS == ret)
    {
      char path[512] = {'\0'};
      snprintf(path, 512, "%s/%s%s", output_dir_path, output_prefix, buf);
      std::pair<std::set<std::string>::iterator, bool > res = input_server_paths.insert(path);
      if (res.second)
      {
        FILE* file = fopen(path, "w+");
        ret = (NULL != file) ? TFS_SUCCESS : EXIT_OPEN_FILE_ERROR;
        if (TFS_SUCCESS == ret)
        {
          output_files.push_back(file);
        }
        else
        {
          fprintf(stderr, "[ERROR] open %s failed, error: %s", path, strerror(errno));
        }
      }
    }
  }

  if (TFS_SUCCESS == ret)
  {
    int32_t index = 0;
    const int32_t TOTAL = output_files.size();
    while (NULL != fgets(buf, BUF_LEN, input_item))
    {
      fprintf(output_files[index], "%s", buf);
      if (++index >= TOTAL)
        index = 0;
    }
  }

  fclose(input_item);
  fclose(input_server);
  std::vector<FILE*>::const_iterator iter = output_files.begin();
  for (; iter != output_files.end(); ++iter)
  {
    fclose((*iter));
  }
  return ret;
}
