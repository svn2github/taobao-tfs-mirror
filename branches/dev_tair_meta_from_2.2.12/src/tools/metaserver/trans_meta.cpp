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

using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;

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
  printf("%s -f meta_config_file -s source_file -o dest_file -l log_file\n", n);
  return 0;
}
int transfer(const int64_t app_id, const int64_t uid, FILE *d_fd)
{
  UNUSED(app_id);
  UNUSED(uid);
  UNUSED(d_fd);
  return 0;
}
void transfer(const string &source_file_name, const string &out_file_name)
{
  int ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  FILE *d_fd = NULL;
  s_fd = fopen(source_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", source_file_name.c_str());
    ret =  TFS_ERROR;
  }
  d_fd = fopen(out_file_name.c_str(), "a+");
  if (NULL == s_fd)
  {
    printf(" open file %s for append\n", out_file_name.c_str());
    ret =  TFS_ERROR;
  }
  char buff[128];
  while(TFS_SUCCESS == ret && 1 != stop)
  {
    if (NULL == fgets(buff, 128, s_fd))
    {
      break;
    }
    buff[127] = 0;
    char *p = NULL;
    const char DLIMER = ',';
    p = strstr(buff, &DLIMER);
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    int64_t app_id = -1;
    int64_t uid = -1;
    *p = '\0';
    app_id = strtoll(buff, NULL, 10);
    uid = strtoll(p + 1, NULL, 10);
    if (app_id <= 0 || uid <= 0)
    {
      *p = DLIMER;
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    TBSYS_LOG(INFO, "transfer app_id %ld uid %ld", app_id, uid);
    ret = transfer(app_id, uid, d_fd);
  }
  fclose(s_fd);
  s_fd = NULL;
  fclose(d_fd);
  d_fd = NULL;
  return ;
}

int main(int argc, char *argv[])
{
  int i;
  std::string config_file_name;
  std::string source_file_name;
  std::string out_file_name;
  std::string log_file_name;
  while ((i = getopt(argc, argv, "f:s:o:l:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        config_file_name = optarg;
        break;
      case 's':
        source_file_name = optarg;
        break;
      case 'o':
        out_file_name = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if (config_file_name.empty() || source_file_name.empty() ||
      out_file_name.empty() || log_file_name.empty())
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  if (TBSYS_CONFIG.load(config_file_name.c_str()) == TFS_ERROR)
  {
    TBSYS_LOG(ERROR, "load conf(%s) failed", config_file_name.c_str());
    return TFS_ERROR;
  }
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);
  transfer(source_file_name, out_file_name);

  return 0;
}


