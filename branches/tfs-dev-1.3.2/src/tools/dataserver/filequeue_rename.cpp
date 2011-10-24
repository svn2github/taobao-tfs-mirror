/*                                                                                                                                                           
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: recover_disk_data_to_cluster.cpp 553 2011-06-24 08:47:47Z duanfei@taobao.com $
 *
 * Authors:
 *   mingyan <mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include "common/internal.h"
#include "common/parameter.h"
#include "common/directory_op.h"
#include "common/func.h"

using namespace tfs::common;
using namespace std;

int main(int argc,char* argv[])
{
  int ret = -1;
  char* conf_file = NULL;
  int32_t help_info = 0;
  int32_t i;
  string server_index_list;

  while ((i = getopt(argc, argv, "f:i:h")) != EOF)
  {
    switch (i)
    {
      case 'f':
        conf_file = optarg;
        break;
      case 'i':
        server_index_list = optarg;
        break;
      case 'h':
      default:
        help_info = 1;
        break;
    }
  }

  if (NULL == conf_file || 0 == server_index_list.size() || help_info)
  {
    fprintf(stderr, "\nUsage: %s -f conf_file -i index -h\n", argv[0]);
    fprintf(stderr, "  -f configure file\n");
    fprintf(stderr, "  -i server_index  dataserver index number, support 1-8 format\n");
    fprintf(stderr, "  -h help info\n");
    fprintf(stderr, "\n");
    return ret;
  }

  // load config
  int start_index = atoi(server_index_list.c_str());
  int end_index = atoi(server_index_list.c_str());
  if (server_index_list.size() > 1)
  {
    if ('-' == server_index_list.at(1))
    {
      if (server_index_list.size() > 2)
      {
        start_index = atoi(&server_index_list.at(0));
        end_index = atoi(&server_index_list.at(2));
       }
       else
       {
         fprintf(stderr, "server index invalid!\n");
         return ret;
       }
    }
    else
    {
      fprintf(stderr, "server index invalid!\n");
      return ret;
    }
  }

  for (int i = start_index; i <= end_index; i++)
  {
    char tmp[2];
    sprintf(tmp, "%d", i);
    string server_index(tmp);
    ret = SysParam::instance().load_data_server(conf_file, server_index);
    if (TFS_SUCCESS != ret)
    {
      fprintf(stderr, "SysParam::loadDataServerParam failed: %s\n", conf_file);
      return ret;
    }

    //get work directory
    string work_dir = SysParam::instance().dataserver().work_dir_;
    ret = work_dir.empty() ? TFS_ERROR: TFS_SUCCESS;
    if (TFS_SUCCESS != ret)
    {
      fprintf(stderr, "Can not find work dir!\n");
      return ret;
    }
    string mirror_dir = work_dir + string("/mirror");
    if (!DirectoryOp::is_directory(mirror_dir.c_str()))
    {
      ret = TFS_ERROR;
      fprintf(stderr, "Directory %s not exist, error: %s\n", mirror_dir.c_str(), strerror(errno));
      return ret;
    }

    char *slave_ns_ip = SysParam::instance().dataserver().slave_ns_ip_;
    if (slave_ns_ip != NULL && strlen(slave_ns_ip) > 0)
    {
      string old_queue_name = mirror_dir + "/firstqueue";
      string new_queue_name = mirror_dir + "/";
      uint64_t dest_ns_id = Func::get_host_ip(slave_ns_ip);
      char file_queue_name[20];
      sprintf(file_queue_name, "queue_%"PRI64_PREFIX"u", dest_ns_id);
      new_queue_name += file_queue_name;

      if (DirectoryOp::is_directory(old_queue_name.c_str()))
      {
        bool bret = DirectoryOp::rename(old_queue_name.c_str(), new_queue_name.c_str());
        if (false == bret)
        {
          fprintf(stderr, "Rename %s to %s failed!\n", old_queue_name.c_str(), new_queue_name.c_str());
          ret = TFS_ERROR;
        }
        else
        {
          fprintf(stderr, "Rename %s to %s successful!\n", old_queue_name.c_str(), new_queue_name.c_str());
          ret = TFS_SUCCESS;
        }
      }
      else
      {
        fprintf(stderr, "Directory %s not exist!\n", old_queue_name.c_str());
        ret = TFS_ERROR;
      }
    }
    else
    {
      fprintf(stderr, "Slave ns ip invalid!\n");
      ret = TFS_ERROR;
    }
  }
  return ret;
}

