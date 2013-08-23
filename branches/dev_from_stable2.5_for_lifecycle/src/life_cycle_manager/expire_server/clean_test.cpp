/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#include <stdio.h>
#include "tfs_client_api.h"
#include "new_client/tfs_rc_client_api.h"
#include "tbsys.h"

static const char *app_key = "tfscom";
static const char *nginx_rs_addr = "10.232.38.18:3800";
static const char *rc_addr = "10.232.36.201:5755";
static const char *kv_rs_addr = "10.232.35.41:4567";

using namespace tfs::common;
using namespace tfs::client;

typedef struct
{
  RestClient *rest_client_;
  RcClient *rc_client_;
  char ret_tfs_name_[128];
  int32_t avg_;
  int32_t expire_time_;
}ThreadParam;

void *func(void *arg)
{
  ThreadParam *thread_param = (ThreadParam *)arg;

  RestClient *rest_client = thread_param->rest_client_;
  RcClient *rc_client = thread_param->rc_client_;
  char *ret_tfs_name = thread_param->ret_tfs_name_;
  int expire_time = thread_param->expire_time_;

  int32_t avg = thread_param->avg_;
  char app_key[20] = "tfscom";

  TBSYS_LOG(INFO, "task time: %d", expire_time);


  for (int i = 0; i < avg; i++)
  {
    int ret = rest_client->save_file("aa", ret_tfs_name, 20);
    if (ret > 0)
    {
      ret = rc_client->set_life_cycle(1, ret_tfs_name, expire_time, app_key);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "set tfs_name: %s life cycle error", ret_tfs_name);
      }
    }
  }

  return (void*)0;
}

void usage(const char * s)
{
  printf("%s batch_num thread_num expire_time(20130810120000)\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  if (argc < 4)
  {
    usage(argv[0]);
  }

  int32_t batch_num = atoi(argv[1]);
  int32_t thread_num = atoi(argv[2]);
  int32_t spec_time = tbsys::CTimeUtil::strToTime(argv[3]);

  pthread_t *threads = new pthread_t[thread_num];
  ThreadParam* thread_params = new ThreadParam[thread_num];

  for (int i = 0; i < thread_num; i++)
  {
    RestClient *rest_client = new RestClient();
    RcClient *rc_client = new RcClient();
    int ret = rest_client->initialize(nginx_rs_addr, app_key);
    rest_client->set_log_level("info");

    if (TFS_SUCCESS == ret)
    {
      rc_client->set_kv_rs_addr(kv_rs_addr);
      ret = rc_client->initialize(rc_addr, app_key);
    }

    if (TFS_SUCCESS == ret)
    {
      thread_params[i].rest_client_ = rest_client;
      thread_params[i].rc_client_ = rc_client;
      thread_params[i].avg_ = batch_num / thread_num;
      thread_params[i].expire_time_ = spec_time;
    }
  }

  for (int i = 0; i < thread_num; i++)
  {
    pthread_create(&threads[i], NULL, func, &thread_params[i]);
  }

  for (int i = 0; i < thread_num; i++)
  {
    pthread_join(threads[i], NULL);
  }

  for (int i = 0; i < thread_num; i++)
  {
    delete thread_params[i].rest_client_;
    delete thread_params[i].rc_client_;
  }

  delete [] thread_params;
  delete [] threads;

  return 0;
}
