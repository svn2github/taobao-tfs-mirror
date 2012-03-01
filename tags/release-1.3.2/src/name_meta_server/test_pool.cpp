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
*
*/
#include <pthread.h>
#include "mysql_database_helper.h"
#include "database_pool.h"
using namespace tfs::namemetaserver;
using namespace tfs::common;

void* run(void* arg)
{
  DataBasePool* dbp = (DataBasePool*) arg;
  int failed = 0;
  int ok = 0;
  std::vector<MetaInfo> out_v_meta_info;
  int ret;
  for (int i = 0; i < 1000; i++)
  {
    DatabaseHelper* db = dbp->get(1);
    if (NULL != db)
    {
      ok++;
      ret = db->ls_meta_info(out_v_meta_info, 2, 2);
      //TBSYS_LOG(INFO, "ls_meta_info ret = %d size = %lu\n", ret , out_v_meta_info.size());
      dbp->release(db);
    }
    else
    {
      failed++;
    }
    usleep(500);
  }
  TBSYS_LOG(INFO, "get faild %d ok = %d", failed, ok);
  pthread_exit(NULL);
}
int main()
{
  assert (1==mysql_thread_safe());
  enum {POOL_SIZE = 10,};
  DataBasePool tt;
  char coon[50] = {"10.232.35.41:3306:tfs_name_db"};
  char usr[50] = {"root"};
  char pass[50] = {"root"};
  const char* coon_str[POOL_SIZE];
  const char* usr_name[POOL_SIZE];
  const char* pass_word[POOL_SIZE];
  int32_t hash_flag[POOL_SIZE];
  for (int i = 0; i < POOL_SIZE; i++)
  {
    coon_str[i] = coon;
    usr_name[i] = usr;
    pass_word[i]= pass;
    hash_flag[i] = 1;
  }

  assert(tt.init_pool(POOL_SIZE, (char**)coon_str, (char**)usr_name, (char**)pass_word, hash_flag));

  pthread_t tid[30];
  for (int i = 0; i < 20; i++)
  {
    assert(0 == pthread_create(&tid[i], NULL, run, &tt));
  }
  for (int i = 0; i < 20; i++)
  {
    pthread_join(tid[i], NULL);
  }
  TBSYS_LOG(INFO, "all thread end");
  return 0;
}
