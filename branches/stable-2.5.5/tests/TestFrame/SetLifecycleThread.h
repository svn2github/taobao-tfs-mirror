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
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/
#ifndef SET_LIFECYCLE_THREAD_H_
#define SET_LIFECYCLE_THREAD_H_

#include <iostream>
#include "Thread.h"
#include "Function.h"
#include "expire_define.h"
#include "fsname.h"

using namespace std;
using namespace tfs::common;

extern pthread_mutex_t mutex;
extern pthread_mutex_t cond_mutex;
extern pthread_cond_t cond;
extern long AllCount;

class SetLifecycleThread : public Thread
{

  public:
    SetLifecycleThread(KVMeta *client, int count, int thread_num, const char *local_file)
      :client_(client), interior_loop_(count), thread_num_(thread_num), local_file_(local_file)
    {}

    void run (void )
    {
      pthread_mutex_lock(&cond_mutex);
      pthread_cond_wait(&cond,&cond_mutex);
      pthread_mutex_unlock(&cond_mutex);
      sleep(1);

      tbutil::Time now = tbutil::Time::now(tbutil::Time::Realtime);
      int32_t invalid_time_s = now.toSeconds() + 3600;
      int32_t file_type = ExpireDefine::FILE_TYPE_RAW_TFS;
      const char *app_key = "tfscom";

      int ret = TFS_SUCCESS;
      //random blockid & fileid

      for (int j = 0; j < interior_loop_; j++)
      {
        //uint32_t block_id = thread_num_ * 5;
        //int64_t file_id = thread_num_ * 100 + j;
        //FSName file_name(block_id, file_id, 0);

        char *Name;
        Name = CreateMetaName(thread_num_ * 3353, j);

        //printf("%s\n", file_name.get_name());

        ret = client_->rc_client_.set_life_cycle(file_type, Name, invalid_time_s, app_key);

        delete Name;
        Name = NULL;

        pthread_mutex_lock(&mutex);
        AllCount++;
        pthread_mutex_unlock(&mutex);
      }
    }
  protected:
    KVMeta *client_;
    int interior_loop_;
    int thread_num_;
    const char *local_file_;
};
#endif
