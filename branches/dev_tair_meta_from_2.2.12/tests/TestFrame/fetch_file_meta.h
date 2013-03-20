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
#ifndef SAVE_FILE_RAW_H_
#define SAVE_FILE_RAW_H_

#include <iostream>
#include "Thread.h"
#include "Function.h"

using namespace std;

extern pthread_mutex_t mutex;
extern pthread_mutex_t cond_mutex;
extern pthread_cond_t cond;
extern long AllCount;

class FetchFileMetaThread : public Thread
{

  public:
     FetchFileMetaThread(Rest *client, const char *meta_file, int64_t uid, int count, int num)
     :rest_client_(client), meta_file_(meta_file), uid_(uid), thread_num_(num)
    {
      this->interior_loop = count;
    }

    void run (void )
    {
      pthread_mutex_lock(&cond_mutex);
      pthread_cond_wait(&cond, &cond_mutex);
      pthread_mutex_unlock(&cond_mutex);
      sleep(1);

      int64_t app_id = (rest_client_->get_client()).get_app_id();
      for (int i = 0; i < interior_loop; i++)
      {
        const char *local_file = CreateLocalFile(thread_num_, i);
        int ret = (rest_client_->get_client()).fetch_file(app_id, uid_, local_file, meta_file_);
        if (TFS_SUCCESS != ret)
        {
          printf("save file: %s fail\n", local_file);
        }

        unlink(local_file);
        pthread_mutex_lock(&mutex);
        AllCount++;
        pthread_mutex_unlock(&mutex);
      }
    }
  protected:
    Rest *rest_client_;
    const char *meta_file_;
    int64_t uid_;
    int thread_num_;
};
#endif
