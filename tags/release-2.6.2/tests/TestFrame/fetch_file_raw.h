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

#ifndef FETCH_FILE_RAW_H_
#define FETCH_FILE_RAW_H_

#include <iostream>
#include "Thread.h"
#include "Function.h"

using namespace std;

extern pthread_mutex_t mutex;
extern pthread_mutex_t cond_mutex;
extern pthread_cond_t cond;
extern long AllCount;

class FetchFileRawThread : public Thread
{

  public:
     FetchFileRawThread(Rest *client, const char *tfs_name, int count, int num)
     :rest_client_(client), tfs_name_(tfs_name), thread_num_(num)
    {
      this->interior_loop = count;
    }

    void run (void )
    {
      pthread_mutex_lock(&cond_mutex);
      pthread_cond_wait(&cond, &cond_mutex);
      pthread_mutex_unlock(&cond_mutex);
      sleep(1);

      for (int i = 0; i < interior_loop; i++)
      {
        char *local_file = CreateLocalFile(thread_num_, i);
        int ret = (rest_client_->get_client()).fetch_file(local_file, tfs_name_);
        if (TFS_SUCCESS != ret)
        {
          printf("fetch file: %s fail\n", local_file);
        }

        unlink(local_file);

        pthread_mutex_lock(&mutex);
        AllCount++;
        pthread_mutex_unlock(&mutex);
      }
    }
  protected:
    Rest *rest_client_;
    const char *tfs_name_;
    int thread_num_;
};
#endif
