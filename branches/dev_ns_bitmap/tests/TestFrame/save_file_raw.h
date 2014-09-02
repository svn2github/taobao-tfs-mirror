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

class SaveFileRawThread : public Thread
{

  public:
    SaveFileRawThread(Rest *client, const char *local_file, int count, int num)
     :rest_client_(client), local_file_(local_file), thread_num_(num)
    {
      this->interior_loop = count;
    }

    virtual ~SaveFileRawThread()
    {
    }

    void run (void )
    {
      pthread_mutex_lock(&cond_mutex);
      pthread_cond_wait(&cond, &cond_mutex);
      pthread_mutex_unlock(&cond_mutex);
      sleep(1);

      char ret_tfs_name[20];
      for (int i = 0; i < interior_loop; i++)
      {
        int64_t ret = (rest_client_->get_client()).save_file(local_file_, ret_tfs_name, 19);
        if (ret < 0)
        {
          printf("save file: %s fail\n", local_file_);
        }

        pthread_mutex_lock(&mutex);
        AllCount++;
        pthread_mutex_unlock(&mutex);
      }
    }
  protected:
    Rest *rest_client_;
    const char *local_file_;
    int thread_num_;
};
#endif
