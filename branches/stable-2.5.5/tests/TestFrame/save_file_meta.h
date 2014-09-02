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
#ifndef SAVE_FILE_META_H_
#define SAVE_FILE_META_H_

#include <iostream>
#include "Thread.h"
#include "Function.h"

using namespace std;

extern pthread_mutex_t mutex;
extern pthread_mutex_t cond_mutex;
extern pthread_cond_t cond;
extern long AllCount;

class SaveFileMetaThread : public Thread
{

  public:
    SaveFileMetaThread(Rest *client, const char *local_file, int count, int num)
     :rest_client_(client), local_file_(local_file), thread_num_(num)
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
        char *meta_file = CreateMetaName(thread_num_, i);
        int ret = (rest_client_->get_client()).save_file(thread_num*10 + i, local_file_, meta_file);
        if (TFS_SUCCESS != ret)
        {
          printf("save file: %s to %s fail\n", local_file_, meta_file);
        }
        //(rest_client_->get_client()).rm_file(uid, meta_file);

        delete meta_file;
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
