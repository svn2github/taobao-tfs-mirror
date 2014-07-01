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
#ifndef PUT_BUCKET_THREAD_H_
#define PUT_BUCKET_THREAD_H_

#include <iostream>
#include "Thread.h"
#include "Function.h"
#include "kv_meta_define.h"

using namespace std;

extern pthread_mutex_t mutex;
extern pthread_mutex_t cond_mutex;
extern pthread_cond_t cond;
extern long AllCount;

class PutBucketThread : public Thread
{

  public:
    PutBucketThread(KVMeta*client,int count,int thread_num)
    {
      this->client = client;
      this->interior_loop = count;
      this->thread_num = thread_num;
    }

    void run (void )
    {
      pthread_mutex_lock(&cond_mutex);
      pthread_cond_wait(&cond,&cond_mutex);
      pthread_mutex_unlock(&cond_mutex);
      sleep(1);
      for(int i =0;i<interior_loop;i++)
      {
        char*Name;
        tfs::common::UserInfo user_info;
        user_info.owner_id_ = 1;
        Name=CreateName(thread_num,i);
        int Ret = client->client.put_bucket(Name,user_info);
        if(Ret!=0)
        {
          cout<<"Put "<<Name<<" fail !"<<endl;
          cout<<"Error: "<<Ret<<endl;
        }
        // cout<<Name<<endl;
        delete Name;
        Name = NULL;
        pthread_mutex_lock(&mutex);
        AllCount++;
        pthread_mutex_unlock(&mutex);
      }
    }
  protected:
    KVMeta*client;
    int thread_num;
};
#endif
