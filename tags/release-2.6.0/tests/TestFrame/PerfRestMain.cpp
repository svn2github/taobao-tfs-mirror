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

#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>

#include "tbsys.h"
#include "Parameter.h"
#include "RestClient.h"
#include "thread.h"
//#include "save_file_raw.h"
//#include "save_file_meta.h"
//#include "fetch_file_raw.h"
#include "fetch_file_meta.h"

using namespace std;

pthread_mutex_t mutex;
pthread_cond_t  cond;
pthread_mutex_t cond_mutex;
long AllCount;


void *AllStart(void* )
{
  sleep(3);
  struct timeval time_begin;
  struct timeval time_end;
  cout << "all start!!!" << endl;
  long old = 0 ;
  pthread_cond_broadcast(&cond);
  while (true)
  {
    gettimeofday(&time_begin, NULL);
    sleep(1);
    pthread_mutex_lock(&mutex);
    gettimeofday(&time_end, NULL);
    cout << "All:" << AllCount << endl;
    double TPS = (1000*(AllCount-old))/(((time_end.tv_sec-time_begin.tv_sec)*1000000+time_end.tv_usec-time_begin.tv_usec)/    1000);
//  cout<<"cout: "<<1000*(AllCount-old)<<endl;
//  cout<<"time: "<<((time_end.tv_sec-time_begin.tv_sec)*1000000+time_end.tv_usec-time_begin.tv_usec)/1000<<endl;
    cout << "TPS: " << TPS << endl;
    old = AllCount;
//  cout<<"old: "<<old<<endl;
    pthread_mutex_unlock(&mutex);
  }
}

void *run(void *s)
{
  //((SaveFileRawThread*)(s))->run();
  //((SaveFileMetaThread*)(s))->run();
  //((FetchFileRawThread*)(s))->run();
  ((FetchFileMetaThread*)(s))->run();
}

int main(int argc, char*argv[] )
{

  int ch;
  char *conf_file = NULL;
  Parameter Para;

  if (argc < 3)
  {
    cout << "Need enter correct conf file: ./PerfRestMain -p conf_file" << endl;
    return 1;
  }

  while((ch = getopt(argc, argv, "p:")) != -1)
  {
    switch(ch)
    {
      case 'p':
        conf_file = (char*)optarg;
        break;
      default:
        cout<<"Need enter correct conf file: ./PerfMain -p conf_file"<<endl;
        break;
    }
  }

  TBSYS_CONFIG.load(conf_file);

  Para.rs_addr = TBSYS_CONFIG.getString("public","rs_addr","");
  Para.app_key = TBSYS_CONFIG.getString("public","app_key","");
  Para.thread_num = TBSYS_CONFIG.getInt("public","thread_num");
  Para.interior_loop = TBSYS_CONFIG.getInt("public","interior_loop");
  Para.local_file = TBSYS_CONFIG.getString("public","local_file");

  Rest *rest_client = new Rest(Para.rs_addr, Para.app_key);

  pthread_mutex_init(&mutex,NULL);
  pthread_mutex_init(&cond_mutex,NULL);
  pthread_cond_init(&cond,NULL);
  AllCount = 0;

  int i = 0 ;
  int type = 4;
  pthread_t id;

  Thread *p[Para.thread_num];

  //const char *meta_file = "/aabb";

  char tfs_name[20];

  int64_t ret = -1;
  int64_t uid = 101;

  const char *meta_file = "/bbk/kkk";
  if (Fetch_File_Raw_Type == type)
  {
    ret = rest_client->get_client().save_file(Para.local_file, tfs_name, 20);
  }
  else if (Fetch_File_Meta_Type == type)
  {
    ret = rest_client->get_client().save_file(uid, Para.local_file, meta_file);
  }

  for (i = 0; i < Para.thread_num; i++)
  {
    p[i] = new FetchFileMetaThread(rest_client, meta_file, uid, Para.interior_loop, i);
    //p[i] = new FetchFileRawThread(rest_client, tfs_name, Para.interior_loop, i);
    //p[i] = new SaveFileMetaThread(rest_client, Para.local_file, Para.interior_loop, i);
    //p[i] = new SaveFileRawThread(rest_client, Para.local_file, Para.interior_loop, i);
    //p[i] = new GetObjectThread(client, rcv_name, bucket_name, Para.interior_loop, i);
    pthread_create(&id, NULL, run, (void*)(p[i]));
  }

  pthread_create(&id, NULL, AllStart, NULL);
  pthread_join(id, NULL);

  return 0;
}

