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
#include "KVMeta.h"
#include "PutBucketThread.h"
#include "GetBucketThread.h"
#include "PutObjectThread.h"
#include "GetObjectThread.h"

using namespace std;

pthread_mutex_t mutex;
pthread_cond_t  cond;
pthread_mutex_t cond_mutex;
long AllCount;


void*AllStart(void*)
{
  sleep(3);
  struct timeval time_begin;
  struct timeval time_end;
  cout<<"all start!!!"<<endl;
  long old = 0 ;
  pthread_cond_broadcast(&cond);
  while(true)
  {
    gettimeofday(&time_begin,NULL);
    sleep(1);
    pthread_mutex_lock(&mutex);
    gettimeofday(&time_end,NULL);
    cout<<"All:"<<AllCount<<endl;
    double TPS = (1000*(AllCount-old))/(((time_end.tv_sec-time_begin.tv_sec)*1000000+time_end.tv_usec-time_begin.tv_usec)/    1000);
//  cout<<"cout: "<<1000*(AllCount-old)<<endl;
//  cout<<"time: "<<((time_end.tv_sec-time_begin.tv_sec)*1000000+time_end.tv_usec-time_begin.tv_usec)/1000<<endl;
    cout<<"TPS: "<<TPS<<endl;
    old = AllCount;
//  cout<<"old: "<<old<<endl;
    pthread_mutex_unlock(&mutex);
  }
}

void * run(void*s)
{
 ((PutBucketThread*)(s))->run();
}

int main(int argc, char*argv[] )
{

  int ch;
  char*conf_file;
  Parameter Para;

  if(argc == 1)
  {
    cout<<"Need enter correct conf file: ./PerfMain -p conf_file"<<endl;
    exit(0);
  }
  while((ch = getopt(argc,argv,"p:")) != -1)
  switch(ch)
  {
    case 'p': conf_file = (char*)optarg; break;
    default:  cout<<"Need enter correct conf file: ./PerfMain -p conf_file"<<endl; exit(0);
  }

  TBSYS_CONFIG.load(conf_file);

  Para.rc_addr = TBSYS_CONFIG.getString("public","rc_addr","");
  Para.app_key = TBSYS_CONFIG.getString("public","app_key","");
  Para.kms_addr = TBSYS_CONFIG.getString("public","kms_addr","");
  Para.thread_num = TBSYS_CONFIG.getInt("public","thread_num");
  Para.interior_loop = TBSYS_CONFIG.getInt("public","interior_loop");
  Para.File = TBSYS_CONFIG.getString("public","File");

  KVMeta*client = new KVMeta(Para.kms_addr,Para.rc_addr,Para.app_key);

  pthread_mutex_init(&mutex,NULL);
  pthread_mutex_init(&cond_mutex,NULL);
  pthread_cond_init(&cond,NULL);
  AllCount = 0;

  int i = 0 ;
  pthread_t id;
  char * bucket_name = "PutObjectThread";
  char * rcv_name = "Temp";
  Thread *p[Para.thread_num];
  for(i=0;i<Para.thread_num;i++)
  {
  //  p[i] = new PutBucketThread(client, Para.interior_loop, i);
  //  p[i] = new GetBucketThread(client, Para.interior_loop, i);
  //  p[i] = new PutObjectThread(client, Para.File, bucket_name, Para.interior_loop, i);
      p[i] = new GetObjectThread(client, rcv_name, bucket_name, Para.interior_loop, i);
    pthread_create(&id,NULL,run,(void*)(p[i]));
  }

  pthread_create(&id,NULL,AllStart,NULL);
  pthread_join(id,NULL);

  return 0;
}

