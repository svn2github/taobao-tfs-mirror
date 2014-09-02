/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: showssm.cpp 199 2011-04-12 08:49:55Z duanfei@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include <stdio.h>
#include <vector>
#include <string>
#include <string.h>
#include <signal.h>
#include <Memory.hpp>
#include <pthread.h>
#include <sys/time.h>

#include "common/parameter.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/mysql_cluster/database_pool.h"
#include "common/mysql_cluster/mysql_engine_helper.h"


using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;
int stop;
int duplicate_count = 0;
typedef struct
{
  uint32_t tnum;
  uint32_t ttotal;
} arg_type;
string source_file_name;
DataBasePool data_base_pool_;
static uint32_t decode_fixed32(const char* buf)
{
  return ((static_cast<uint32_t>(static_cast<unsigned char>(buf[0])))
      | (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 8)
      | (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 16)
      | (static_cast<uint32_t>(static_cast<unsigned char>(buf[3])) << 24));
}


void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
      stop = 1;
      break;
  }
}
int usage (const char* n)
{
  printf("%s -f dump_file -t thread_count -c connstr -u user -p passwd  -l log_file -d duplicate_count\n", n);
  return 0;
}


void* transfer(void* param)
{
  int ret = TFS_SUCCESS;
  MysqlDatabaseHelper* data_base = data_base_pool_.get();
  FILE *s_fd = NULL;
  s_fd = fopen(source_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", source_file_name.c_str());
    ret =  TFS_ERROR;
  }
  const int KEY_BUFF = 700;
  const int VALUE_BUFF = 1024*1024;
  char key_buff[KEY_BUFF];
  char value_buff[VALUE_BUFF];
  char size_buff[10];


  arg_type* param_t = (arg_type*)(param);
  TBSYS_LOG(INFO, " num = %d total = %d", param_t->tnum, param_t->ttotal);

  uint32_t tnum = param_t->tnum;
  uint32_t total = param_t->ttotal;
  int64_t count = 0;
  int64_t count_thread = 0;
  int area =10;
  KvKey key;
  KvMemValue value;



  while(1 != stop)
  {
    int key_size1 = 0;
    int key_size2 = 0;
    int value_size = 0;
    size_t read_len = 0;
    int ret = 0;
    read_len = fread(size_buff, sizeof(int), 1, s_fd);
    key_size1 = decode_fixed32(size_buff);
    if (read_len <= 0 || key_size1 > KEY_BUFF ) break;

    read_len = fread(key_buff, key_size1, 1, s_fd);
    assert(read_len == 1);

    read_len = fread(size_buff, sizeof(int), 1, s_fd);
    key_size2 = decode_fixed32(size_buff);
    assert(read_len == 1);
    if (read_len <= 0 || key_size1 + key_size2 > KEY_BUFF ) break;


    if (key_size2 > 0)
    {
      read_len = fread(key_buff + key_size1, key_size2, 1, s_fd);
      assert(read_len == 1);
    }

    read_len = fread(size_buff, sizeof(int), 1, s_fd);
    value_size = decode_fixed32(size_buff);
    assert(read_len == 1);
    if (read_len <= 0 || value_size > VALUE_BUFF ) break;

    read_len = fread(value_buff, value_size, 1, s_fd);
    assert(read_len == 1);
    if (read_len <= 0) break;
    if (count%total == tnum)
    {
      key.key_= key_buff;
      key.key_size_ = key_size1 + key_size2;

      KvKey e_key;
      char e_s[5];
      vector<KvValue*> keys;
      vector<KvValue*> values;
      memcpy(e_s, key_buff, 5);
      e_s[4]=127;
      e_key.key_ = e_s;
      e_key.key_size_ = 5;

      int32_t rest_count;
      ret = data_base->scan_v(area, e_key, key, -duplicate_count, false, &keys, &values, &rest_count);
      if (TFS_SUCCESS != ret )
      {
        TBSYS_LOG(ERROR, "ret =%d ", ret);
      }
      else
      {
        for(int i = 0; i < (int)keys.size(); i++)
        {
          keys[i]->free();
          values[i]->free();
        }
      }
      count_thread++;
      if (0 == count_thread%1000)
        TBSYS_LOG(INFO, "have dealed %ld record", count_thread);
      //generate duplicate data for test

      /*if (duplicate_count)
      {
        *(key_buff + key_size1 -1 ) = 't';
        key.key_size_ = key_size1 + key_size2 + 1;
        for (int i = 0; i < duplicate_count && 1!=stop; i++)
        {
          *(key_buff + key_size1 + key_size2) = i+'1';
          data_base->insert_kv(area, key, value);
        }
      }*/
    }
    count++;
  }
  TBSYS_LOG(INFO, "deal %ld of %ld record", count_thread, count);
  fclose(s_fd);
  s_fd = NULL;
  data_base_pool_.release(data_base);
  pthread_exit(NULL);
}
void thread_create(pthread_t* thread, arg_type* p_arg, int32_t thread_num)
{
  int temp;
  int i;
  memset(thread, 0, sizeof(thread));
  for (i = 0 ; i < thread_num ; ++i)
  {

    if((temp = pthread_create(thread + i, NULL, transfer, (void*)(p_arg + i))) != 0)
    {
      TBSYS_LOG(ERROR,"create thread fails");
    }
    else
    {
      TBSYS_LOG(INFO,"create thread success");
    }
  }
}

void thread_wait(pthread_t* thread, int32_t thread_num)
{
  int i;
  for (i = 0 ; i < thread_num ; ++i)
  {
    if(thread[i] != 0)
    {
      //TBSYS_LOG(INFO,"thread %d is start tid is %ld\n", i, thread[i]);
      pthread_join(thread[i], NULL);
      //TBSYS_LOG(INFO,"thread %d is end   tid is %ld\n", i, thread[i]);
    }
    else
    {
      TBSYS_LOG(ERROR,"thread tid is zero is %d", i);
    }
  }
}


int main(int argc, char *argv[])
{

  int i;
  std::string conn_;
  std::string user_name_;
  std::string pass_wd_;
  std::string log_file_name;
  uint32_t thread_num = 3;
  duplicate_count = 0;
  while ((i = getopt(argc, argv, "f:c:u:p:l:t:d:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        source_file_name = optarg;
        break;
      case 'c':
        conn_= optarg;
        break;
      case 'u':
        user_name_ = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      case 't':
        thread_num = atoi(optarg);
        break;
      case 'p':
        pass_wd_ = optarg;
        break;
      case 'd':
        duplicate_count = atoi(optarg);

        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if (source_file_name.empty() || log_file_name.empty())
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);

  pthread_t* tid = (pthread_t*) malloc(thread_num * sizeof(pthread_t));
  arg_type* p_arg = (arg_type*) malloc(thread_num * sizeof(arg_type));
  for (uint32_t i = 0 ; i < thread_num ; ++i)
  {
    p_arg[i].tnum = i;
    p_arg[i].ttotal = thread_num;
    TBSYS_LOG(INFO,"i is %d tum is %d, ttotal is %d\n",i, p_arg[i].tnum, p_arg[i].ttotal);
  }
  data_base_pool_.init_pool(thread_num, conn_.c_str(), user_name_.c_str(), pass_wd_.c_str());

  thread_create(tid, p_arg, thread_num);

  thread_wait(tid, thread_num);



  //transfer(source_file_name, s3_server);
  free(p_arg);
  free(tid);
  return 0;
}


