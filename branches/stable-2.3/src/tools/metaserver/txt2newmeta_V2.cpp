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

#include "common/parameter.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "name_meta_server/meta_store_manager.h"
#include "name_meta_server/meta_server_service.h"
#include "new_client/tfs_kv_meta_helper.h"

using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;
using namespace tfs::namemetaserver;

const int MAX_OBJECT_NAME_LEN = 256;

const int MAGIC_NUMBER0 = 0x4f264975;
const int MAGIC_NUMBER1 = 0x5f375a86;
const int MAGIC_NUMBER2 = 0x60486b97;
const int MAGIC_NUMBER3 = 0x71597ca8;
const int MAGIC_NUMBER4 = 0x826a8db9;

int stop = 0;
int64_t server_id = 0;
int64_t time_point = 0;
std::string source_file_name;
int64_t last_appid = -1;
int64_t last_uid = -1;
static tfs::message::MessageFactory gfactory;
static tfs::common::BasePacketStreamer gstreamer;

typedef struct
{
  uint32_t tnum;
  uint32_t ttotal;
} arg_type;

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
  printf("%s -f ource_file -d S3_server -t time_point(2362023593) -l log_file\n", n);
  return 0;
}


void* transfer(void* param)
{
  int magic_number_tmp = 0;
  int ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  s_fd = fopen(source_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", source_file_name.c_str());
    ret =  TFS_ERROR;
  }
  char buff[MAX_OBJECT_NAME_LEN];
  char bucket_name[MAX_OBJECT_NAME_LEN];
  int magic_number;
  string object_name;
  int full_name_len;
  MetaInfo meta_info;
  int read_len = 0;
  int64_t app_id = 0;
  int64_t uid = 0;
  TfsFileInfo tfs_file_info_u;
  ObjectInfo obj_info;
  UserInfo user_info;
  ObjectInfo obj_info_head;
  UserInfo user_info_head;
  BucketMetaInfo bucket_meta_info;

  bool go_on_mode_lock = false;
  if (last_appid == 0 && last_uid == 0)
  {
    go_on_mode_lock = true;
  }

  arg_type* param_t = (arg_type*)(param);
  TBSYS_LOG(INFO, " num = %d total = %d", param_t->tnum, param_t->ttotal);

  uint32_t tnum = param_t->tnum;
  uint32_t total = param_t->ttotal;
  bool doflag_thread = false;


  while(TFS_SUCCESS == ret && 1 != stop)
  {
    obj_info.v_tfs_file_info_.clear();
    obj_info.v_tfs_file_info_.push_back(tfs_file_info_u);
    read_len = fread(&magic_number, sizeof(magic_number), 1, s_fd);
    if (read_len <= 0) break;
    if (MAGIC_NUMBER0 == magic_number)
    {
      fread(&app_id, sizeof(app_id), 1, s_fd);
      fread(&uid, sizeof(uid), 1, s_fd);
      //go on mode open
      if (last_appid == app_id && last_uid == uid)
      {
        go_on_mode_lock = true;
      }

      continue;
    }
    else if (magic_number != MAGIC_NUMBER1)
    {
      TBSYS_LOG(ERROR, "magic number error");
      break;
    }
    fread(&full_name_len, sizeof(full_name_len), 1, s_fd);
    if (full_name_len > MAX_OBJECT_NAME_LEN || full_name_len < 0)
    {
      TBSYS_LOG(ERROR, "full name len error %d", full_name_len);
      break;
    }
    fread(buff, full_name_len, 1, s_fd);
    object_name.assign(buff + 1, full_name_len - 1);
    char *p = NULL;
    char *q = NULL;
    //for hash
    q = buff + 1;
    while(1)
    {
      p = NULL;
      p = strchr(q, '/');
      if (NULL == p)
      {
        break;
      }
      q = p + 1;
    }

    int pre_len = q - buff;
    char * de = q;
    int64_t hashnum;
    doflag_thread = false;
    if (tbsys::CStringUtil::murMurHash((const void*)de, full_name_len - pre_len) % total == tnum)
    {
      hashnum = (tbsys::CStringUtil::murMurHash((const void*)de, full_name_len - pre_len) - 1) % 10243 + 1;
      sprintf(bucket_name, "%ld^%ld", app_id, hashnum);
      user_info.owner_id_ = app_id;
      doflag_thread = true;
    }
    if (doflag_thread && go_on_mode_lock)
    {
      TBSYS_LOG(INFO, "deal appid:%ld uid:%ld OBJ_name:%s thread:%d hashnum:%ld size:%d", app_id, uid, object_name.c_str(), tnum, hashnum, full_name_len - pre_len);
      /*
      ret = tfs::client::KvMetaHelper::do_put_bucket(server_id, bucket_name,
        bucket_meta_info, user_info);
      if (ret == EXIT_BUCKET_EXIST)
      {
        TBSYS_LOG(DEBUG, "[conflict] put bucket conflict |%s|", bucket_name);
        ret = TFS_SUCCESS;
      }
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "put bucket error |%s|", bucket_name);
        ret = TFS_SUCCESS;
      }
      */
    }

    fread(&(meta_info.file_info_.create_time_), sizeof(meta_info.file_info_.create_time_), 1, s_fd);//ct
    fread(&(meta_info.file_info_.modify_time_), sizeof(meta_info.file_info_.modify_time_), 1, s_fd);//mt
    fread(&(meta_info.file_info_.size_), sizeof(meta_info.file_info_.size_), 1, s_fd);//size
    fread(&magic_number, sizeof(MAGIC_NUMBER2), 1, s_fd); //magic number
    if (magic_number != MAGIC_NUMBER2)
    {
      TBSYS_LOG(INFO, "magic number error");
      break;
    }
    //TBSYS_LOG(INFO, "full_name %*s ct %d, mt %d, sz %lu",
    //    object_name.length(), object_name.c_str(),
    //    meta_info.file_info_.create_time_,
    //    meta_info.file_info_.modify_time_,
    //    meta_info.file_info_.size_);

    obj_info.has_meta_info_ = true;
    obj_info.has_customize_info_ = false;
    obj_info.meta_info_.create_time_ = meta_info.file_info_.create_time_;
    obj_info.meta_info_.modify_time_ = meta_info.file_info_.modify_time_;
    obj_info.meta_info_.max_tfs_file_size_ = -5; //TODO this is a tricky in server

    bool need = true;
    if (obj_info.meta_info_.modify_time_ > time_point)
    {
      need = false;
      TBSYS_LOG(INFO, "[time_point] |%s|%s| is no need write to kvmeta", bucket_name, object_name.c_str());
    }
    FragMeta frag_it;
    int32_t cluster_id;
    bool exist = true;
    if (doflag_thread && go_on_mode_lock)
    {
      ret = tfs::client::KvMetaHelper::do_head_object(server_id, bucket_name,
                object_name.c_str(), &obj_info_head, user_info_head);
      if (EXIT_OBJECT_NOT_EXIST == ret)
      {
        exist = false;
      }
      else if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(ERROR, "[conflict] put object conflict |%s|%s|",bucket_name, object_name.c_str());
      }
      else
      {
        TBSYS_LOG(ERROR, "head object error,ret:%d",ret);
      }
    }
    ret = TFS_SUCCESS;
    magic_number_tmp = magic_number;
    while(1)
    {
      fread(&magic_number, sizeof(MAGIC_NUMBER3), 1, s_fd); //magic number
      if (magic_number == MAGIC_NUMBER4 &&  magic_number_tmp == MAGIC_NUMBER2)
      {
        if (doflag_thread && go_on_mode_lock)
        {
          if (need && !exist)
          {
            obj_info.v_tfs_file_info_.clear();
            ret = tfs::client::KvMetaHelper::do_put_object(server_id, bucket_name,
                object_name.c_str(), obj_info, user_info);
          }
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "put object 0file error,ret:%d",ret);
          }
        }
      }
      if (magic_number == MAGIC_NUMBER4) break;
      if (magic_number != MAGIC_NUMBER3)
      {
        TBSYS_LOG(ERROR, "magic number error");
        break;
      }
      magic_number_tmp = MAGIC_NUMBER3;
      fread(&(frag_it.offset_), sizeof(frag_it.offset_), 1, s_fd);
      fread(&(cluster_id), sizeof(cluster_id), 1, s_fd);
      fread(&(frag_it.block_id_), sizeof(frag_it.block_id_), 1, s_fd);
      fread(&(frag_it.file_id_), sizeof(frag_it.file_id_), 1, s_fd);
      fread(&(frag_it.size_), sizeof(frag_it.size_), 1, s_fd);

      TfsFileInfo& tfs_file_info = obj_info.v_tfs_file_info_[0];

      tfs_file_info.block_id_ = frag_it.block_id_;
      tfs_file_info.file_id_ = frag_it.file_id_;
      tfs_file_info.cluster_id_ = cluster_id;
      tfs_file_info.file_size_ = frag_it.size_;
      tfs_file_info.offset_ = frag_it.offset_;

      ret = TFS_SUCCESS;
      if (doflag_thread && go_on_mode_lock)
      {
        if (need && !exist)
        {
          ret = tfs::client::KvMetaHelper::do_put_object(server_id, bucket_name,
              object_name.c_str(), obj_info, user_info);

          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "put object error,ret:%d",ret);
            TBSYS_LOG(INFO, "offset %ld cluster_id %d block_id %d file_id %ld size %d",
                frag_it.offset_,
                cluster_id,
                frag_it.block_id_, frag_it.file_id_, frag_it.size_);
            ret = TFS_SUCCESS;
          }
        }
      }

    }
  }
  fclose(s_fd);
  s_fd = NULL;
  TBSYS_LOG(INFO, "THIS THREAD EXIT,ret:%d",ret);
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
  gstreamer.set_packet_factory(&gfactory);

  tfs::common::NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  int i;
  std::string s3_server;
  std::string log_file_name;
  std::string date_time;
  uint32_t thread_num;
  while ((i = getopt(argc, argv, "f:d:t:l:p:a:u:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        source_file_name = optarg;
        break;
      case 'd':
        s3_server = optarg;
        break;
      case 't':
        date_time = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      case 'p':
        thread_num = atoi(optarg);
        break;
      case 'a':
        last_appid = atol(optarg);
        break;
      case 'u':
        last_uid = atol(optarg);
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
  time_point = tbsys::CTimeUtil::strToTime(const_cast<char*>(date_time.c_str()));
  server_id = Func::get_host_ip(s3_server.c_str());
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);
  TBSYS_LOG(INFO, "date %s num_time %ld",date_time.c_str(), time_point);

  pthread_t* tid = (pthread_t*) malloc(thread_num * sizeof(pthread_t));
  arg_type* p_arg = (arg_type*) malloc(thread_num * sizeof(arg_type));
  for (uint32_t i = 0 ; i < thread_num ; ++i)
  {
     p_arg[i].tnum = i;
     p_arg[i].ttotal = thread_num;
     TBSYS_LOG(INFO,"i is %d tum is %d, ttotal is %d\n",i, p_arg[i].tnum, p_arg[i].ttotal);
  }

  thread_create(tid, p_arg, thread_num);

  thread_wait(tid, thread_num);



  //transfer(source_file_name, s3_server);
  free(p_arg);
  free(tid);
  return 0;
}


