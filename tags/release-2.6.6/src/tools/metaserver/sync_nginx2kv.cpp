/*
* (C) 2007-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   qixiao <qixiao.zs@alibaba-inc.com>
*      - initial release
*
*/
#include <stdio.h>
#include <vector>
#include <string>
#include <string.h>
#include <signal.h>
#include <Memory.hpp>

#include "common/version.h"
#include "common/parameter.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/new_client.h"
#include "common/meta_server_define.h"
#include "message/message_factory.h"
#include "new_client/tfs_meta_client_api_impl.h"
#include "new_client/tfs_kv_meta_helper.h"


using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;
using namespace tfs::client;

int64_t old_server_id = 0;
int64_t new_server_id = 0;

static tfs::message::MessageFactory gfactory;
static tfs::common::BasePacketStreamer gstreamer;
enum CheckResultType
{
  ERROR_MES = 0,
  YES_NO = 1,
  YES_YES = 2,
  NO_YES = 3,
  NO_NO = 4
};

int usage(const char* name)
{
  fprintf(stderr, "%s\n", Version::get_build_description());
  printf("-i: means input_file \n");
  printf("-o: means old_root_addr \n");
  printf("-n: means new_kv_meta_addr \n");
  printf("-l: means output_file \n");
  printf("Usage: %s -i input_sync.txt -o old_root_ipport -n new_kvmeta_ipport -l ./log_file \n", name);
  return 0;
}


int stop = 0;
void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
      stop = 1;
      break;
  }
}

int write_to_kv(NameMetaClientImpl &client, int64_t app_id, int64_t uid, const string& bucket_name, const string& object_name)
{
  int ret = TFS_SUCCESS;
  FragInfo frag_info;
  ret = client.read_frag_info(app_id, uid, object_name.c_str(), frag_info);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "read_frag_info,ret:%d",ret);
  }
  FileMetaInfo file_meta_info;
  ret = client.ls_file(app_id, uid, object_name.c_str(), file_meta_info);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ls_file,ret:%d",ret);
  }

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = app_id;
  ret = tfs::client::KvMetaHelper::do_put_bucket(new_server_id, bucket_name.c_str(),
          bucket_meta_info, user_info);

  ObjectInfo obj_info;
  obj_info.has_meta_info_ = true;
  obj_info.has_customize_info_ = false;
  obj_info.meta_info_.create_time_ = file_meta_info.create_time_;
  obj_info.meta_info_.modify_time_ = file_meta_info.modify_time_;
  obj_info.meta_info_.max_tfs_file_size_ = -5; //TODO this is a tricky in server

  if (frag_info.v_frag_meta_.size() == 0)
  {
    ret = tfs::client::KvMetaHelper::do_put_object(new_server_id, bucket_name.c_str(),
    object_name.c_str() + 1, obj_info, user_info);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "[SYNC_LOG] put object error,ret:%d",ret);
    }
    else
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] write to kv put object success,offst:0 size:0");
    }
  }
  TfsFileInfo tfs_file_info_u;
  obj_info.v_tfs_file_info_.clear();
  obj_info.v_tfs_file_info_.push_back(tfs_file_info_u);

  for(size_t i = 0; i < frag_info.v_frag_meta_.size(); ++i)
  {
    TfsFileInfo& tfs_file_info = obj_info.v_tfs_file_info_[0];
    tfs_file_info.block_id_ = frag_info.v_frag_meta_[i].block_id_;
    tfs_file_info.file_id_ = frag_info.v_frag_meta_[i].file_id_;
    tfs_file_info.cluster_id_ = frag_info.cluster_id_;
    tfs_file_info.file_size_ = frag_info.v_frag_meta_[i].size_;
    tfs_file_info.offset_ = frag_info.v_frag_meta_[i].offset_;

    ret = tfs::client::KvMetaHelper::do_put_object(new_server_id, bucket_name.c_str(),
            object_name.c_str() + 1, obj_info, user_info);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "[SYNC_LOG] put object error,ret:%d",ret);
      TBSYS_LOG(INFO, "[SYNC_LOG] offset %ld cluster_id %d block_id %d file_id %ld size %d",
          frag_info.v_frag_meta_[i].offset_,
          frag_info.cluster_id_,
          frag_info.v_frag_meta_[i].block_id_, frag_info.v_frag_meta_[i].file_id_, frag_info.v_frag_meta_[i].size_);
    }
    else
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] write to kv put object success,offst:%"PRI64_PREFIX"d size:%d",
                frag_info.v_frag_meta_[i].offset_, frag_info.v_frag_meta_[i].size_);
    }
  }
  return ret;
}

bool diff_info(NameMetaClientImpl &client, int64_t app_id, int64_t uid, const string& bucket_name, const string& object_name)
{
  int ret = TFS_SUCCESS;
  bool noret = false;
  FragInfo frag_info;
  client.read_frag_info(app_id, uid, object_name.c_str(), frag_info);
  FileMetaInfo file_meta_info;
  client.ls_file(app_id, uid, object_name.c_str(), file_meta_info);

  ObjectInfo *base_object_info = new ObjectInfo();
  ObjectInfo *object_info = new ObjectInfo();
  UserInfo *user_info = new UserInfo();
  bool still_have = false;
  int64_t offset = 0;
  int64_t length = file_meta_info.size_;
  do
  {
    int64_t read_size = 0;
    still_have = false;
    ret = tfs::client::KvMetaHelper::do_get_object(new_server_id,
        bucket_name.c_str(), object_name.c_str() + 1,
        offset, length,
        object_info, &still_have,
        *user_info);
    if (ret != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "[SYNC_LOG] get_obejct |%s|%s| error", bucket_name.c_str(), object_name.c_str());
    }
    if (offset == 0)
    {
      *base_object_info = *object_info;
      base_object_info->v_tfs_file_info_.clear();
    }

    for(size_t j = 0; j < object_info->v_tfs_file_info_.size(); ++j)
    {
      if (object_info->v_tfs_file_info_[j].offset_ < offset)
      {
        continue;
      }
      base_object_info->v_tfs_file_info_.push_back(object_info->v_tfs_file_info_[j]);
      read_size += object_info->v_tfs_file_info_[j].file_size_;
    }
    offset += read_size;
    length -= read_size;
  }while(still_have && length > 0);

  if (abs(file_meta_info.create_time_ - base_object_info->meta_info_.create_time_) > 120)
  {
    TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s create_time is diff old:%d new:%"PRI64_PREFIX"d",
        bucket_name.c_str(), object_name.c_str(), file_meta_info.create_time_, base_object_info->meta_info_.create_time_);
    //old meta bug
    //noret = true;
  }
  if (abs(file_meta_info.modify_time_ != base_object_info->meta_info_.modify_time_) > 120)
  {
    TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s modify_time is diff old:%d new:%"PRI64_PREFIX"d",
        bucket_name.c_str(), object_name.c_str(), file_meta_info.modify_time_, base_object_info->meta_info_.modify_time_);
    //old meta bug
    //noret = true;
  }
  if (file_meta_info.size_ != base_object_info->meta_info_.big_file_size_)
  {
    TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s object_size is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
        bucket_name.c_str(), object_name.c_str(), file_meta_info.size_, base_object_info->meta_info_.big_file_size_);
    noret = true;
  }
  if (!noret)
  {
    if (frag_info.v_frag_meta_.size() == base_object_info->v_tfs_file_info_.size())
    {
      TBSYS_LOG(DEBUG, " now size equal is %lu",frag_info.v_frag_meta_.size());
      for(size_t i = 0; i < frag_info.v_frag_meta_.size(); ++i)
      {
        if (frag_info.cluster_id_ != base_object_info->v_tfs_file_info_[i].cluster_id_)
        {
          TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s cluster_id is diff old:%d new:%d",
              bucket_name.c_str(), object_name.c_str(), frag_info.cluster_id_, base_object_info->v_tfs_file_info_[i].cluster_id_);
          noret = true;
          break;
        }
        if ((int64_t)(frag_info.v_frag_meta_[i].file_id_) != base_object_info->v_tfs_file_info_[i].file_id_)
        {
          TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s file_id is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
              bucket_name.c_str(), object_name.c_str(), frag_info.v_frag_meta_[i].file_id_, base_object_info->v_tfs_file_info_[i].file_id_);
          noret = true;
          break;
        }
        if (frag_info.v_frag_meta_[i].offset_ != base_object_info->v_tfs_file_info_[i].offset_)
        {
          TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s offset is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
              bucket_name.c_str(), object_name.c_str(), frag_info.v_frag_meta_[i].offset_, base_object_info->v_tfs_file_info_[i].offset_);
          noret = true;
          break;
        }
        if (frag_info.v_frag_meta_[i].block_id_ != base_object_info->v_tfs_file_info_[i].block_id_)
        {
          TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s block_id is diff old:%d new:%"PRI64_PREFIX"d",
              bucket_name.c_str(), object_name.c_str(), frag_info.v_frag_meta_[i].block_id_, base_object_info->v_tfs_file_info_[i].block_id_);
          noret = true;
          break;
        }
        if (frag_info.v_frag_meta_[i].size_ != base_object_info->v_tfs_file_info_[i].file_size_)
        {
          TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s frag_size is diff old:%d new:%"PRI64_PREFIX"d",
              bucket_name.c_str(), object_name.c_str(), frag_info.v_frag_meta_[i].size_, base_object_info->v_tfs_file_info_[i].file_size_);
          noret = true;
          break;
        }
        // old_meta
        TBSYS_LOG(DEBUG,"old_meta-> cluster_id:%d, file_id:%"PRI64_PREFIX"d, offset:%"PRI64_PREFIX"d, block_id:%d, size:%d",
            frag_info.cluster_id_,
            frag_info.v_frag_meta_[i].file_id_,
            frag_info.v_frag_meta_[i].offset_,
            frag_info.v_frag_meta_[i].block_id_,
            frag_info.v_frag_meta_[i].size_);
        //new kv_meta
        TBSYS_LOG(DEBUG,"new_meta-> cluster_id:%d, file_id:%"PRI64_PREFIX"d, offset:%"PRI64_PREFIX"d, block_id:%ld, size:%ld",
            base_object_info->v_tfs_file_info_[i].cluster_id_,
            base_object_info->v_tfs_file_info_[i].file_id_,
            base_object_info->v_tfs_file_info_[i].offset_,
            base_object_info->v_tfs_file_info_[i].block_id_,
            base_object_info->v_tfs_file_info_[i].file_size_);
      }
    }
    else
    {
      TBSYS_LOG(INFO, "[DIFF_INFO] bucket:%s object:%s frag_size is diff old:%lu new:%lu",
          bucket_name.c_str(), object_name.c_str(), frag_info.v_frag_meta_.size(), base_object_info->v_tfs_file_info_.size());
      noret = true;
    }
  }
  delete user_info;
  delete object_info;
  delete base_object_info;
  TBSYS_LOG(INFO, "[SYNC_LOG] diff complete");
  return noret;
}

int del_from_kv(const string& bucket_name, const string& object_name)
{
  ObjectInfo object_info;
  UserInfo user_info;
  bool still_have = false;
  int ret;
  do
  {
    ret = tfs::client::KvMetaHelper::do_del_object(new_server_id,
                  bucket_name.c_str(), object_name.c_str() + 1,
                  &object_info, &still_have, user_info);
    if (ret == TFS_SUCCESS)
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] NO_YES del success");
    }
    else
    {
      TBSYS_LOG(ERROR, "[SYNC_LOG] NO_YES del fail");
    }
  }while(still_have);
  return ret;
}
int check(NameMetaClientImpl &client, int64_t app_id, int64_t uid, const string& bucket_name, const string& object_name)
{
  int ret = TFS_SUCCESS;
  bool old_is_exist = false;
  old_is_exist = client.is_file_exist(app_id, uid, object_name.c_str());
  ObjectInfo object_info;
  UserInfo user_info;
  int iret = -1;
  ret = tfs::client::KvMetaHelper::do_head_object(new_server_id,
            bucket_name.c_str(), object_name.c_str() + 1,
            &object_info, user_info);
  bool new_is_exist = false;
  if (EXIT_OBJECT_NOT_EXIST == ret)
  {
    new_is_exist = false;
  }
  else if (TFS_SUCCESS == ret)
  {
    new_is_exist = true;
  }
  else
  {
    iret = ERROR_MES;
    TBSYS_LOG(ERROR, "head_obejct |%s|%s| error",bucket_name.c_str(),object_name.c_str());
    return iret;
  }
  if (old_is_exist && !new_is_exist)//yes no
  {
    iret = YES_NO;
  }
  else if(old_is_exist && new_is_exist)//yes yes
  {
    iret = YES_YES;
  }
  else if(!old_is_exist && new_is_exist)//no yes
  {
    iret = NO_YES;
  }
  else if(!old_is_exist && !new_is_exist)//no no
  {
    iret = NO_NO;
  }
  return iret;
}
int main(int argc, char *argv[])
{
  gstreamer.set_packet_factory(&gfactory);

  tfs::common::NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  int i;

  std::string old_server;
  std::string new_server;
  std::string date_time;
  std::string log_file_name;
  std::string input_file_name;
  while ((i = getopt(argc, argv, "i:o:n:t:l:v")) != EOF)
  {
    switch (i)
    {
      case 'i':
        input_file_name = optarg;
        break;
      case 'o':
        old_server = optarg;
        break;
      case 'n':
        new_server = optarg;
        break;
      case 't':
        date_time = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      case 'v':
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  cout << input_file_name  << "  -" << old_server << "  -" << new_server << "  -" << date_time << "  -" << log_file_name << endl;
  if (input_file_name.empty() || log_file_name.empty())
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);

  NameMetaClientImpl old_client;
  if( TFS_SUCCESS != old_client.initialize( old_server.c_str() ) )
  {
    return TFS_ERROR;
  }
  TBSYS_LOG(DEBUG, "old_client initialize SUCCESS");

  old_server_id = Func::get_host_ip(old_server.c_str());
  new_server_id = Func::get_host_ip(new_server.c_str());
  string tfs_name = "/";
  int64_t time_point = tbsys::CTimeUtil::strToTime(const_cast<char*>(date_time.c_str()));

  int64_t app_id;
  int64_t uid;
  string bucket_name;
  string object_name;
  string data_time;
  int64_t time_this = 0;
  int32_t ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  s_fd = fopen(input_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", input_file_name.c_str());
    ret =  TFS_ERROR;
  }
  char buff[640];
  while(TFS_SUCCESS == ret && 1 != stop)
  {
    if (NULL == fgets(buff, 640, s_fd))
    {
      break;
    }
    buff[639] = 0;
    char *p = NULL;
    char *q = NULL;
    const char DLIMER = ',';
    data_time.clear();
    //year
    q = buff;
    p = strchr(q, '/');
    *p = '\0';
    data_time = q;
    q = p + 1;
    //month
    p = strchr(q, '/');
    *p = '\0';
    data_time += q;
    q = p + 1;
    //day
    p = strchr(q, ' ');
    *p = '\0';
    data_time += q;
    q = p + 1;
    //hour
    p = strchr(q, ':');
    *p = '\0';
    data_time += q;
    q = p + 1;
    //minute
    p = strchr(q, ':');
    *p = '\0';
    data_time += q;
    q = p + 1;
    //second
    p = strchr(q, ' ');
    *p = '\0';
    data_time += q;
    q = p + 1;

    time_this = tbsys::CTimeUtil::strToTime(const_cast<char*>(data_time.c_str()));
    if (time_this < time_point)
    {
      continue;
    }

    //guolv
    p = strchr(q, '#');
    q = p;
    p = strchr(q, ':');
    q = p + 2;
    //appid
    p = strchr(q, DLIMER);
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", q);
      continue;
    }
    app_id = -1;
    *p = '\0';
    app_id = strtoll(q, NULL, 10);
    q = p + 2;
    //uid
    p = strchr(q, DLIMER);
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", q);
      continue;
    }
    uid = -1;
    *p = '\0';
    uid = strtoll(q, NULL, 10);
    q = p + 2;
    if (app_id <= 0 || uid <= 0)
    {
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    //bucketname
    p = strchr(q, DLIMER);
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", q);
      continue;
    }
    *p = '\0';
    bucket_name = q;
    q = p + 2;
    //objectname
    p = strchr(q, '\n');
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", q);
    }
    if (*p == '\n')
    {
      *p = '\0';
    }
    string tmp_object;
    tmp_object = q;
    object_name = "/";
    object_name += tmp_object;
    TBSYS_LOG(INFO, "[SYNC_LOG]data_time is %s app_id %ld uid %ld bucketname %s objectname %s", data_time.c_str(), app_id, uid, bucket_name.c_str(), object_name.c_str());

    int iret;
    bool no_ret;
    iret = check(old_client, app_id, uid, bucket_name, object_name);
    if (iret == YES_NO)
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] YES_NO");
      write_to_kv(old_client, app_id, uid, bucket_name, object_name);
    }
    else if (iret == YES_YES)
    {
      no_ret = false;
      TBSYS_LOG(INFO, "[SYNC_LOG] YES_YES");
      no_ret = diff_info(old_client, app_id, uid, bucket_name, object_name);
      if (no_ret)
      {
        del_from_kv(bucket_name, object_name);
        write_to_kv(old_client, app_id, uid, bucket_name, object_name);
      }
    }
    else if (iret == NO_YES)
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] NO_YES");
      del_from_kv(bucket_name, object_name);
    }
    else if (iret == NO_NO)
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] NO_NO app_id %ld uid %ld bucketname %s objectname %s",
       app_id, uid, bucket_name.c_str(), object_name.c_str());
    }
    else
    {
      TBSYS_LOG(INFO, "[SYNC_LOG] sorry check fail");
    }
  }

  fclose(s_fd);
  s_fd = NULL;
  return 0;
}

