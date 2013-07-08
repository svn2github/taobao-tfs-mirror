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
int64_t app_id = 0;
int64_t uid = 0;

static tfs::message::MessageFactory gfactory;
static tfs::common::BasePacketStreamer gstreamer;


int usage(const char* name)
{
  printf("-i: means input_file \n");
  printf("-o: means old_root_addr \n");
  printf("-n: means new_kv_meta_addr \n");
  printf("-l: means output_file \n");
  printf("Usage: %s -i input.txt -o old_root_ipport -n new_kvmeta_ipport -l ./log_file \n", name);
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
int check(NameMetaClientImpl &client ,string path)
{
  int check_num = 0;
  string new_path;
  string now_name;

  std::vector<FileMetaInfo> meta_info;
  std::vector<FileMetaInfo>::const_iterator it;
  client.ls_dir(app_id, uid, path.c_str(), meta_info, false);
  TBSYS_LOG(INFO, "vector num %lu", meta_info.size());
  for(it = meta_info.begin(); it != meta_info.end(); ++it)
  {
    now_name = it->name_;
    new_path = path + now_name;
    if(it->id_ == 0)//file
    {
      TBSYS_LOG(DEBUG, "this is file %s", new_path.c_str());
      FragInfo *frag_info = new FragInfo();
      client.read_frag_info(app_id, uid, new_path.c_str(), *frag_info);

      char *bucket_name = new char[256];
      sprintf(bucket_name, "%ld^%ld", app_id, uid);
      ObjectInfo *base_object_info = new ObjectInfo();
      ObjectInfo *object_info = new ObjectInfo();
      UserInfo *user_info = new UserInfo();
      bool still_have = false;
      int64_t offset = 0;
      int64_t length = it->size_;
      int ret = TFS_SUCCESS;
      do
      {
        int64_t read_size = 0;
        ret = tfs::client::KvMetaHelper::do_get_object(new_server_id,
            bucket_name, new_path.c_str() + 1,
            offset, length,
            object_info, &still_have,
            *user_info);
        if (ret == EXIT_OBJECT_NOT_EXIST)
        {
          check_num++;
          TBSYS_LOG(ERROR, "[DIFF_INFO] bucket: %s object: %s ",bucket_name, new_path.c_str() + 1);
          break;
        }
        else if (ret != TFS_SUCCESS)
        {
          check_num++;
          TBSYS_LOG(ERROR, "[DIFF_INFO] bucket: %s object: %s ",bucket_name, new_path.c_str() + 1);
          break;
        }
        if (offset == 0)
        {
          *base_object_info = *object_info;
          base_object_info->v_tfs_file_info_.clear();
        }
        //todo better
        for(size_t j = 0; j < object_info->v_tfs_file_info_.size(); ++j)
        {
          if (object_info->v_tfs_file_info_[j].offset_ < offset)
          {
            continue;
          }
          base_object_info->v_tfs_file_info_.push_back(object_info->v_tfs_file_info_[j]);
          read_size = object_info->v_tfs_file_info_[j].offset_ + object_info->v_tfs_file_info_[j].file_size_;
        }
        offset = read_size;
        length = it->size_ - offset;

      }while(still_have && length > 0);
      if (TFS_SUCCESS == ret)
      {
        //check
        if (abs(it->create_time_ - base_object_info->meta_info_.create_time_) > 120)
        {
          TBSYS_LOG(INFO, "check [DIFF_TIME] bucket: %s object: %s create_time is diff old:%d new:%"PRI64_PREFIX"d",
              bucket_name, new_path.c_str() + 1, it->create_time_, base_object_info->meta_info_.create_time_);
        }
        if (abs(it->modify_time_ - base_object_info->meta_info_.modify_time_) > 120)
        {
          TBSYS_LOG(INFO, "check [DIFF_TIME] bucket: %s object: %s modify_time is diff old:%d new:%"PRI64_PREFIX"d",
              bucket_name, new_path.c_str() + 1, it->modify_time_, base_object_info->meta_info_.modify_time_);
        }
        if (it->size_ != base_object_info->meta_info_.big_file_size_)
        {
          TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s object_size is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
              bucket_name, new_path.c_str() + 1, it->size_, base_object_info->meta_info_.big_file_size_);
        }
        check_num++;
        if (frag_info->v_frag_meta_.size() == base_object_info->v_tfs_file_info_.size())
        {
          TBSYS_LOG(DEBUG, " now size equal is %lu",frag_info->v_frag_meta_.size());
          for(size_t i = 0; i < frag_info->v_frag_meta_.size(); ++i)
          {
            if (frag_info->cluster_id_ != base_object_info->v_tfs_file_info_[i].cluster_id_ && frag_info->cluster_id_ != -1)
            {
              TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s cluster_id is diff old:%d new:%d",
                  bucket_name, new_path.c_str() + 1, frag_info->cluster_id_, base_object_info->v_tfs_file_info_[i].cluster_id_);
            }
            if ((int64_t)(frag_info->v_frag_meta_[i].file_id_) != base_object_info->v_tfs_file_info_[i].file_id_)
            {
              TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s file_id is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
                  bucket_name, new_path.c_str() + 1, frag_info->v_frag_meta_[i].file_id_, base_object_info->v_tfs_file_info_[i].file_id_);
            }
            if (frag_info->v_frag_meta_[i].offset_ != base_object_info->v_tfs_file_info_[i].offset_)
            {
              TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s offset is diff old:%"PRI64_PREFIX"d new:%"PRI64_PREFIX"d",
                  bucket_name, new_path.c_str() + 1, frag_info->v_frag_meta_[i].offset_, base_object_info->v_tfs_file_info_[i].offset_);
            }
            if (frag_info->v_frag_meta_[i].block_id_ != base_object_info->v_tfs_file_info_[i].block_id_)
            {
              TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s block_id is diff old:%d new:%"PRI64_PREFIX"d",
                  bucket_name, new_path.c_str() + 1, frag_info->v_frag_meta_[i].block_id_, base_object_info->v_tfs_file_info_[i].block_id_);
            }
            if (frag_info->v_frag_meta_[i].size_ != base_object_info->v_tfs_file_info_[i].file_size_)
            {
              TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s frag_size is diff old:%d new:%"PRI64_PREFIX"d",
                  bucket_name, new_path.c_str() + 1, frag_info->v_frag_meta_[i].size_, base_object_info->v_tfs_file_info_[i].file_size_);
            }
            // old_meta
            TBSYS_LOG(DEBUG,"old_meta-> cluster_id:%d, file_id:%"PRI64_PREFIX"d, offset:%"PRI64_PREFIX"d, block_id:%d, size:%d",
                frag_info->cluster_id_,
                frag_info->v_frag_meta_[i].file_id_,
                frag_info->v_frag_meta_[i].offset_,
                frag_info->v_frag_meta_[i].block_id_,
                frag_info->v_frag_meta_[i].size_);

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
          TBSYS_LOG(INFO, "check [DIFF_INFO] bucket: %s object: %s frag_size is diff old:%lu new:%lu",
              bucket_name, new_path.c_str() + 1, frag_info->v_frag_meta_.size(), base_object_info->v_tfs_file_info_.size());
        }
      }
      delete user_info;
      delete object_info;
      delete base_object_info;
      delete[] bucket_name;
      delete frag_info;
      continue;
    }
    new_path += "/";
    check(client, new_path);
  }
  TBSYS_LOG(INFO, "checkr num %d", check_num);
  return 0;
}

int main(int argc, char *argv[])
{
  gstreamer.set_packet_factory(&gfactory);

  tfs::common::NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  int i;

  std::string old_server;
  std::string new_server;
  std::string log_file_name;
  std::string input_file_name;
  while ((i = getopt(argc, argv, "i:o:n:l:")) != EOF)
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
      case 'l':
        log_file_name = optarg;
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  cout << input_file_name  << "  -" << old_server << "  -" << new_server << "  -" << log_file_name << endl;
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
  int32_t ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  s_fd = fopen(input_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", input_file_name.c_str());
    ret =  TFS_ERROR;
  }
  char buff[128];
  while(TFS_SUCCESS == ret && 1 != stop)
  {
    if (NULL == fgets(buff, 128, s_fd))
    {
      break;
    }
    buff[127] = 0;
    char *p = NULL;
    const char DLIMER = ',';
    p = strchr(buff, DLIMER);
    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    app_id = -1;
    uid = -1;
    *p = '\0';
    app_id = strtoll(buff, NULL, 10);
    uid = strtoll(p + 1, NULL, 10);
    if (app_id <= 0 || uid <= 0)
    {
      *p = DLIMER;
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    TBSYS_LOG(INFO, "check app_id %ld uid %ld", app_id, uid);
    ret = check(old_client, tfs_name);
  }
  fclose(s_fd);
  s_fd = NULL;
  return 0;
}

