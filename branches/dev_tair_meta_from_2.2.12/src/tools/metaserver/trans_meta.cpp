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

#include "common/parameter.h"
#include "common/config_item.h"
#include "name_meta_server/meta_store_manager.h"
#include "name_meta_server/meta_server_service.h"

using namespace std;
using namespace __gnu_cxx;
using namespace tfs::common;
using namespace tfs::namemetaserver;

MetaStoreManager g_store_manager;
const int MAX_OBJECT_NAME_LEN = 256;

const int MAGIC_NUMBER0 = 0x4f264975;
const int MAGIC_NUMBER1 = 0x5f375a86;
const int MAGIC_NUMBER2 = 0x60486b97;
const int MAGIC_NUMBER3 = 0x71597ca8;
const int MAGIC_NUMBER4 = 0x826a8db9;

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
int usage (const char* n)
{
  printf("%s -f meta_config_file -s source_file -o dest_file -l log_file\n", n);
  return 0;
}

int deal_file(const int64_t app_id, const int64_t uid, const vector<MetaInfo> &v_meta_info,
    const string& full_name, FILE *d_fd)
{
  int ret = TFS_SUCCESS;
  vector<MetaInfo>::const_iterator it = v_meta_info.begin();
  for (; it != v_meta_info.end(); it++)
  {
    int full_name_len = full_name.length() + it->get_name_len() -1;
    if (full_name_len > MAX_OBJECT_NAME_LEN)
    {
      TBSYS_LOG(ERROR, "object name len out of range");
      TBSYS_LOG(INFO, "full_name %s name =%*s ct %d, mt %d, sz %lu",
          full_name.c_str(),
          it->get_name_len() - 1, it->get_name() + 1,
          it->file_info_.create_time_,
          it->file_info_.modify_time_,
          it->file_info_.size_);
      continue;
    }
    else
    {
      fwrite(&MAGIC_NUMBER1, sizeof(MAGIC_NUMBER1), 1, d_fd); //magic number
      fwrite(&full_name_len, sizeof(full_name_len), 1, d_fd); //ful name len
      fwrite(full_name.c_str(), full_name.length(), 1, d_fd);
      fwrite(it->get_name() + 1, it->get_name_len() - 1, 1, d_fd);  //name end
      fwrite(&(it->file_info_.create_time_), sizeof(it->file_info_.create_time_), 1, d_fd);//ct
      fwrite(&(it->file_info_.modify_time_), sizeof(it->file_info_.modify_time_), 1, d_fd);//mt
      fwrite(&(it->file_info_.size_), sizeof(it->file_info_.size_), 1, d_fd);//size
      fwrite(&MAGIC_NUMBER2, sizeof(MAGIC_NUMBER2), 1, d_fd); //magic number
    }
    int32_t cluster_id = 0;
    int64_t last_offset = 0;
    vector<MetaInfo> tmp_v_meta_info;
    ret = g_store_manager.get_meta_info_from_db(app_id, uid, it->file_info_.pid_,
        it->get_name(), it->get_name_len(), 0, tmp_v_meta_info,
        cluster_id, last_offset);
    for (size_t i = 0; i < tmp_v_meta_info.size(); i++)
    {
      vector<FragMeta>::const_iterator frag_it = tmp_v_meta_info[i].frag_info_.v_frag_meta_.begin();
      for (; frag_it != tmp_v_meta_info[i].frag_info_.v_frag_meta_.end(); frag_it++)
      {
        fwrite(&MAGIC_NUMBER3, sizeof(MAGIC_NUMBER3), 1, d_fd); //magic number
        //TBSYS_LOG(INFO, "off_set %ld cluster_id %d lock_id %d file_id %ld size %d",
        //    frag_it->offset_,
        //    cluster_id,
        //    frag_it->block_id_, frag_it->file_id_, frag_it->size_);
        fwrite(&(frag_it->offset_), sizeof(frag_it->offset_), 1, d_fd);
        fwrite(&(cluster_id), sizeof(cluster_id), 1, d_fd);
        fwrite(&(frag_it->block_id_), sizeof(frag_it->block_id_), 1, d_fd);
        fwrite(&(frag_it->file_id_), sizeof(frag_it->file_id_), 1, d_fd);
        fwrite(&(frag_it->size_), sizeof(frag_it->size_), 1, d_fd);
      }
    }
    fwrite(&MAGIC_NUMBER4, sizeof(MAGIC_NUMBER3), 1, d_fd); //magic number
  }
  return ret;
}
int transfer_dir(const int64_t app_id, const int64_t uid, const int64_t pid,
    char *name, int32_t &name_len, string full_name, FILE *d_fd)
{
  UNUSED(d_fd);
  vector<MetaInfo> v_meta_info;
  bool still_have = false;
  bool is_file = true;
  int ret = TFS_SUCCESS;
  int64_t file_count = 0;
  MetaInfo last_meta_info;
  vector<MetaInfo> tmp_v_meta_info;
  vector<MetaInfo>::iterator tmp_v_meta_info_it;
  //TBSYS_LOG(INFO, "transerfer dir name %s", full_name.c_str());

  name[0] = '\0';
  name_len = 1;       //continue get files
  do
  {
    tmp_v_meta_info.clear();
    still_have = false;

    ret = g_store_manager.ls(app_id, uid, pid, name, name_len, NULL, 0, is_file,
        tmp_v_meta_info, still_have);
    if (!tmp_v_meta_info.empty())
    {
      tmp_v_meta_info_it = tmp_v_meta_info.begin();

      if (is_file)
      {
        //convert info to file_info. merge the info hav been splited
        g_store_manager.calculate_file_meta_info(tmp_v_meta_info_it, tmp_v_meta_info.end(),
            false, v_meta_info, last_meta_info);
      }
      else
      {
        for (; tmp_v_meta_info_it != tmp_v_meta_info.end(); tmp_v_meta_info_it++)
        {
          v_meta_info.push_back(*tmp_v_meta_info_it);
        }
      }

      if (still_have)
      {
        tmp_v_meta_info_it--;
        MetaServerService::next_file_name_base_on(name, name_len,
            tmp_v_meta_info_it->get_name(), tmp_v_meta_info_it->get_name_len());
      }
    }

    if (!still_have && is_file)
    {
      //deal all files in v_meta_info;
      if (!full_name.empty() && full_name[full_name.length() -1] != '/')
        full_name += '/';

      deal_file(app_id, uid, v_meta_info, full_name, d_fd);
      fflush(d_fd);
      TBSYS_LOG(DEBUG, "we will ls dir's dir");

      file_count = v_meta_info.size();
      v_meta_info.clear();
      still_have = true;
      is_file = false;
      name[0] = '\0';
      name_len = 1;       //continue get files
    }

  } while(TFS_SUCCESS == ret && still_have);

  TBSYS_LOG(INFO, "app_id:%ld uid:%ld pid:%ld file_count = %ld, dir_count %lu",
      app_id, uid, pid, file_count, v_meta_info.size());

  for (size_t i = 0; i < v_meta_info.size() && TFS_SUCCESS == ret; i++)
  {
    name_len = v_meta_info[i].get_name_len();
    memcpy(name, v_meta_info[i].get_name(), name_len);
    if (full_name.empty() || full_name[full_name.length() -1] != '/')
      full_name += '/';
    string sub_full_name(full_name);
    if (name_len == 2 && *(name +1) == '/')
    {
      ;
    }
    else
    {
      sub_full_name.append(name + 1, name_len - 1);
    }

    ret = transfer_dir(app_id, uid, v_meta_info[i].file_info_.id_,
        name, name_len, sub_full_name, d_fd);
  }

  return ret;
}
void transfer(const string &source_file_name, const string &out_file_name)
{
  int ret = TFS_SUCCESS;
  FILE *s_fd = NULL;
  FILE *d_fd = NULL;
  s_fd = fopen(source_file_name.c_str(), "r");
  if (NULL == s_fd)
  {
    printf(" open file %s for read error\n", source_file_name.c_str());
    ret =  TFS_ERROR;
  }
  d_fd = fopen(out_file_name.c_str(), "a+");
  if (NULL == s_fd)
  {
    printf(" open file %s for append\n", out_file_name.c_str());
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
    int64_t app_id = -1;
    int64_t uid = -1;
    *p = '\0';
    app_id = strtoll(buff, NULL, 10);
    uid = strtoll(p + 1, NULL, 10);
    if (app_id <= 0 || uid <= 0)
    {
      *p = DLIMER;
      TBSYS_LOG(ERROR, "err input line %s", buff);
      continue;
    }
    TBSYS_LOG(INFO, "transfer app_id %ld uid %ld", app_id, uid);
    char name[512] = {'\0'};
    name[1]='/';
    name[0]=1;
    int32_t name_len = 2;
    string full_name;
    fwrite(&MAGIC_NUMBER0, sizeof(MAGIC_NUMBER0), 1, d_fd); //magic number
    fwrite(&app_id, sizeof(app_id), 1, d_fd);
    fwrite(&uid, sizeof(uid), 1, d_fd);
    ret = transfer_dir(app_id, uid, 0, name, name_len, full_name, d_fd);
  }
  fclose(s_fd);
  s_fd = NULL;
  fclose(d_fd);
  d_fd = NULL;
  return ;
}

int main(int argc, char *argv[])
{
  int i;
  std::string config_file_name;
  std::string source_file_name;
  std::string out_file_name;
  std::string log_file_name;
  while ((i = getopt(argc, argv, "f:s:l:o:")) != EOF)
  {
    switch (i)
    {
      case 'f':
        config_file_name = optarg;
        break;
      case 's':
        source_file_name = optarg;
        break;
      case 'o':
        out_file_name = optarg;
        break;
      case 'l':
        log_file_name = optarg;
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if (config_file_name.empty() || source_file_name.empty())
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  if (TBSYS_CONFIG.load(config_file_name.c_str()) == TFS_ERROR)
  {
    TBSYS_LOG(ERROR, "load conf(%s) failed", config_file_name.c_str());
    return TFS_ERROR;
  }
  TBSYS_LOGGER.setFileName(log_file_name.c_str());
  TBSYS_LOGGER.setLogLevel("info");
  signal(SIGINT, sign_handler);
  int ret = TFS_SUCCESS;
  if (TFS_SUCCESS != SYSPARAM_NAMEMETASERVER.initialize())
  {
    TBSYS_LOG(ERROR, "call SYSPARAM_NAMEMETASERVER::initialize fail.");
  }
  else
  {
    ret = g_store_manager.init(SYSPARAM_NAMEMETASERVER.max_pool_size_,
        SYSPARAM_NAMEMETASERVER.max_cache_size_,
        SYSPARAM_NAMEMETASERVER.gc_ratio_, SYSPARAM_NAMEMETASERVER.max_mutex_size_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "init store_manager error");
    }
    else
    {
      transfer(source_file_name, out_file_name);
    }
  }

  return 0;
}


