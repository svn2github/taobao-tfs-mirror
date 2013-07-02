/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfstool.cpp $
 *
 * Authors:
 *
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>

#include <vector>
#include <string>
#include <map>

#include "tbsys.h"

#include "common/internal.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "common/kv_meta_define.h"
#include "common/kv_rts_define.h"
#include "message/server_status_message.h"
#include "message/client_cmd_message.h"
#include "message/message_factory.h"
#include "message/kv_rts_message.h"
#include "common/base_packet_streamer.h"
#include "tools/util/tool_util.h"
#include "tools/util/ds_lib.h"
#include "new_client/fsname.h"

#include "new_client/tfs_kv_meta_helper.h"
#define _WITH_READ_LINE

using namespace std;
using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

static STR_FUNC_MAP g_cmd_map;
char app_key[256];
//NameMetaClientImpl impl;

#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

char* match_cmd(const char* text, int state)
{
  static STR_FUNC_MAP_ITER it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = g_cmd_map.begin();
    len = strlen(text);
  }

  while(it != g_cmd_map.end())
  {
    cmd = it->first.c_str();
    it++;
    if (strncmp(cmd, text, len) == 0)
    {
      int32_t cmd_len = strlen(cmd) + 1;
      // memory will be freed by readline
      return strncpy(new char[cmd_len], cmd, cmd_len);
    }
  }
  return NULL;
}

char** tfscmd_completion (const char* text, int start, int)
{
  rl_attempted_completion_over = 1;
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

static void sign_handler(const int32_t sig);
static void usage(const char* name);
void init();
int main_loop();
int do_cmd(char* buffer);
int cmd_show_help(const VSTRING& param);
int cmd_quit(const VSTRING& param);
int cmd_batch_file(const VSTRING& param);
// for kv meta server
int cmd_put_bucket(const VSTRING& param);
int cmd_get_bucket(const VSTRING& param);
int cmd_del_bucket(const VSTRING& param);
int cmd_head_bucket(const VSTRING& param);

int cmd_put_object(const VSTRING& param);
int cmd_get_object(const VSTRING& param);
int cmd_del_object(const VSTRING& param);
int cmd_head_object(const VSTRING& param);

const char* krs_addr = NULL;
int64_t new_server_id = 0;

static tfs::common::BasePacketStreamer gstreamer;
static tfs::message::MessageFactory gfactory;



int main(int argc, char* argv[])
{
  int32_t i;
  bool directly = false;
  bool set_log_level = false;

  // analyze arguments
  while ((i = getopt(argc, argv, "s:k:ih")) != EOF)
  {
    switch (i)
    {
      case 'n':
        set_log_level = true;
        break;
      case 'k':
        krs_addr = optarg;
        break;
      case 'i':
        directly = true;
        break;
      case 'h':
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  if (NULL == krs_addr)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  gstreamer.set_packet_factory(&gfactory);
  NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  if (krs_addr != NULL)
  {
    new_server_id = Func::get_host_ip(krs_addr);
  }

  init();

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else // has other params
  {
    int32_t i = 0;
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        do_cmd(argv[i]);
      }
    }
    else
    {
      VSTRING param;
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
        cmd_batch_file(param);
      }
    }
  }
  return TFS_SUCCESS;
}

static void usage(const char* name)
{
  fprintf(stderr,"name:%s"
          "       -k kvmetaserver ip port\n"
          "       -n set log level\n"
          "       -i directly execute the command\n"
          "       -h help\n"
          ,name
         );
}

static void sign_handler(const int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "\nTFS> ");
      break;
  }
}

void init()
{
  g_cmd_map["help"] = CmdNode("help", "show help info", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit", "quit", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit", "exit", 0, 0, cmd_quit);
  g_cmd_map["@"] = CmdNode("@ file", "batch run command in file", 1, 1, cmd_batch_file);
  g_cmd_map["batch"] = CmdNode("batch file", "batch run command in file", 1, 1, cmd_batch_file);

  g_cmd_map["put_bucket"] = CmdNode("put_bucket bucket_name owner_id", "create a bucket", 2, 2, cmd_put_bucket);
  g_cmd_map["get_bucket"] = CmdNode("get_bucket bucket_name [ prefix start_key delimiter limit ]", "get a bucket(list object)", 1, 5, cmd_get_bucket);
  g_cmd_map["del_bucket"] = CmdNode("del_bucket bucket_name", "delete a bucket", 1, 1, cmd_del_bucket);
  g_cmd_map["head_bucket"] = CmdNode("head_bucket bucket_name", "stat a bucket", 1, 1, cmd_head_bucket);

  g_cmd_map["put_object"] = CmdNode("put_object bucket_name object_name owner_id", "put a object", 3, 3, cmd_put_object);
  g_cmd_map["get_object"] = CmdNode("get_object bucket_name object_name offset length", "get a object", 4, 4, cmd_get_object);
  g_cmd_map["del_object"] = CmdNode("del_object bucket_name object_name", "delete a object", 2, 2, cmd_del_object);
  g_cmd_map["head_object"] = CmdNode("head_object bucket_name object_name", "stat a object", 2, 2, cmd_head_object);

}

int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = tfscmd_completion;
#else
  char cmd_line[MAX_CMD_SIZE];
#endif

  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("TFS> ");
    if (!cmd_line)
#else
      fprintf(stdout, "TFS> ");
    if (NULL == fgets(cmd_line, MAX_CMD_SIZE, stdin))
#endif
    {
      continue;
    }
    ret = do_cmd(cmd_line);
#ifdef _WITH_READ_LINE
    free(cmd_line);
#endif
    if (TFS_CLIENT_QUIT == ret)
    {
      break;
    }
  }
  return TFS_SUCCESS;
}

int32_t do_cmd(char* key)
{
  char* token;
  while (' ' == *key)
  {
    key++;
  }
  token = key + strlen(key);
  while (' ' == *(token - 1) || '\n' == *(token - 1) || '\r' == *(token - 1))
  {
    token--;
  }
  *token = '\0';
  if ('\0' == key[0])
  {
    return TFS_SUCCESS;
  }

#ifdef _WITH_READ_LINE
  // not blank line, add to history
  add_history(key);
#endif

  token = strchr(key, ' ');
  if (token != NULL)
  {
    *token = '\0';
  }

  string cur_cmd = Func::str_to_lower(key);
  STR_FUNC_MAP_ITER it = g_cmd_map.find(cur_cmd);

  if (it == g_cmd_map.end())
  {
    fprintf(stderr, "unknown command. \n");
    return TFS_ERROR;
  }

  if (token != NULL)
  {
    token++;
    key = token;
  }
  else
  {
    key = NULL;
  }

  VSTRING param;
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if ('\0' == token[0])
    {
      continue;
    }
    param.push_back(token);
  }

  // check param count
  int32_t param_cnt = param.size();
  if (param_cnt < it->second.min_param_cnt_ || param_cnt > it->second.max_param_cnt_)
  {
    fprintf(stderr, "%s\t\t%s\n\n", it->second.syntax_, it->second.info_);
    return TFS_ERROR;
  }

  return it->second.func_(param);
}

const char* canonical_param(const string& param)
{
  const char* ret_param = param.c_str();
  if (NULL != ret_param &&
      (strlen(ret_param) == 0 ||
       strcasecmp(ret_param, "null") == 0))
  {
    ret_param = NULL;
  }
  return ret_param;
}

// expand ~ to HOME. modify argument
const char* expand_path(string& path)
{
  if (path.size() > 0 && '~' == path.at(0) &&
      (1 == path.size() ||                      // just one ~
       (path.size() > 1 && '/' == path.at(1)))) // like ~/xxx
  {
    char* home_path = getenv("HOME");
    if (NULL == home_path)
    {
      fprintf(stderr, "can't get HOME path: %s\n", strerror(errno));
    }
    else
    {
      path.replace(0, 1, home_path);
    }
  }
  return path.c_str();
}

int cmd_show_help(const VSTRING&)
{
  return ToolUtil::show_help(g_cmd_map);
}

int cmd_quit(const VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_batch_file(const VSTRING& param)
{
  const char* batch_file = expand_path(const_cast<string&>(param[0]));
  FILE* fp = fopen(batch_file, "rb");
  int ret = TFS_SUCCESS;
  if (fp == NULL)
  {
    fprintf(stderr, "open file error: %s\n\n", batch_file);
    ret = TFS_ERROR;
  }
  else
  {
    int32_t error_count = 0;
    int32_t count = 0;
    VSTRING params;
    char buffer[MAX_CMD_SIZE];
    while (fgets(buffer, MAX_CMD_SIZE, fp))
    {
      if ((ret = do_cmd(buffer)) == TFS_ERROR)
      {
        error_count++;
      }
      if (++count % 100 == 0)
      {
        fprintf(stdout, "tatol: %d, %d errors.\r", count, error_count);
        fflush(stdout);
      }
      if (TFS_CLIENT_QUIT == ret)
      {
        break;
      }
    }
    fprintf(stdout, "tatol: %d, %d errors.\n\n", count, error_count);
    fclose(fp);
  }
  return TFS_SUCCESS;
}

int cmd_put_bucket(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();
  int64_t owner_id = strtoll(param[1].c_str(), NULL, 10);
  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  user_info.owner_id_ = owner_id;

  int ret = tfs::client::KvMetaHelper::do_put_bucket(new_server_id, bucket_name,
                                   bucket_meta_info, user_info);
  if (TFS_SUCCESS == ret)
  {
    ToolUtil::print_info(ret, "put bucket %s owner_id : %ld", bucket_name, owner_id);
  }
  return ret;
}

int cmd_get_bucket(const VSTRING& param)
{
  int size = param.size();

  int ret = TFS_SUCCESS;
  const char *bucket_name = NULL;
  const char *prefix = NULL;
  const char *start_key = NULL;
  char delimiter = DEFAULT_CHAR;
  int32_t limit = MAX_LIMIT;

  bucket_name = param[0].c_str();

  if (size > 1)
  {
    prefix = canonical_param(param[1]);
  }

  if (size > 2)
  {
    start_key = canonical_param(param[2]);
  }

  if (size > 3)
  {
    delimiter = canonical_param(param[3]) == NULL ? DEFAULT_CHAR : (param[3].size() == 1 ? param[3][0] : DEFAULT_CHAR);
  }

  if (size > 4)
  {
    limit = atoi(param[4].c_str());
  }

  vector<ObjectMetaInfo> v_object_meta_info;
  VSTRING v_object_name;
  set<string> s_common_prefix;
  int8_t is_truncated = 0;
  UserInfo user_info;
  ret = tfs::client::KvMetaHelper::do_get_bucket(new_server_id, bucket_name, prefix, start_key, delimiter, limit,
                                   &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  if (TFS_SUCCESS == ret)
  {
    printf("bucket: %s has %d common_prefix\n", bucket_name, static_cast<int>(s_common_prefix.size()));
    set<string>::iterator iter = s_common_prefix.begin();
    for (int i = 0; iter != s_common_prefix.end(); iter++, i++)
    {
      cout << i << ": " << *iter << endl;
    }
  }

  if (TFS_SUCCESS == ret)
  {
    printf("bucket: %s has %d objects\n", bucket_name, static_cast<int>(v_object_name.size()));
    for (int i = 0; i < static_cast<int>(v_object_name.size()); i++)
    {
      cout << i << ": " << v_object_name[i] << endl;
    }
  }

  //todo show info of objects
  ToolUtil::print_info(ret, "get bucket %s", bucket_name);
  return ret;
}

int cmd_del_bucket(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();
  UserInfo user_info;
  int ret = tfs::client::KvMetaHelper::do_del_bucket(new_server_id, bucket_name, user_info);
  //int ret = g_kv_meta_client.del_bucket(bucket_name, user_info);
  ToolUtil::print_info(ret, "del bucket %s", bucket_name);
  return ret;
}

int cmd_head_bucket(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();

  BucketMetaInfo bucket_meta_info;
  UserInfo user_info;
  int ret = tfs::client::KvMetaHelper::do_head_bucket(new_server_id, bucket_name, &bucket_meta_info, user_info);
  //int ret = g_kv_meta_client.head_bucket(bucket_name, &bucket_meta_info, user_info);

  if (TFS_SUCCESS == ret)
  {
    printf("bucket: %s, create_time: %"PRI64_PREFIX"d, owner_id: %"PRI64_PREFIX"d\n",
        bucket_name, bucket_meta_info.create_time_, bucket_meta_info.owner_id_);
  }

  ToolUtil::print_info(ret, "head bucket %s", bucket_name);

  return ret;
}

int cmd_put_object(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();
  const char* object_name = param[1].c_str();
  int64_t owner_id = strtoll(param[2].c_str(), NULL, 10);
  int ret = 0;
  ToolUtil::print_info(ret, "put object: %s, object: %s owner_id: %"PRI64_PREFIX"d",
      bucket_name, object_name, owner_id);
  UserInfo user_info;
  user_info.owner_id_ = owner_id;
  ObjectInfo object_info;

  ret = tfs::client::KvMetaHelper::do_put_object(new_server_id, bucket_name, object_name, object_info, user_info);
  //ret = g_kv_meta_client.put_object(bucket_name, object_name, local_file, user_info);
  ToolUtil::print_info(ret, "put object: %s, object: %s owner_id: %"PRI64_PREFIX"d",
      bucket_name, object_name, owner_id);
  return ret;
}

int cmd_get_object(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();
  const char* object_name = param[1].c_str();
  int64_t offset = strtoll(param[2].c_str(), NULL, 10);
  int64_t length = strtoll(param[3].c_str(), NULL, 10);
  bool still_have = false;
  int64_t read_size = 0;
  UserInfo user_info;
  ObjectInfo object_info;
  int ret = 0;
  do
  {

    ret = tfs::client::KvMetaHelper::do_get_object(new_server_id, bucket_name, object_name,
                                                 offset, length,
                                                 &object_info, &still_have, user_info);
    if (read_size == 0)
    {
      cout << "ObjectMetaInfo" << endl;
      cout << "create_time_:" << object_info.meta_info_.create_time_ << endl;
      cout << "modify_time_:" << object_info.meta_info_.modify_time_ << endl;
      cout << "big_file_size_:" << object_info.meta_info_.big_file_size_ << endl;
      cout << "max_tfs_file_size_:" << object_info.meta_info_.max_tfs_file_size_ << endl;
      cout << "owner_id_:" << object_info.meta_info_.owner_id_ << endl;
    }
    for(size_t j = 0; j < object_info.v_tfs_file_info_.size(); ++j)
    {
      if (object_info.v_tfs_file_info_[j].offset_ < offset)
      {
        continue;
      }

      cout << "cluster_id_:" << object_info.v_tfs_file_info_[j].cluster_id_ << " ";
      cout << "block_id_:" << object_info.v_tfs_file_info_[j].block_id_ << " ";
      cout << "file_id_:" << object_info.v_tfs_file_info_[j].file_id_ << " ";
      cout << "offset_:" << object_info.v_tfs_file_info_[j].offset_ << " ";
      cout << "file_size_:" << object_info.v_tfs_file_info_[j].file_size_ << endl;
      read_size += object_info.v_tfs_file_info_[j].file_size_;
    }
    offset += read_size;
    length -= read_size;
  }while(still_have && length > 0);

  ToolUtil::print_info(ret, "get object: %s, object: %s",
      bucket_name, object_name);

  return ret;
}

int cmd_del_object(const VSTRING& param)
{
  int ret = 0;
  const char* bucket_name = param[0].c_str();
  const char* object_name = param[1].c_str();
  bool still_have = false;
  UserInfo user_info;
  ObjectInfo object_info;
  do
  {
    ret = tfs::client::KvMetaHelper::do_del_object(new_server_id, bucket_name, object_name, &object_info, &still_have, user_info);
    cout << "vector_size" << object_info.v_tfs_file_info_.size() << endl;
  }while(still_have);
  //int ret = g_kv_meta_client.del_object(bucket_name, object_name, user_info);
  ToolUtil::print_info(ret, "del bucket: %s, object: %s", bucket_name, object_name);

  return ret;
}

int cmd_head_object(const VSTRING& param)
{
  const char* bucket_name = param[0].c_str();
  const char* object_name = param[1].c_str();

  ObjectInfo object_info;
  UserInfo user_info;
  int ret = tfs::client::KvMetaHelper::do_head_object(new_server_id,bucket_name, object_name, &object_info, user_info);
  //int ret = g_kv_meta_client.head_object(bucket_name, object_name, &object_info, user_info);

  if (TFS_SUCCESS == ret)
  {
    printf("create_time: %"PRI64_PREFIX"d, modify_time: %"PRI64_PREFIX"d, total_size: %"PRI64_PREFIX"d, owner_id: %"PRI64_PREFIX"d \n",
        object_info.meta_info_.create_time_, object_info.meta_info_.modify_time_, object_info.meta_info_.big_file_size_, object_info.meta_info_.owner_id_);
  }
  ToolUtil::print_info(ret, "head bucket: %s, object: %s", bucket_name, object_name);

  return ret;
}

