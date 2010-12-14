/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *
 */
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <vector>
#include <string>
#include <map>

#include "common/config.h"
#include "client/fsname.h"
#include "message/client_pool.h"
#include "common/config_item.h"
#include "client/tfs_client_api.h"

using namespace std;
using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;

static const int32_t CMD_MAX_LEN = 4096;
static int32_t cluster_id = 0x01;
static string dev_name = "eth0";
static string nsip;
static uint64_t local_server_ip;
static const int TFS_CLIENT_QUIT = 0xfff1234;

typedef vector<string> VEC_STRING;
typedef int (*cmd_function)(TfsClient*, VEC_STRING&);
typedef map<string, cmd_function> STR_FUNC_MAP;
typedef STR_FUNC_MAP::iterator STR_FUNC_MAP_ITER;
static STR_FUNC_MAP cmd_map;

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
    it = cmd_map.begin();
    len = strlen(text);
  }

  while(it != cmd_map.end())
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

char** tfscmd_completion (const char* text, int start, int end)
{
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

int main_loop(TfsClient* tfs_client);
int do_cmd(char* buffer, TfsClient* tfs_client);
int send_message_to_server(uint64_t server_id, Message* ds_message, string& err_msg, Message** retmessage = NULL);

int put_file_ex(TfsClient* tfs_client, VEC_STRING& param, int32_t unique);
int remove_file_ex(TfsClient* tfs_client, VEC_STRING& param, int32_t unique);

// for uniformity, some argument is just mock
int cmd_cd(TfsClient* tfs_client, VEC_STRING& param);
int cmd_ls(TfsClient* tfs_client, VEC_STRING& param);
int cmd_pwd(TfsClient* tfs_client, VEC_STRING& param);
int cmd_show_help(TfsClient* tfs_client, VEC_STRING& param);
int cmd_quit_end(TfsClient* tfs_client, VEC_STRING& param);
int cmd_put_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_uput_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_get_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_remove_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_uremove_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_undel_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_hide_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_rename_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_stat_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_stat_blk(TfsClient* tfs_client, VEC_STRING& param);
int cmd_visit_count_blk(TfsClient* tfs_client, VEC_STRING& param);
int cmd_list_file_info(TfsClient* tfs_client, VEC_STRING& param);
int cmd_batch_file(TfsClient* tfs_client, VEC_STRING& param);
int cmd_unexpire_blk(TfsClient* tfs_client, VEC_STRING& param);
int cmd_expire_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_compact_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_new_file_name(TfsClient* tfs_client, VEC_STRING& param);
int cmd_repair_lose_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_repair_group_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_set_run_param(TfsClient* tfs_client, VEC_STRING& param);
int cmd_unload_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_add_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_check_file_info(TfsClient* tfs_client, VEC_STRING& param);
int cmd_repair_crc(TfsClient* tfs_client, VEC_STRING& param);
int cmd_list_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_remove_block(TfsClient* tfs_client, VEC_STRING& param);
int cmd_get_repl_info(TfsClient* tfs_client, VEC_STRING& param);
int cmd_clear_repl_info(TfsClient* tfs_client, VEC_STRING& param);
int cmd_access_stat_info(TfsClient* tfs_client, VEC_STRING& param);
int cmd_access_control_flag(TfsClient* tfs_client, VEC_STRING& param);
int cmd_get_scale_image(TfsClient* tfs_client, VEC_STRING &param);

void init()
{
  cmd_map["help"] = cmd_show_help;
  cmd_map["quit"] = cmd_quit_end;
  cmd_map["exit"] = cmd_quit_end;
  cmd_map["cd"] = cmd_cd;
  cmd_map["ls"] = cmd_ls;
  cmd_map["pwd"] = cmd_pwd;
  cmd_map["put"] = cmd_put_file;
  // cmd_map["uput"] = cmd_uput_file;
  cmd_map["get"] = cmd_get_file;
  cmd_map["rm"] = cmd_remove_file;
  // cmd_map["urm"] = cmd_uremove_file;
  cmd_map["undel"] = cmd_undel_file;
  cmd_map["rename"] = cmd_rename_file;
  cmd_map["stat"] = cmd_stat_file;
  cmd_map["statblk"] = cmd_stat_blk;
  cmd_map["vcblk"] = cmd_visit_count_blk;
  cmd_map["batch"] = cmd_batch_file;
  cmd_map["@"] = cmd_batch_file;
  cmd_map["expblk"] = cmd_expire_block;
  cmd_map["ueblk"] = cmd_unexpire_blk;
  cmd_map["compact"] = cmd_compact_block;
  cmd_map["newfilename"] = cmd_new_file_name;
  cmd_map["repairblk"] = cmd_repair_lose_block;
  cmd_map["repairgrp"] = cmd_repair_group_block;
  cmd_map["param"] = cmd_set_run_param;
  cmd_map["unloadblk"] = cmd_unload_block;
  cmd_map["lsf"] = cmd_list_file_info;
  cmd_map["addblk"] = cmd_add_block;
  cmd_map["cfi"] = cmd_check_file_info;
  cmd_map["repaircrc"] = cmd_repair_crc;
  cmd_map["listblock"] = cmd_list_block;
  cmd_map["removeblock"] = cmd_remove_block;
  cmd_map["hide"] = cmd_hide_file;
  cmd_map["getri"] = cmd_get_repl_info;
  cmd_map["clri"] = cmd_clear_repl_info;
  cmd_map["aci"] = cmd_access_stat_info;
  cmd_map["setacl"] = cmd_access_control_flag;
  cmd_map["getx"] = cmd_get_scale_image;
}

// transfer "ip:port" to server_id (64bit)
uint64_t trans_to_server_id(char* ipport)
{
  char* ip = ipport;
  char* port_str = strchr(ip, ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "ip:port, format error! \n\n");
    return 0;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  return Func::str_to_addr(ip, port);
}

// expand ~ to HOME. modify argument
const char* expand_path(string& path)
{
  if (path.size() > 0 && '~' == path.at(0) &&
      (1 == path.size() ||                      // just one ~
       (path.size() > 1 && '/' == path.at(1)))) // like ~/xxx
  {
    char* home_path = getenv("HOME");
    if (!home_path)
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

static void usage(const char* name)
{
  fprintf(stderr, "Usage: %s -s -c [-d] [-n] [-i] [-h]\n", name);
  fprintf(stderr, "       -s nameserver ip port\n");
  fprintf(stderr, "       -c nameserver cluster id\n");
  fprintf(stderr, "       -d network device\n");
  fprintf(stderr, "       -n set log level\n");
  fprintf(stderr, "       -i directly execute the command\n");
  fprintf(stderr, "       -h help\n");
  exit(TFS_ERROR);
}

static void sign_handler(int32_t sig)
{
  switch (sig)
  {
  case SIGINT:
  case SIGTERM:
    fprintf(stderr, "\nTFS> ");
    break;
  }
}

int main(int argc, char* argv[])
{
  int32_t i;
  bool directly = false;
  bool set_log_level = false;
  // analyze arguments
  while ((i = getopt(argc, argv, "s:c:d:nih")) != EOF)
  {
    switch (i)
    {
    case 'n':
      set_log_level = true;
      break;
    case 's':
      nsip = optarg;
      break;
    case 'd':
      dev_name = optarg;
      break;
    case 'c':
      cluster_id = atoi(optarg);
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

  if ((nsip.empty())
      || (nsip.compare(" ") == 0)
      || (cluster_id <= 0)
      || (cluster_id > 9))
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }

  init();

  TfsClient tfs_client;
  int iret = tfs_client.initialize(nsip);
  if (iret != TFS_SUCCESS)
  {
    return TFS_ERROR;
  }

  local_server_ip = tbsys::CNetUtil::getLocalAddr(dev_name.c_str());
  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop(&tfs_client);
  }
  else // has other params
  {
    int32_t i = 0;
    VEC_STRING param;
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        param.clear();
        do_cmd(argv[i], &tfs_client);
      }
    }
    else
    {
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
        cmd_batch_file(&tfs_client, param);
      }
    }
  }
  return TFS_SUCCESS;
}

int32_t do_cmd(char* key, TfsClient* tfs_client)
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
  STR_FUNC_MAP_ITER it = cmd_map.find(Func::str_to_lower(key));

  if (it == cmd_map.end())
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

  VEC_STRING param;
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if ('\0' == token[0])
    {
      continue;
    }
    param.push_back(token);
  }

  return it->second(tfs_client, param);
}

int main_loop(TfsClient* tfs_client)
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = tfscmd_completion;
#else
  char cmd_line[CMD_MAX_LEN];
#endif

  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("TFS> ");
    if (!cmd_line)
#else
      fprintf(stderr, "TFS> ");
    if (NULL == fgets(cmd_line, CMD_MAX_LEN, stdin))
#endif
    {
      continue;
    }
    ret = do_cmd(cmd_line, tfs_client);
#ifdef _WITH_READ_LINE
    delete cmd_line;
    cmd_line = NULL;
#endif
    if (TFS_CLIENT_QUIT == ret)
    {
      break;
    }
  }
  return TFS_SUCCESS;
}

int cmd_quit_end(TfsClient*, VEC_STRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_show_help(TfsClient*, VEC_STRING&)
{
  fprintf(stderr, "\nsupported command"
          "\nexit|quit                      quit console"
          "\nls [path]                      list path or current directory"
          "\ncd [dir]                       change work directory to dir or HOME"
          "\npwd                            print current work directory"
          "\nput local_file [tfs_file]      upload a local file to tfs"
          "\nget tfs_file local_file        download a remote file to local disk"
          "\nrm tfs_file                    delete a remote file"
          "\nrename from_file to_file       rename a remote file"
          "\nnewfilename                    generate a new tfs file name"

          "\nstat tfs_file                  get the file info from tfs"
          "\nstatblk                        get the block info"
          "\nlsf                            list the files in the block"
          "\ncfi                            check a remote file"
          "\nvcblk                          list the most used blocks"

          "\n@|batch                        batch handle cmds\n"

          "\ncompact                        [Attention!] force to compact a block"
          "\nparam                          [Attention!] set the params"
          "\nrepairblk                      repair the back block"
          "\nrepairgrp                      repair the group of a block"
          "\nremoveblock                    [Attention!] remove a block(includes the meta infos)"
          "\nlistblock                      list the dataservers the block allocated on"

          "\nhelp                           show help info\n\n");
  return TFS_SUCCESS;
}

int cmd_ls(TfsClient*, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size > 1)
  {
    fprintf(stderr, "ls [path]\n\n");
    return TFS_ERROR;
  }
  const char* path = (1 == size) ? param.at(0).c_str() : ".";
  char sys_cmd[1024];
  // just use system tool ls, maybe DIY
  sprintf(sys_cmd, "ls -FCl %s", path);
  return system(sys_cmd);
}

int cmd_pwd(TfsClient*, VEC_STRING&)
{
  char dir[MAX_PATH_LENGTH], *path;

  path = getcwd(dir, MAX_PATH_LENGTH - 1);
  if (!path)
  {
    fprintf (stderr, "can't get current work directory: %s\n", strerror(errno));
    return TFS_ERROR;
  }
  fprintf(stderr, "%s\n", dir);
  return TFS_SUCCESS;
}

int cmd_cd(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size > 1)
  {
    fprintf(stderr, "cd [dir]\n\n");
    return TFS_ERROR;
  }
  const char* dest_dir = (1 == size) ? expand_path(param[0]) : getenv("HOME");
  if (!dest_dir)
  {
    fprintf(stderr, "no directory argument and HOME not found\n\n");
    return TFS_ERROR;
  }

  if (chdir(dest_dir) == -1)
  {
    fprintf(stderr, "can't change directory %s: %s\n", dest_dir, strerror(errno));
    return TFS_ERROR;
  }
  cmd_pwd(tfs_client, param);
  return TFS_SUCCESS;
}

int put_file_ex(TfsClient* tfs_client, VEC_STRING& param, int unique)
{
  int32_t size = param.size();
  if (size == 0 || size > 3)
  {
    fprintf(stderr, "put local_file [tfs_name] [suffix]\n\n");
    return TFS_ERROR;
  }
  char* local_file = const_cast<char*> (expand_path(param[0]));
  char* tfs_name = NULL;
  char* prefix = NULL;
  if (size > 1)
  {
    tfs_name = const_cast<char*> (param[1].c_str());
  }

  if (size > 2)
  {
    prefix = const_cast<char*> (param[2].c_str());
  }

  if (tfs_name != NULL)
  {
    if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
    {
      if (strcasecmp(tfs_name, "null") == 0)
      {
        tfs_name = NULL;
      }
      else
      {
        fprintf(stderr, "file name error: %s\n", tfs_name);
        return TFS_ERROR;
      }
    }
    else if (prefix == NULL)
    {
      prefix = tfs_name + FILE_NAME_LEN;
    }
  }
  int ret = TFS_ERROR;
  char* name, new_tfs_name[MAX_FILE_NAME_LEN];
  if (unique)
  {
    ret = TFS_SUCCESS;
    // ret = tfs_client->save_unique_file(local_file, tfs_name, prefix);
  }
  else
  {
    ret = tfs_client->save_file(local_file, tfs_name, prefix);
  }
  if (ret == TFS_ERROR)
  {
    fprintf(stderr, "close tfs_client fail: %s\n", tfs_client->get_error_message());
  }
  else
  {
    name = unique ? new_tfs_name : const_cast<char*>(tfs_client->get_file_name());
    FSName fs_name(name, NULL, cluster_id);
    fprintf(stdout, "put %s => %s, %u:%" PRI64_PREFIX "u\n", local_file, name, fs_name.get_block_id(), fs_name.get_file_id());
  }
  return ret;
}

int cmd_put_file(TfsClient* tfs_client, VEC_STRING& param)
{
  return put_file_ex(tfs_client, param, 0);
}

int cmd_uput_file(TfsClient* tfs_client, VEC_STRING& param)
{
  return put_file_ex(tfs_client, param, 1);
}

int cmd_get_file(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "get tfs_name local_file\n\n");
    return TFS_ERROR;
  }

  char* tfs_name = const_cast<char*> (param[0].c_str());
  char* prefix = NULL;
  char local_file[256];
  sprintf(local_file, "%s", expand_path(param[1]));
  struct stat stat_buf;
  if (stat(local_file, &stat_buf) == 0 && S_ISDIR(stat_buf.st_mode))
  {
    sprintf(local_file, "%s/%s", param[1].c_str(), tfs_name);
  }
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  prefix = tfs_name + FILE_NAME_LEN;

  int32_t fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (-1 == fd)
  {
    fprintf(stderr, "open local file fail: %s\n", local_file);
    return TFS_ERROR;
  }

  if (tfs_client->tfs_open(tfs_name, prefix, READ_MODE) != TFS_SUCCESS)
  {
    fprintf(stderr, "open tfs_client fail: %s\n", tfs_client->get_error_message());
    close(fd);
    return TFS_ERROR;
  }

  FileInfo file_info;
  if (tfs_client->tfs_stat(&file_info) == TFS_ERROR)
  {
    tfs_client->tfs_close();
    close(fd);
    fprintf(stderr, "fstat tfs_client fail: %s\n", tfs_client->get_error_message());
    return TFS_ERROR;
  }

  char data[MAX_READ_SIZE];
  uint32_t crc = 0;
  int32_t total_size = 0;
  for (;;)
  {
    int32_t read_len = tfs_client->tfs_read_v2(data, MAX_READ_SIZE, &file_info);
    if (read_len < 0)
    {
      fprintf(stderr, "read tfs_client fail: %s\n", tfs_client->get_error_message());
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
    }
    if (read_len == 0)
    {
      break;
    }

    if (write(fd, data, read_len) != read_len)
    {
      fprintf(stderr, "write local file fail.\n");
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
    }
    crc = Func::crc(crc, data, read_len);
    total_size += read_len;
    if (tfs_client->is_eof())
    {
      break;
    }
    /*
      int32_t read_len = tfs_client->tfs_read(data, MAX_READ_SIZE);
      if (read_len < 0) {
      fprintf(stderr, "read tfs_client fail: %s\n", tfs_client->get_error_message());
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
      }
      if (read_len == 0) break;
      if (write(fd, data, read_len) != read_len) {
      fprintf(stderr, "write local file fail.\n");
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
      }
      crc = Func::crc(crc, data, read_len);
      total_size += read_len;
      if (read_len != MAX_READ_SIZE) break;
    */
  }

  tfs_client->tfs_close();
  close(fd);
  if (crc != file_info.crc_ || total_size != file_info.size_)
  {
    fprintf(stderr, "%s, crc error: %u <> %u, size: %u <> %u\n", tbsys::CNetUtil::addrToString(
              tfs_client->get_last_elect_ds_id()).c_str(), crc, file_info.crc_, total_size, file_info.size_);
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_get_scale_image(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size < 2)
  {
    fprintf(stderr, "get tfs_name local_file width height\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  char* prefix = NULL;

  char local_file[256];
  sprintf(local_file, "%s", expand_path(param[1]));
  struct stat stat_buf;
  if (stat(local_file, &stat_buf) == 0 && S_ISDIR(stat_buf.st_mode))
  {
    sprintf(local_file, "%s/%s", param[1].c_str(), tfs_name);
  }
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  prefix = tfs_name + FILE_NAME_LEN;

  int32_t width = 0;
  int32_t height = 0;
  if (size > 2)
  {
    width = atoi(param[2].c_str());
  }
  if (size > 3)
  {
    height = atoi(param[3].c_str());
  }

  int32_t fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd == -1)
  {
    fprintf(stderr, "open local file fail: %s\n", local_file);
    return TFS_ERROR;
  }

  if (tfs_client->tfs_open(tfs_name, prefix, READ_MODE) != TFS_SUCCESS)
  {
    fprintf(stderr, "open tfs_client fail: %s\n", tfs_client->get_error_message());
    close(fd);
    return TFS_ERROR;
  }

  FileInfo file_info;
  char data[MAX_READ_SIZE];
  uint32_t crc = 0;
  int32_t total_size = 0;
  for (;;)
  {
    int32_t read_len = tfs_client->tfs_read_scale_image(data, MAX_READ_SIZE, width, height, &file_info);
    if (read_len < 0)
    {
      fprintf(stderr, "read tfs_client fail: %s\n", tfs_client->get_error_message());
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
    }
    if (read_len == 0)
      break;

    if (write(fd, data, read_len) != read_len)
    {
      fprintf(stderr, "write local file fail.\n");
      tfs_client->tfs_close();
      close(fd);
      return TFS_ERROR;
    }
    crc = Func::crc(crc, data, read_len);
    total_size += read_len;
    if (tfs_client->is_eof())
    {
      fprintf(stdout, "read complete, size:%d,usize:%d\n", file_info.size_, file_info.usize_);
      break;
    }
  }
  tfs_client->tfs_close();
  close(fd);
  if (crc != file_info.crc_ || total_size != file_info.size_)
  {
    fprintf(stderr, "%s, crc error: %u <> %u, size: %u <> %u\n", tbsys::CNetUtil::addrToString(
              tfs_client->get_last_elect_ds_id()).c_str(), crc, file_info.crc_, total_size, file_info.size_);
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int remove_file_ex(TfsClient* tfs_client, VEC_STRING& param, int unique)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "rm tfs_name\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  printf("try to remove file: %s\n", tfs_name);
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  char* prefix = tfs_name + FILE_NAME_LEN;
  int32_t ret = TFS_ERROR;
  if (unique)
  {
    ret = TFS_SUCCESS;
    // ret = (tfs_client->unlink_unique_file(tfs_name, prefix) < 0) ? TFS_ERROR : TFS_SUCCESS;
  }
  else
  {
    ret = tfs_client->unlink(tfs_name, prefix);
  }

  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "failed: %s\n", tfs_client->get_error_message());
  }
  else
  {
    printf("file deleted: %s\n", tfs_name);
  }

  return TFS_SUCCESS;
}

int cmd_remove_file(TfsClient* tfs_client, VEC_STRING& param)
{
  return remove_file_ex(tfs_client, param, 0);
}

int cmd_uremove_file(TfsClient* tfs_client, VEC_STRING& param)
{
  return remove_file_ex(tfs_client, param, 1);
}

int cmd_undel_file(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "undel tfs_name\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  char* prefix = tfs_name + FILE_NAME_LEN;
  if (tfs_client->unlink(tfs_name, prefix, 2) != TFS_SUCCESS)
  {
    fprintf(stderr, "undelete file failed: %s\n", tfs_client->get_error_message());
  }

  return TFS_SUCCESS;
}

int cmd_hide_file(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size < 1)
  {
    fprintf(stderr, "hide tfs_name [mode]\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  int32_t hide_opt = 4;
  if (param.size() > 1)
  {
    hide_opt = atoi(param[1].c_str());
  }
  char* prefix = tfs_name + FILE_NAME_LEN;
  if (tfs_client->unlink(tfs_name, prefix, hide_opt) != TFS_SUCCESS)
  {
    fprintf(stderr, "hide failed: %s\n", tfs_client->get_error_message());
  }

  return TFS_SUCCESS;
}

int cmd_rename_file(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "rename tfs_name new_name\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  char* prefix = tfs_name + FILE_NAME_LEN;
  char* new_tfs_name = const_cast<char*> (param[1].c_str());
  if (static_cast<int32_t> (strlen(new_tfs_name)) < FILE_NAME_LEN || new_tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", new_tfs_name);
    return TFS_ERROR;
  }
  char* newprefix = new_tfs_name + FILE_NAME_LEN;
  FSName t1(tfs_name, NULL, cluster_id);
  FSName t2(new_tfs_name, NULL, cluster_id);
  if (t1.get_block_id() != t2.get_block_id())
  {
    fprintf(stderr, "new file must be in the same block with the old file\n");
    return TFS_ERROR;
  }
  if (strcmp(prefix, newprefix) == 0)
  {
    fprintf(stderr, "failed.\n");
    return TFS_ERROR;
  }
  if (tfs_client->rename(tfs_name, prefix, newprefix) != TFS_SUCCESS)
  {
    fprintf(stderr, "rename failed: %s\n", tfs_client->get_error_message());
  }

  return TFS_SUCCESS;
}

int cmd_stat_file(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "stat tfs_name\n\n");
    return TFS_ERROR;
  }
  char* tfs_name = const_cast<char*> (param[0].c_str());
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }
  char* prefix = tfs_name + FILE_NAME_LEN;

  if (tfs_client->tfs_open(tfs_name, prefix, READ_MODE) != TFS_SUCCESS)
  {
    fprintf(stderr, "open tfs_client fail: %s\n", tfs_client->get_error_message());
    return TFS_ERROR;
  }

  FileInfo file_info;
  if (tfs_client->tfs_stat(&file_info, 1) != TFS_SUCCESS)
  {
    tfs_client->tfs_close();
    fprintf(stderr, "fstat tfs_client fail: %s\n", tfs_client->get_error_message());
    return TFS_ERROR;
  }
  tfs_client->tfs_close();

  FSName fs_name(tfs_name, NULL, cluster_id);
  fs_name.set_file_id(file_info.id_);
  printf("  FILE_NAME:     %s\n", fs_name.get_name());
  printf("  BLOCK_ID:      %u\n", fs_name.get_block_id());
  printf("  FILE_ID:       %" PRI64_PREFIX "u\n", file_info.id_);
  printf("  OFFSET:        %d\n", file_info.offset_);
  printf("  SIZE:          %d\n", file_info.size_);
  printf("  MODIFIED_TIME: %s\n", Func::time_to_str(file_info.modify_time_).c_str());
  printf("  CREATE_TIME:   %s\n", Func::time_to_str(file_info.create_time_).c_str());
  printf("  STATUS:        %d\n", file_info.flag_);
  printf("  CRC:           %u\n", file_info.crc_);
  uint64_t id = file_info.size_;
  id <<= 32;
  id |= file_info.crc_;
  //  uint32_t tindex = static_cast<uint32_t>(id & 0xFFFFFFFF);
  //  printf("  TABLE:         select * from t%d where id='%" PRI64_PREFIX "u'\n", tindex % TABLE_COUNT, id);

  return TFS_SUCCESS;
}

int cmd_batch_file(TfsClient* tfs_client, VEC_STRING& param)
{
  if (static_cast<int32_t> (param.size()) != 1)
  {
    fprintf(stderr, "batch file_name\n\n");
    return TFS_ERROR;
  }

  char* batch_file = const_cast<char*> (param[0].c_str());
  FILE* fp = fopen(batch_file, "rb");
  if (fp == NULL)
  {
    fprintf(stderr, "open file error: %s\n\n", batch_file);
    return TFS_ERROR;
  }

  int32_t error_count = 0;
  int32_t count = 0;
  VEC_STRING params;
  char buffer[CMD_MAX_LEN];
  while (fgets(buffer, CMD_MAX_LEN, fp))
  {
    if (do_cmd(buffer, tfs_client) == TFS_ERROR)
    {
      error_count++;
    }
    if (++count % 100 == 0)
    {
      fprintf(stderr, "tatol: %d, %d errors.\r", count, error_count);
      fflush( stderr);
    }
  }
  fprintf(stderr, "tatol: %d, %d errors.\n\n", count, error_count);
  fclose(fp);

  return TFS_SUCCESS;
}

int send_message_to_server(uint64_t server_id, Message* ds_message, string& err_msg, Message** ret_message)
{
  Client* client = CLIENT_POOL.get_client(server_id);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }
  int32_t ret_status = TFS_ERROR;
  Message* message = client->call(ds_message);
  if (message != NULL)
  {
    if (ret_message == NULL)
    {
      if (message->get_message_type() == STATUS_MESSAGE)
      {
        StatusMessage* s_msg = (StatusMessage*) message;
        if (STATUS_MESSAGE_OK == s_msg->get_status())
        {
          ret_status = TFS_SUCCESS;
        }
        if (s_msg->get_error() != NULL)
        {
          err_msg = s_msg->get_error();
        }
      }
      delete message;
    }
    else
    {
      (*ret_message) = message;
    }
  }
  client->disconnect();
  CLIENT_POOL.release_client(client);
  return (ret_status);
}

int cmd_unexpire_blk(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "ueblk ip:port block_id\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (port_str == NULL)
  {
    fprintf(stderr, "ip:port format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  uint32_t block_id = strtoul(param[1].c_str(), reinterpret_cast<char**> (NULL), 10);
  uint64_t nsip_port = tfs_client->get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_LOADBLK);
  req_cc_msg.set_server_id(server_id);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_add_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "addblk block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
  if (0 == block_id)
  {
    fprintf(stderr, "block_id: %u\n\n", block_id);
    return TFS_ERROR;
  }
  VUINT64 ds_list;
  VUINT64 fail_server;
  if (tfs_client->create_block_info(block_id, ds_list, 1, fail_server) != TFS_SUCCESS)
  {
    if (ds_list.size() == 0)
    {
      fprintf(stderr, "create fail block_id: %u\n\n", block_id);
      return TFS_ERROR;
    }
  }

  return TFS_SUCCESS;
}

int cmd_expire_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "expblk ip:port block_id\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "ip:port format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  uint32_t block_id = strtoul(param[1].c_str(), reinterpret_cast<char**> (NULL), 10);
  fprintf(stderr, "block_id: %u\n", block_id);

  uint64_t nsip_port = tfs_client->get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_server_id(server_id);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_compact_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "compact block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
  uint64_t nsip_port = tfs_client->get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_COMPACT);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_repair_lose_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  int32_t action = 2;
  if (size != 1 && size != 4)
  {
    fprintf(stderr, "repairblk block_id [source] [dest] [action]\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), (char**) NULL, 10);
  uint64_t src_server_id = 0;
  uint64_t dest_server_id = 0;
  if (4 == size)
  {
    src_server_id = trans_to_server_id(const_cast<char*> (param[1].c_str()));
    dest_server_id = trans_to_server_id(const_cast<char*> (param[2].c_str()));
    action = atoi(param[3].c_str());
  }

  uint64_t nsip_port = tfs_client->get_ns_ip_port();
  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_IMMEDIATELY_REPL);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(action);
  req_cc_msg.set_from_server_id(src_server_id);
  req_cc_msg.set_server_id(dest_server_id);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_repair_group_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "repairgrp block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), (char**) NULL, 10);
  uint64_t nsip_port = tfs_client->get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_REPAIR_GROUP);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_new_file_name(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size > 1)
  {
    fprintf(stderr, "newfilename [suffix]\n\n");
    return TFS_ERROR;
  }
  char* prefix = NULL;
  if (param.size() > 0)
  {
    prefix = const_cast<char*> (param[0].c_str());
  }

  if (tfs_client->new_filename() == TFS_SUCCESS)
  {
    const char* tfs_name = tfs_client->get_file_name();
    FSName fs_name(tfs_name, NULL, cluster_id);
    fprintf(stderr, "Name:%s, BlockId:%u, FileId:%" PRI64_PREFIX "u\n\n", tfs_name, fs_name.get_block_id(), fs_name.get_file_id());
  }
  else
  {
    fprintf(stderr, "ERROR: %s\n\n", tfs_client->get_error_message());
  }

  return TFS_SUCCESS;
}

int cmd_set_run_param(TfsClient* tfs_client, VEC_STRING& param)
{
  const char* param_str[] =
    {
      "minReplication",
      "maxReplication",
      "maxWriteFileCount",
      "maxUseCapacityRatio",
      "dsDeadTime",
      "heartInterval",
      "replCheckInterval",
      "balanceCheckInterval",
      "pauseReplication",
      "replWaitTime",
      "replicateMaxTime",
      "replicateMaxCountPerServer",
      "redundantCheckInterval",
      "compactTimeLower",
      "compactTimeUpper",
      "compactDeleteRatio",
      "compactMaxLoad",
      "compactPreserverTime",
      "compactCheckInterval",
      "clusterIndex"
    };
  int32_t i, param_strlen = sizeof(param_str) / sizeof(char*);
  int32_t size = param.size();
  if (size != 1 && size != 3 && size != 4)
  {
    fprintf(stderr, "param param_name\n\n");
    for (i = 0; i < param_strlen; i++)
    {
      fprintf(stderr, "%s\n", param_str[i]);
    }
    return TFS_ERROR;
  }
  char* param_name = const_cast<char*> (param[0].c_str());
  uint32_t index = 0;
  for (i = 0; i < param_strlen; i++)
  {
    if (strcmp(param_name, param_str[i]) == 0)
    {
      index = i + 1;
      break;
    }
  }
  if (0 == index)
  {
    fprintf(stderr, "param %s not valid\n", param_name);
    return TFS_ERROR;
  }
  uint64_t value = 0;
  if (3 == size || 4 == size)
  {
    if (strcmp("set", param[1].c_str()))
    {
      fprintf(stderr, "param %s set value\n\n", param_name);
      return TFS_ERROR;
    }
    index |= 0x10000000;
    value = atoi(param[2].c_str());
    if (4 == size)
    {
      value <<= 32;
      value |= atoi(param[3].c_str());
    }
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_block_id(index);
  req_cc_msg.set_server_id(value);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;

  uint64_t nsip_port = tfs_client->get_ns_ip_port();
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }
  fprintf(stderr, "%s\n\n", err_msg.c_str());

  return TFS_SUCCESS;
}

int cmd_unload_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "unloadblk ip:port block_id\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "ip:port ,format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  uint32_t block_id = strtoul(param[1].c_str(), reinterpret_cast<char**> (NULL), 10);

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_UNLOADBLK);
  req_cc_msg.set_server_id(server_id);
  req_cc_msg.set_block_id(block_id);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(server_id, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_stat_blk(TfsClient*, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "statblk ip:port block_id\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (port_str == NULL)
  {
    fprintf(stderr, "ip:port ,format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  uint32_t block_id = strtoul(param[1].c_str(), reinterpret_cast<char**> (NULL), 10);

  GetBlockInfoMessage req_gbi_msg;
  req_gbi_msg.set_block_id(block_id);
  string err_msg;
  Message* ret_message = NULL;
  send_message_to_server(server_id, &req_gbi_msg, err_msg, &ret_message);
  if (ret_message == NULL)
  {
    return TFS_ERROR;
  }
  if (ret_message->get_message_type() == UPDATE_BLOCK_INFO_MESSAGE)
  {
    UpdateBlockInfoMessage* ubiMessage = (UpdateBlockInfoMessage*) ret_message;
    SdbmStat* dbstat = ubiMessage->get_db_stat();
    if (block_id != 0)
    {
      BlockInfo* bi = ubiMessage->get_block();
      printf("ID:            %u\n", bi->block_id_);
      printf("VERSION:       %u\n", bi->version_);
      printf("FILE_COUNT:    %d\n", bi->file_count_);
      printf("SIZE:          %d\n", bi->size_);
      printf("DELFILE_COUNT: %d\n", bi->del_file_count_);
      printf("DEL_SIZE:      %d\n", bi->del_size_);
      printf("SEQNO:         %d\n", bi->seq_no_);
      printf("VISITCOUNT:    %d\n", ubiMessage->get_repair());
      int32_t value = static_cast<int32_t> (ubiMessage->get_server_id());
      printf("INFO_LOADED:   %d%s\n", value, (value == 1 ? " (ERR)" : ""));
    }
    else if (dbstat)
    {
      printf("CACHE_HIT:     %d%%\n", 100 * (dbstat->fetch_count_ - dbstat->miss_fetch_count_) / (dbstat->fetch_count_
                                                                                                  + 1));
      printf("FETCH_COUNT:   %d\n", dbstat->fetch_count_);
      printf("MISFETCH_COUNT:%d\n", dbstat->miss_fetch_count_);
      printf("STORE_COUNT:   %d\n", dbstat->store_count_);
      printf("DELETE_COUNT:  %d\n", dbstat->delete_count_);
      printf("OVERFLOW:      %d\n", dbstat->overflow_count_);
      printf("ITEM_COUNT:    %d\n", dbstat->item_count_);
    }
  }
  else if (ret_message->get_message_type() == STATUS_MESSAGE)
  {
    StatusMessage* s_msg = reinterpret_cast<StatusMessage*> (ret_message);
    if (s_msg->get_error() != NULL)
    {
      printf("%s\n", s_msg->get_error());
    }
  }
  delete ret_message;

  return TFS_SUCCESS;
}

int cmd_visit_count_blk(TfsClient*, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 2)
  {
    fprintf(stderr, "vcblk ip:port count\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "ip:port ,format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  int32_t count = atoi(param[1].c_str());

  GetServerStatusMessage req_gss_msg;
  req_gss_msg.set_status_type(GSS_MAX_VISIT_COUNT);
  req_gss_msg.set_return_row(count);
  string err_msg;
  Message* ret_message = NULL;
  send_message_to_server(server_id, &req_gss_msg, err_msg, &ret_message);
  if (NULL == ret_message)
  {
    return TFS_ERROR;
  }
  if (CARRY_BLOCK_MESSAGE == ret_message->get_message_type())
  {
    CarryBlockMessage* req_cb_msg = (CarryBlockMessage*) ret_message;
    const VUINT32* ids = req_cb_msg->get_expire_blocks();
    const VUINT32* vcs = req_cb_msg->get_new_blocks();
    if (ids->size() != vcs->size())
    {
      delete ret_message;
      return TFS_ERROR;
    }
    for (int32_t i = 0; i < static_cast<int32_t> (ids->size()); i++)
    {
      printf("Block: %u => %d\n", ids->at(i), vcs->at(i));
    }
  }
  else if (ret_message->get_message_type() == STATUS_MESSAGE)
  {
    StatusMessage* s_msg = (StatusMessage*) ret_message;
    if (s_msg->get_error() != NULL)
    {
      printf("%s\n", s_msg->get_error());
    }
  }
  delete ret_message;

  return TFS_SUCCESS;
}

int cmd_access_control_flag(TfsClient*, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size < 2)
  {
    fprintf(stderr, "setacl ip:port type [v1 v2]\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (port_str == NULL)
  {
    fprintf(stderr, "ip:port ,format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  uint32_t op_type = atoi(param[1].c_str());
  if (op_type < 1 || op_type > 5)
  {
    fprintf(stderr, "error type %d must in [1,5]\n\n", op_type);
    return TFS_ERROR;
  }

  const char* value1 = NULL;
  const char* value2 = NULL;
  uint64_t v1 = 0;
  uint32_t v2 = 0;
  if (size > 2)
    value1 = param[2].c_str();
  if (size > 3)
    value2 = param[3].c_str();

  switch (op_type)
  {
  case 1:
    if (!value1)
    {
      fprintf(stderr, "setacl ip:port 1 flag\n");
      return TFS_ERROR;
    }
    v1 = atoi(value1);
    v2 = 0;
    break;
  case 2:
    if (!value1 || !value2)
    {
      fprintf(stderr, "setacl ip:port 2 ip mask\n");
      return TFS_ERROR;
    }
    v1 = tbsys::CNetUtil::strToAddr(const_cast<char*> (value1), 0);
    v2 = static_cast<uint32_t> (tbsys::CNetUtil::strToAddr(const_cast<char*> (value2), 0));
    if (!v1 || !v2)
    {
      fprintf(stderr, "setacl ip:port 2 ip mask, not  a valid ip & mask\n");
      return TFS_ERROR;
    }
    break;
  case 3:
    if (!value1)
    {
      fprintf(stderr, "setacl ip:port 3 ipaddr\n");
      return TFS_ERROR;
    }
    v1 = tbsys::CNetUtil::strToAddr(const_cast<char*> (value1), 0);
    v2 = 0;
    break;
  case 4:
  case 5:
    v1 = 0;
    v2 = 0;
    break;
  default:
    fprintf(stderr, "error type %d must in [1,5]\n\n", op_type);
    return TFS_ERROR;
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_block_id(op_type); // param type == 1 as set acl flag.
  req_cc_msg.set_server_id(v1); // server_id as flag
  req_cc_msg.set_version(v2);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(server_id, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }

  return TFS_SUCCESS;
}

int cmd_access_stat_info(TfsClient*, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size < 1)
  {
    fprintf(stderr, "aci ip:port [startrow returnrow]\n\n");
    return TFS_ERROR;
  }
  char* ip = const_cast<char*> (param[0].c_str());
  char* port_str = strchr(ip, ':');
  if (port_str == NULL)
  {
    fprintf(stderr, "ip:port ,format error.\n\n");
    return TFS_ERROR;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  uint64_t server_id = Func::str_to_addr(ip, port);
  //uint32_t ipaddr = tbsys::CNetUtil::getAddr(reinterpret_cast<char*>param[1].c_str());

  uint32_t start_row = 0;
  uint32_t return_row = 0;
  if (size > 1)
  {
    start_row = atoi(param[1].c_str());
  }
  if (size > 2)
  {
    return_row = atoi(param[2].c_str());
  }
  bool get_all = (start_row == 0 && return_row == 0);
  if (get_all)
    return_row = 1000;
  int32_t has_next = 0;

  GetServerStatusMessage req_gss_msg;

  printf("ip addr           | read count  | read bytes  | write count  | write bytes\n");
  printf("------------------ -------------- ------------- -------------- ------------\n");
  while (true)
  {
    req_gss_msg.set_status_type(GSS_CLIENT_ACCESS_INFO);
    req_gss_msg.set_from_row(start_row);
    req_gss_msg.set_return_row(return_row);
    string err_msg;
    Message* ret_message = NULL;
    send_message_to_server(server_id, &req_gss_msg, err_msg, &ret_message);
    if (ret_message == NULL)
    {
      return TFS_ERROR;
    }
    if (ret_message->get_message_type() == ACCESS_STAT_INFO_MESSAGE)
    {
      AccessStatInfoMessage* req_cb_msg = reinterpret_cast<AccessStatInfoMessage*> (ret_message);
      const AccessStatInfoMessage::COUNTER_TYPE & m = req_cb_msg->get();
      for (AccessStatInfoMessage::COUNTER_TYPE::const_iterator it = m.begin(); it != m.end(); ++it)
      {
        printf      ("%15s : %14" PRI64_PREFIX "u %14s %14" PRI64_PREFIX "u %14s\n", tbsys::CNetUtil::addrToString(it->first).c_str(),
                     it->second.read_file_count_, Func::format_size(it->second.read_byte_).c_str(),
                     it->second.write_file_count_, Func::format_size(it->second.write_byte_).c_str());
      }

      has_next = req_cb_msg->has_next();

    }
    else if (ret_message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
      if (s_msg->get_error() != NULL)
      {
        printf("%s\n", s_msg->get_error());
      }
      break;
    }
    delete ret_message;

    if (get_all)
    {
      if (!has_next)
        break;
      else
      {
        start_row += return_row;
      }
    }
    else
    {
      break;
    }

  }

  return TFS_SUCCESS;
}

void print_file_info(uint32_t block_id, int32_t index, FileInfo* file_info, int32_t detail)
{
  if (detail && index == 0)
  {
    printf(
      "FILE_NAME          FILE_ID              OFFSET     SIZE       MODIFIED_TIME       CREATE_TIME         ST CRC       \n");
    printf(
      "------------------ -------------------- ---------- ---------- ------------------- ------------------- -- ----------\n");
  }
  FSName fs_name;
  fs_name.set_block_id(block_id);
  fs_name.set_file_id(file_info->id_);
  if (detail)
  {
    printf  ("%s %20" PRI64_PREFIX "u %10d %10d %s %s %02d %10u\n", fs_name.get_name(), file_info->id_, file_info->offset_,
             file_info->size_, Func::time_to_str(file_info->modify_time_).c_str(),
             Func::time_to_str(file_info->create_time_).c_str(), file_info->flag_, file_info->crc_);
  }
  else
  {
    printf("%s\n", fs_name.get_name());
  }
}

int cmd_list_file_info(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size < 1 || size > 3)
  {
    fprintf(stderr, "lsf block_id ip:port [detail]\n\n");
    return TFS_ERROR;
  }
  uint64_t server_id = 0;
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
  if (size >= 2)
  {
    char* ip = const_cast<char*> (param[1].c_str());
    char* port_str = strchr(ip, ':');
    if (NULL == port_str)
    {
      fprintf(stderr, "ip:port ,format error.\n\n");
      return TFS_ERROR;
    }
    *port_str = '\0';
    int32_t port = atoi(port_str + 1);
    server_id = Func::str_to_addr(ip, port);
  }
  int32_t show_detail = 0;
  if (3 == size && 0 == strcmp(param[2].c_str(), "detail"))
  {
    show_detail = 1;
  }

  FILE_INFO_LIST file_list;
  if (0 == server_id)
  {
    tfs_client->get_file_list(block_id, file_list);
  }
  else
  {
    GetServerStatusMessage req_gss_msg;
    req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO);
    req_gss_msg.set_return_row(block_id);
    string err_msg;
    Message* ret_message = NULL;
    send_message_to_server(server_id, &req_gss_msg, err_msg, &ret_message);
    if (ret_message == NULL)
    {
      return TFS_ERROR;
    }
    if (ret_message->get_message_type() == BLOCK_FILE_INFO_MESSAGE)
    {
      BlockFileInfoMessage* req_bfi_msg = dynamic_cast<BlockFileInfoMessage*> (ret_message);
      FILE_INFO_LIST* file_info_list =  req_bfi_msg->get_fileinfo_list();
      int32_t i = 0;
      int32_t list_size = file_info_list->size();
      for (i = 0; i < list_size; i++)
      {
        FileInfo *file_info = new FileInfo();
        memcpy(file_info, file_info_list->at(i), sizeof(FileInfo));
        file_list.push_back(file_info);
      }
    }
    else if (ret_message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = (StatusMessage*) ret_message;
      if (s_msg->get_error() != NULL)
      {
        printf("%s\n", s_msg->get_error());
      }
    }
    delete ret_message;
  }
  for (int32_t i = 0; i < static_cast<int32_t> (file_list.size()); i++)
  {
    print_file_info(block_id, i, file_list.at(i), show_detail);
    if (server_id == 0)
    {
      delete file_list.at(i);
    }
  }
  printf("total file: %d.\n", static_cast<int32_t> (file_list.size()));
  file_list.clear();

  return TFS_SUCCESS;
}

int cmd_list_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "listblock block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), (char **) (NULL), 10);
  VUINT64 dsList;
  int32_t ret = tfs_client->get_block_info(block_id, dsList);
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "block no exist in nameserver, blockid:%u.\n", block_id);
    return ret;
  }
  fprintf(stdout, "------block: %u, has %d replicas------\n", block_id, static_cast<int32_t> (dsList.size()));
  for (uint32_t i = 0; i < dsList.size(); ++i)
  {
    fprintf(stdout, "block: %u, (%d)th server: %s \n", block_id, i, tbsys::CNetUtil::addrToString(dsList[i]).c_str());
  }

  return TFS_SUCCESS;
}

int cmd_remove_block(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "removeblock block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
  VUINT64 dsList;
  dsList.clear();
  int32_t ret = tfs_client->get_block_info(block_id, dsList);
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "block no exist in nameserver, blockid:%u.\n", block_id);
    //      return ret;
  }
  fprintf(stdout, "------block: %u, has %d replicas------\n", block_id, static_cast<int32_t> (dsList.size()));
  //remove meta
  dsList.push_back(0);
  for (uint32_t i = 0; i < dsList.size(); ++i)
  {
    if (i < dsList.size() - 1)
    {
      fprintf(stdout, "removeblock: %u, (%d)th server: %s \n", block_id, i,
              tbsys::CNetUtil::addrToString(dsList[i]).c_str());
    }

    uint64_t nsip_port = tfs_client->get_ns_ip_port();

    ClientCmdMessage req_cc_msg;
    req_cc_msg.set_type(CLIENT_CMD_EXPBLK);
    req_cc_msg.set_server_id(dsList[i]);
    req_cc_msg.set_block_id(block_id);
    req_cc_msg.set_version(0);
    req_cc_msg.set_from_server_id(local_server_ip);
    string err_msg;
    if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
    {
      fprintf(stderr, "%s\n\n", err_msg.c_str());
      return TFS_ERROR;
    }
  }
  return TFS_SUCCESS;
}

int cmd_check_file_info(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "cfi filename\n\n");
    return TFS_ERROR;
  }

  char* filename = const_cast<char*> (param[0].c_str());
  FSName fs_name;
  //fs_name.set_name(filename, cluster_id, filename + FILE_NAME_LEN);
  fs_name.set_name(filename, filename + FILE_NAME_LEN, cluster_id);
  uint32_t block_id = fs_name.get_block_id();
  uint64_t file_id = fs_name.get_file_id();

  VUINT64 rds;
  if (tfs_client->get_unlink_block_info(block_id, rds) == TFS_ERROR)
  {
    return TFS_ERROR;
  }

  printf("BLOCK_ID: %u\n", block_id);
  for (int32_t i = 0; i < static_cast<int32_t> (rds.size()); i++)
  {
    uint64_t server_id = rds[i];
    GetServerStatusMessage req_gss_msg;
    req_gss_msg.set_status_type(GSS_BLOCK_FILE_INFO);
    req_gss_msg.set_return_row(block_id);
    string err_msg;
    Message* ret_message = NULL;
    send_message_to_server(server_id, &req_gss_msg, err_msg, &ret_message);
    if (ret_message == NULL)
    {
      return TFS_ERROR;
    }
    if (ret_message->get_message_type() == BLOCK_FILE_INFO_MESSAGE)
    {
      BlockFileInfoMessage* req_bfi_msg = reinterpret_cast<BlockFileInfoMessage*> (ret_message);
      FILE_INFO_LIST* file_list = req_bfi_msg->get_fileinfo_list();
      for (int32_t j = 0; j < static_cast<int32_t> (file_list->size()); j++)
      {
        if (file_list->at(j)->id_ != file_id)
          continue;
        print_file_info(block_id, i, file_list->at(j), 1);
        break;
      }
      printf("%19sSERVER: %s\n", "", tbsys::CNetUtil::addrToString(server_id).c_str());
    }
    else if (ret_message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
      if (s_msg->get_error() != NULL)
      {
        printf("%s\n", s_msg->get_error());
      }
    }
    delete ret_message;
  }
  printf("\n");

  return TFS_SUCCESS;
}

int get_file_retry(TfsClient* tfs_client, char* tfs_name, char* local_file)
{
  fprintf(stderr, "filename: %s\n", tfs_name);
  fflush( stderr);
  if (tfs_client->tfs_open(tfs_name, NULL, READ_MODE) != TFS_SUCCESS)
  {
    fprintf(stderr, "open tfs_client fail: %s\n", tfs_client->get_error_message());
    return TFS_ERROR;
  }
  FileInfo file_info;
  if (tfs_client->tfs_stat(&file_info) == TFS_ERROR)
  {
    tfs_client->tfs_close();
    //fprintf(stderr, "fstat tfs_client fail: %s\n", tfs_client->get_error_message());
    return TFS_ERROR - 100;
  }

  int32_t done = 0;
  while (done >= 0 && done <= 10)
  {
    int64_t t1 = Func::curr_time();
    int32_t fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (-1 == fd)
    {
      fprintf(stderr, "open local file fail: %s\n", local_file);
      tfs_client->tfs_close();
      return TFS_ERROR;
    }

    char data[MAX_READ_SIZE];
    uint32_t crc = 0;
    int32_t total_size = 0;
    for (;;)
    {
      int32_t read_len = tfs_client->tfs_read(data, MAX_READ_SIZE);
      if (read_len < 0)
      {
        fprintf(stderr, "read tfs_client fail: %s\n", tfs_client->get_error_message());
        break;
      }
      if (0 == read_len)
      {
        break;
      }
      if (write(fd, data, read_len) != read_len)
      {
        fprintf(stderr, "write local file fail: %s\n", local_file);
        tfs_client->tfs_close();
        close(fd);
        return TFS_ERROR;
      }
      crc = Func::crc(crc, data, read_len);
      total_size += read_len;
      if (read_len != MAX_READ_SIZE)
      {
        break;
      }
    }
    close(fd);
    if (crc == file_info.crc_ && total_size == file_info.size_)
    {
      tfs_client->tfs_close();
      return TFS_SUCCESS;
    }
    //TBSYS_LOG(ERROR, "%s %s", tbsys::CNetUtil::addrToString(tfs_client->get_last_elect_ds_id()).c_str(), tfs_name);
    int64_t t2 = Func::curr_time();
    if (t2 - t1 > 500000)
    {
      fprintf(stderr, "filename: %s, time: %" PRI64_PREFIX "d, server: %s, done: %d\n", tfs_name, t2 - t1,
              tbsys::CNetUtil::addrToString(tfs_client->get_last_elect_ds_id()).c_str(), done);
      fflush(stderr);
    }
    done++;
    if (tfs_client->tfs_reset_read() <= done)
    {
      break;
    }
  }
  tfs_client->tfs_close();
  return TFS_ERROR;
}

int cmd_repair_crc(TfsClient* tfs_client, VEC_STRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "repaircrc filename\n\n");
    return TFS_ERROR;
  }

  char* tfs_name = const_cast<char*> (param[0].c_str());
  if (static_cast<int32_t> (strlen(tfs_name)) < FILE_NAME_LEN || tfs_name[0] != 'T')
  {
    fprintf(stderr, "file name error: %s\n", tfs_name);
    return TFS_ERROR;
  }

  char local_file[56];
  sprintf(local_file, ".repair_crc_%s", tfs_name);
  int32_t ret = get_file_retry(tfs_client, tfs_name, local_file);
  if (ret == TFS_SUCCESS)
  {
    ret = tfs_client->save_file(local_file, tfs_name, NULL);
    if (ret)
    {
      fprintf(stderr, "save failed: %s => %s\n", local_file, tfs_name);
    }
  }
  else if (ret == TFS_ERROR - 100)
  {
    fprintf(stderr, "file not exits: %s\n", tfs_name);
  }
  else
  {
    fprintf(stderr, "don't have such file: %s\n", tfs_name);
  }
  unlink(local_file);

  tfs_client->tfs_close();
  return ret;
}

int cmd_get_repl_info(TfsClient* tfs_client, VEC_STRING&)
{
  char time_str[256];
  uint64_t nsip_port = tfs_client->get_ns_ip_port();
  ReplicateInfoMessage rep_i_msg;
  Message* ret_msg = NULL;
  if (TFS_SUCCESS == send_message_to_server(nsip_port, &rep_i_msg, &ret_msg))
  {
    if (ret_msg->get_message_type() == REPLICATE_INFO_MESSAGE)
    {
      ReplicateInfoMessage* rim = reinterpret_cast<ReplicateInfoMessage*> (ret_msg);
      ReplicateInfoMessage::REPL_BLOCK_MAP rep_map = rim->get_replicating_map();
      ReplicateInfoMessage::COUNTER_TYPE src_counter = rim->get_source_ds_counter();
      ReplicateInfoMessage::COUNTER_TYPE dst_counter = rim->get_dest_ds_counter();
      fprintf(stderr, "---------------replicating table--------------------------\n");
      for (ReplicateInfoMessage::REPL_BLOCK_MAP::const_iterator it = rep_map.begin(); it != rep_map.end(); ++it)
      {
        tbsys::CTimeUtil::timeToStr(it->second.start_time_, time_str);
        fprintf(stderr, "block: %d ,machine: %s -> %s ,ismove: %d ,server count: %d ,start time: %s\n",
                it->second.block_id_, tbsys::CNetUtil::addrToString(it->second.source_id_).c_str(),
                tbsys::CNetUtil::addrToString(it->second.destination_id_).c_str(), it->second.is_move_,
                it->second.server_count_, time_str);
      }
      fprintf(stderr, "---------------replicating source counter--------------------------\n");
      for (ReplicateInfoMessage::COUNTER_TYPE::const_iterator it = src_counter.begin(); it != src_counter.end(); ++it)
      {
        fprintf(stderr, "source server: %s ,count: %d\n", tbsys::CNetUtil::addrToString(it->first).c_str(), it->second);
      }
      fprintf(stderr, "---------------replicating destination counter--------------------------\n");
      for (ReplicateInfoMessage::COUNTER_TYPE::const_iterator it = dst_counter.begin(); it != dst_counter.end(); ++it)
      {
        fprintf(stderr, "destination server: %s ,count: %d\n", tbsys::CNetUtil::addrToString(it->first).c_str(),
                it->second);
      }
    }
    else
    {
      fprintf(stderr, "getreplinfo error, return type not match:%d\n", ret_msg->get_message_type());
    }
    delete ret_msg;
  }
  else
  {
    fprintf(stderr, "getreplinfo error, return msg is null\n");
  }
  return TFS_SUCCESS;
}

int cmd_clear_repl_info(TfsClient* tfs_client, VEC_STRING&)
{
  uint64_t nsip_port = tfs_client->get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_type(CLIENT_CMD_CLEAR_REPL_INFO);
  req_cc_msg.set_server_id(0);
  req_cc_msg.set_block_id(0);
  req_cc_msg.set_version(0);
  req_cc_msg.set_from_server_id(local_server_ip);
  string err_msg;
  if (send_message_to_server(nsip_port, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}
