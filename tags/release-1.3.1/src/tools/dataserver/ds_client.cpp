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
 *   jihe
 *      - initial release
 *   chuyu <chuyu@taobao.com>
 *      - modify 2010-03-20
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <signal.h>
#include <vector>
#include <string>
#include <map>
#include "client/tfs_session.h"
#include "client/tfs_file.h"
#include "common/func.h"
#include "ds_task.h"
#include "ds_util.h"

#define CMD_MAX_LEN 4096

using namespace tfs::common;

typedef map<string, int> STR_INT_MAP;
typedef STR_INT_MAP::iterator STR_INT_MAP_ITER;
enum CmdSet
{
  CMD_GET_SERVER_STATUS,
  CMD_GET_PING_STATUS,
  CMD_NEW_BLOCK,
  CMD_REMOVE_BlOCK,
  CMD_LIST_BLOCK,
  CMD_GET_BLOCK_INFO,
  CMD_RESET_BLOCK_VERSION,
  CMD_CREATE_FILE_ID,
  CMD_LIST_FILE,
  CMD_READ_FILE_DATA,
  CMD_WRITE_FILE_DATA,
  CMD_UNLINK_FILE,
  CMD_RENAME_FILE,
  CMD_READ_FILE_INFO,
  CMD_SEND_CRC_ERROR,
  CMD_LIST_BITMAP,
  CMD_HELP,
  CMD_UNKNOWN,
  CMD_NOP,
  CMD_QUIT,
};

void init();
void usage(const char* name);
void signal_handler(const int sig);
int parse_cmd(char* buffer, VSTRING & param);
int switch_cmd(const int cmd, VSTRING & param);
int main_loop();
int show_help(VSTRING & param);
int no_operation();

STR_INT_MAP cmd_map;
uint64_t ds_ip = 0;


#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"

char* match_cmd(const char* text, int state)
{
  static STR_INT_MAP_ITER it;
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

char** dscmd_completion (const char* text, int start, int end)
{
  // at the start of line, then it's a cmd completion
  return (0 == start) ? rl_completion_matches(text, match_cmd) : (char**)NULL;
}
#endif

int main(int argc, char* argv[])
{
  int i = 0;
  int iex = 0;

  // input option
  if (argc == 1)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  while ((i = getopt(argc, argv, "d:i::")) != -1)
  {
    switch (i)
    {
    case 'i':
      iex = 1;
      break;
    case 'd':
      ds_ip = get_ip_addr(optarg);
      if (ds_ip == 0)
      {
        printf("ip or port is invalid, please try again.\n");
        return TFS_ERROR;
      }
      break;
    case ':':
      printf("missing -d");
      usage(argv[0]);
      break;
    default:
      usage(argv[0]);
      return TFS_ERROR;
    }
  }
  init();

  if (optind >= argc)
  {
    signal(SIGINT, signal_handler);
    main_loop();
  }
  else
  {
    VSTRING param;
    int i = optind;
    if (iex)
    {
      printf("with i\n");
      for (i = optind; i < argc; i++)
      {
        param.clear();
        int cmd = parse_cmd(argv[i], param);
        switch_cmd(cmd, param);
      }
    }
    else
    {
      printf("without i\n");
      for (i = optind; i < argc; i++)
      {
        param.clear();
        param.push_back(argv[i]);
      }
    }
  }
  return TFS_SUCCESS;
}

// no returns.
// this is initialization for defining the command
void init()
{
  cmd_map["get_server_status"] = CMD_GET_SERVER_STATUS;
  cmd_map["get_ping_status"] = CMD_GET_PING_STATUS;
  cmd_map["new_block"] = CMD_NEW_BLOCK;
  cmd_map["remove_block"] = CMD_REMOVE_BlOCK;
  cmd_map["list_block"] = CMD_LIST_BLOCK;
  cmd_map["get_block_info"] = CMD_GET_BLOCK_INFO;
  cmd_map["reset_block_version"] = CMD_RESET_BLOCK_VERSION;
  cmd_map["create_file_id"] = CMD_CREATE_FILE_ID;
  cmd_map["list_file"] = CMD_LIST_FILE;
  cmd_map["read_file_data"] = CMD_READ_FILE_DATA;
  cmd_map["write_file_data"] = CMD_WRITE_FILE_DATA;
  cmd_map["unlink_file"] = CMD_UNLINK_FILE;
  cmd_map["rename_file"] = CMD_RENAME_FILE;
  cmd_map["read_file_info"] = CMD_READ_FILE_INFO;
  cmd_map["help"] = CMD_HELP;
  cmd_map["unknown_cmd"] = CMD_UNKNOWN;
  cmd_map["nop"] = CMD_NOP;
  cmd_map["quit"] = CMD_QUIT;
  cmd_map["send_crc_error"] = CMD_SEND_CRC_ERROR;
  cmd_map["list_bitmap"] = CMD_LIST_BITMAP;
}

// no return.
// show the prompt of command.
void usage(const char* name)
{
  printf("Usage: %s -d ip:port \n", name);
  exit( TFS_ERROR);
}

//no return.
//process the signal
void signal_handler(const int sig)
{
  switch (sig)
  {
  case SIGINT:
    fprintf(stderr, "\nDataServer> ");
    break;
  }
}

//get the command
int parse_cmd(char* key, VSTRING & param)
{
  int cmd = CMD_NOP;
  char* token;
  //remove the space
  while (*key == ' ')
    key++;
  token = key + strlen(key);
  while (*(token - 1) == ' ' || *(token - 1) == '\n' || *(token - 1) == '\r')
    token--;
  *token = '\0';
  if (key[0] == '\0')
  {
    return cmd;
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
  //find the command
  STR_INT_MAP_ITER it = cmd_map.find(Func::str_to_lower(key));
  if (it == cmd_map.end())
  {
    return CMD_UNKNOWN;
  }
  else
  {
    cmd = it->second;
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
  //get the parameters
  param.clear();
  while ((token = strsep(&key, " ")) != NULL)
  {
    if (token[0] == '\0')
    {
      continue;
    }
    param.push_back(token);
  }
  return cmd;
}

int switch_cmd(const int cmd, VSTRING & param)
{
  int ret = TFS_SUCCESS;
  DsTask* ds_task = new DsTask();
  switch (cmd)
  {
  case CMD_GET_SERVER_STATUS:
    {
      if (param.size() != 1)
      {
        printf("Usage:get_server_status nums\n");
        printf("get the blocks visited most frequently in dataserver.\n");
        break;
      }
      int num_row = atoi(const_cast<char*>(param[0].c_str()));
      ds_task->init(ds_ip, num_row);
      ret = ds_task->get_server_status();
      break;
    }
  case CMD_GET_PING_STATUS:
    {
      if (param.size() != 0)
      {
        printf("Usage:get_ping_status \n");
        printf("get the ping status of dataServer.\n");
        break;
      }
      ds_task->init(ds_ip);
      ret = ds_task->get_ping_status();
      break;
    }
  case CMD_NEW_BLOCK:
    {
      if (param.size() != 1)
      {
        printf("Usage:new_block block_id\n");
        printf("create a new block in dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id);
      ret = ds_task->new_block();
      break;
    }
  case CMD_REMOVE_BlOCK:
    {
      if (param.size() != 1)
      {
        printf("Usage:remove_block block_id\n");
        printf("delete a block in dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id);
      ret = ds_task->remove_block();
      break;
    }
  case CMD_LIST_BLOCK:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_block type\n");
        printf("list all the blocks in a dataserver.\n");
        break;
      }
      int type = atoi(const_cast<char*> (param[0].c_str()));
      ds_task->init_list_block_type(ds_ip, type);
      ret = ds_task->list_block();
      break;
    }
  case CMD_GET_BLOCK_INFO:
    {
      if (param.size() != 1)
      {
        printf("Usage:get_block_info block_id\n");
        printf("get the information of a block in the dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id);
      ret = ds_task->get_block_info();
      break;
    }
  case CMD_RESET_BLOCK_VERSION:
    {
      if (param.size() != 1)
      {
        printf("Usage:rest_block_version block_id\n");
        printf("reset the version of the block in dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id);
      ret = ds_task->reset_block_version();
      break;
    }
  case CMD_CREATE_FILE_ID:
    {
      if (param.size() != 2)
      {
        printf("Usage:create_file_id block_id file_id\n");
        printf("add a new fileID.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_new_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id, ds_new_file_id);
      ret = ds_task->create_file_id();
      break;
    }
  case CMD_LIST_FILE:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_file block_id\n");
        printf("list all the files in a block.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);

      ds_task->init(ds_ip, ds_block_id);
      ret = ds_task->list_file();
      break;
    }
  case CMD_READ_FILE_DATA:
    {
      if (param.size() != 3)
      {
        printf("Usage:read_file_data blockid fileid local_file_name\n");
        printf("download a tfs file to local.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      char local_file[256] = { '\0' };
      strcpy(local_file, (char*) param[2].c_str());
      ds_task->init(ds_ip, ds_block_id, ds_file_id, local_file);
      ret = ds_task->read_file_data();
      break;
    }
  case CMD_WRITE_FILE_DATA:
    {
      if (param.size() != 3)
      {
        printf("Usage:write_file_data block_id file_id local_file_name\n");
        printf("upload a local file to a dataserver.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      char local_file[256] = { '\0' };
      strcpy(local_file, (char*) param[2].c_str());
      ds_task->init(ds_ip, ds_block_id, ds_file_id, local_file);
      ret = ds_task->write_file_data();
      break;
    }
  case CMD_UNLINK_FILE:
    {
      if (param.size() != 5)
      {
        printf("Usage:unlink_file block_id file_id unlink_type option_flag is_master\n");
        printf("delete a file.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);

      int32_t unlink_type = atoi(const_cast<char*> (param[2].c_str()));
      int32_t option_flag = atoi(const_cast<char*> (param[3].c_str()));
      int32_t is_master = atoi(const_cast<char*> (param[4].c_str()));

      ds_task->init(ds_ip, ds_block_id, ds_file_id, unlink_type, option_flag, is_master);
      ret = ds_task->unlink_file();
      break;
    }
  case CMD_RENAME_FILE:
    {
      if (param.size() != 3)
      {
        printf("Usage:rename_file block_id old_file_id new_file_id\n");
        printf("rename file id.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_old_file_id = strtoul(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_new_file_id = strtoul(const_cast<char*> (param[2].c_str()), reinterpret_cast<char**> (NULL), 10);
      ds_task->init(ds_ip, ds_block_id, ds_old_file_id, ds_new_file_id);
      ret = ds_task->rename_file();
      break;
    }
  case CMD_READ_FILE_INFO:
    {
      if (param.size() != 3)
      {
        printf("Usage:read_file_info block_id file_id ds_mode\n");
        printf("get the file information.\n");
        break;
      }
      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      int32_t ds_mode = atoi(const_cast<char*> (param[2].c_str()));
      ds_task->init(ds_ip, ds_block_id, ds_file_id, ds_mode);
      ret = ds_task->read_file_info();
      break;
    }
  case CMD_SEND_CRC_ERROR:
    {
      if (param.size() < 3)
      {
        printf("Usage: send_crc_error block_id file_id crc [error_flag] [serverid1 server_id2 ...]\n");
        printf("send crc error \n");
        break;
      }

      uint32_t ds_block_id = strtoul(const_cast<char*> (param[0].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint64_t ds_file_id = strtoull(const_cast<char*> (param[1].c_str()), reinterpret_cast<char**> (NULL), 10);
      uint32_t ds_crc = strtoul(const_cast<char*> (param[2].c_str()), reinterpret_cast<char**> (NULL), 10);

      int32_t ds_error_flag = 0;
      if (param.size() > 3)
      {
        ds_error_flag = atoi(const_cast<char*> (param[3].c_str()));
      }

      int32_t ds_snum = 1;
      uint64_t *fail_servers;
      if (param.size() > 4)
      {
        ds_snum = param.size() - 4;
        fail_servers = new uint64_t[ds_snum];
        int32_t i = 0;
        int32_t param_size = param.size();
        for (i = 4; i < param_size; i++)
        {
          fail_servers[i - 4] = strtoull(const_cast<char*> (param[i].c_str()), reinterpret_cast<char**> (NULL), 10);
        }
      }
      else
      {
        ds_snum = 1;
        fail_servers = new uint64_t[1];
        fail_servers[0] = ds_ip;
      }

      ds_task->init(ds_ip, ds_block_id, ds_file_id, ds_crc, ds_error_flag, fail_servers, ds_snum);
      ret = ds_task->send_crc_error();
      delete[] fail_servers;
      break;
    }
  case CMD_LIST_BITMAP:
    {
      if (param.size() != 1)
      {
        printf("Usage:list_bitmap type\n");
        printf("list the bitmap of the server.\n");
        break;
      }
      int type = atoi(const_cast<char*> (param[0].c_str()));
      ds_task->init_list_block_type(ds_ip, type);
      ret = ds_task->list_bitmap();
      break;
    }
  case CMD_UNKNOWN:
    fprintf(stderr, "unknown command.\n");
    ret = show_help(param);
    break;
  case CMD_HELP:
    ret = show_help(param);
    break;
  default:
    break;
  }
  delete ds_task;
  return ret;
}

int main_loop()
{
  VSTRING param;
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = dscmd_completion;
#else
  char cmd_line[CMD_MAX_LEN];
#endif

  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("DataServer> ");
    if (!cmd_line)
#else
    fprintf(stderr, "DataServer> ");
    if (NULL == fgets(cmd_line, CMD_MAX_LEN, stdin))
#endif
    {
      continue;
    }

    int cmd = parse_cmd(cmd_line, param);
#ifdef _WITH_READ_LINE
    delete cmd_line;
    cmd_line = NULL;
#endif
    if (cmd == CMD_QUIT)
    {
      break;
    }
    switch_cmd(cmd, param);
  }
  return TFS_SUCCESS;
}

//show help tips
int show_help(VSTRING &)
{
  printf("COMMAND SET:\n"
    "get_server_status            get the information of blocks that were visited most frequently in dataserver.\n"
    "get_ping_status              get the ping status of dataServer.\n"
    "list_block                   list all the blocks in a dataserver.\n"
    "get_block_info               get the information of a block in the dataserver.\n"
    "list_file                    list all the files in a block.\n"
    "read_file_data               download a tfs file to local.\n"
    "unlink_file                  delete a file.\n"
    "read_file_info               get the file information.\n"
    "list_bitmap                  list the bitmap of the server\n"
    "quit                         quit console.\n"
    "help                         show help.\n\n");
  return TFS_SUCCESS;
}
