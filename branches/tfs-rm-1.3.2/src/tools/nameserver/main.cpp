#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <ext/hash_map>
#include <signal.h>
#include <Memory.hpp>
#include "show.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::tools;

typedef vector<std::string> VSTRING;
typedef int (*cmd_function)(VSTRING&);
struct CmdNode
{
  const char* info_;
  int32_t min_param_count_;
  int32_t max_param_count_;
  cmd_function func_;

  CmdNode()
  {
  }

  CmdNode(const char* info, int32_t min_param_count, int32_t max_param_count, cmd_function func) :
    info_(info), min_param_count_(min_param_count), max_param_count_(max_param_count), func_(func)
  {
  }
};

typedef map<string, CmdNode> MSTR_FUNC;
typedef MSTR_FUNC::iterator MSTR_FUNC_ITER;

static const int TFS_CLIENT_QUIT = 0xfff1234;
static const int32_t CMD_MAX_LEN = 4096;
static MSTR_FUNC g_cmd_map;
static char* g_cur_cmd;
static ShowInfo g_show_info;

int main_loop();
int do_cmd(char*);
int parse_block_param(VSTRING&, int8_t&, int32_t&, uint32_t&);
int parse_server_param(VSTRING&, int8_t& , int32_t& );
int cmd_show_help(VSTRING&);
int cmd_quit(VSTRING&);
int cmd_show_block(VSTRING&);
int cmd_show_server(VSTRING& param);
int cmd_show_machine(VSTRING& param);

typedef map<std::string, int32_t> STR_INT_MAP;
typedef STR_INT_MAP::iterator STR_INT_MAP_ITER;
static STR_INT_MAP g_sub_cmd_map;

#ifdef _WITH_READ_LINE
#include "readline/readline.h"
#include "readline/history.h"
template<class T> const char* get_str(T it)
{
  return it->first.c_str();
}

template<> const char* get_str(VSTRING::iterator it)
{
  return (*it).c_str();
}

template<class T> char* do_match(const char* text, int state, T& m)
{
  static typename T::iterator it;
  static int len = 0;
  const char* cmd = NULL;

  if (!state)
  {
    it = m.begin();
    len = strlen(text);
  }

  while(it != m.end())
  {
    cmd = get_str(it);
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

char* match_cmd(const char* text, int32_t state)
{
  return do_match(text, state, g_cmd_map);
}

char** admin_cmd_completion (const char* text, int start, int end)
{
  // disable default filename completion
  rl_attempted_completion_over = 1;
  return rl_completion_matches(text, match_cmd);
}

#endif

int parse_block_param(VSTRING& param, int8_t& flag, int32_t& num, uint32_t& block_id)
{
  flag = CMD_NOP;
  VSTRING::iterator iter = param.begin();
  for (; iter != param.end(); iter++)
  {
    char* key = const_cast<char*>((*iter).c_str());
    STR_INT_MAP_ITER it = g_sub_cmd_map.find(Func::str_to_lower(key));

    if (it == g_sub_cmd_map.end())
    {
      return CMD_UNKNOWN;
    }
    else
    {
      int32_t cmd = it->second;
      switch (cmd)
      {
        case CMD_NUM:
          num = static_cast<int32_t>(atoi((*(++iter)).c_str()));
          if (num == 0 || num == INT_MAX || num == INT_MIN)
          {
            TBSYS_LOG(ERROR, "unvalid param (cmd---%d)", cmd);
            exit(TFS_ERROR);
          }
          break;
        case CMD_BLOCK_ID:
          block_id = static_cast<uint32_t>(atoi((*(++iter)).c_str()));
          if (block_id == 0 || block_id == INT_MAX || block_id == INT_MIN)
          {
            TBSYS_LOG(ERROR, "unvalid param (cmd---%d)", cmd);
            exit(TFS_ERROR);
          }
          break;
        case CMD_SERVER:
          flag = CMD_SERVER;
          break;
        default:
          break;
      }
    }
  }
}
int parse_server_param(VSTRING& param, int8_t& flag, int32_t& num)
{
  flag = CMD_NOP;
  VSTRING::iterator iter = param.begin();
  for (; iter != param.end(); iter++)
  {
    STR_INT_MAP_ITER it = g_sub_cmd_map.find(Func::str_to_lower(const_cast<char*>((*iter).c_str())));

    if (it == g_sub_cmd_map.end())
    {
      return CMD_UNKNOWN;
    }
    else
    {
      int32_t cmd = it->second;
      switch (cmd)
      {
        case CMD_NUM:
          num = static_cast<int32_t>(atoi((*(++iter)).c_str()));
          if (num == 0 || num == INT_MAX || num == INT_MIN)
          {
            TBSYS_LOG(ERROR, "unvalid param (cmd---%d)", cmd);
            exit(TFS_ERROR);
          }
          break;
        case CMD_BLOCK_LIST:
          flag = CMD_BLOCK_LIST;
          break;
        case CMD_BLOCK_WRITABLE:
          flag = CMD_BLOCK_WRITABLE;
          break;
        case CMD_BLOCK_MASTER:
          flag = CMD_BLOCK_MASTER;
          break;
        default:
          break;
      }
    }
  }
}
int parse_machine_param(VSTRING& param, int8_t& flag)
{
  flag = PRINT_ALL;
  VSTRING::iterator iter = param.begin();
  for (; iter != param.end(); iter++)
  {
    STR_INT_MAP_ITER it = g_sub_cmd_map.find(Func::str_to_lower(const_cast<char*>((*iter).c_str())));

    if (it == g_sub_cmd_map.end())
    {
      return CMD_UNKNOWN;
    }
    else
    {
      int32_t cmd = it->second;
      switch (cmd)
      {
        case CMD_ALL:
          flag = PRINT_ALL;
          break;
        case CMD_PART:
          flag = PRINT_PART;
          break;
        default:
          break;
      }
    }
  }

}


void init()
{
  g_cmd_map["help"] = CmdNode("help                 [show help info]", 0, 0, cmd_show_help);
  g_cmd_map["quit"] = CmdNode("quit                 [quit]", 0, 0, cmd_quit);
  g_cmd_map["exit"] = CmdNode("exit                 [exit]", 0, 0, cmd_quit);
  g_cmd_map["block"] = CmdNode("block  [-n num] [-id block_id] [-s]   show block info\n -s print server list ", 0, 3, cmd_show_block);
  g_cmd_map["server"] = CmdNode("server [-n num] [-b] [-w] [-m]    show server info\n -b print block list \n -w print writable block list \n -m print master block list", 0, 3, cmd_show_server);
  g_cmd_map["machine"] = CmdNode("machine [-a] [-p]    show machine info \n -a print all info \n -p print part of infos ", 0, 3, cmd_show_machine);

  g_sub_cmd_map["-num"] = CMD_NUM;
  g_sub_cmd_map["-bid"] = CMD_BLOCK_ID;
  g_sub_cmd_map["-block"] = CMD_BLOCK_LIST;
  g_sub_cmd_map["-writable"] = CMD_BLOCK_WRITABLE;
  g_sub_cmd_map["-master"] = CMD_BLOCK_MASTER;
  g_sub_cmd_map["-server"] = CMD_SERVER;
  g_sub_cmd_map["-all"] = CMD_ALL;
  g_sub_cmd_map["-part"] = CMD_PART;

  g_sub_cmd_map["-n"] = CMD_NUM;
  g_sub_cmd_map["-id"] = CMD_BLOCK_ID;
  g_sub_cmd_map["-b"] = CMD_BLOCK_LIST;
  g_sub_cmd_map["-w"] = CMD_BLOCK_WRITABLE;
  g_sub_cmd_map["-m"] = CMD_BLOCK_MASTER;
  g_sub_cmd_map["-s"] = CMD_SERVER;
  g_sub_cmd_map["-a"] = CMD_ALL;
  g_sub_cmd_map["-p"] = CMD_PART;
}

int cmd_show_help(VSTRING&)
{
  fprintf(stderr, "\nsupported command:");
  for (MSTR_FUNC_ITER it = g_cmd_map.begin(); it != g_cmd_map.end(); it++)
  {
    fprintf(stderr, "\n%s", it->second.info_);
  }
  fprintf(stderr, "\n\n");
  return TFS_SUCCESS;
}

int cmd_quit(VSTRING&)
{
  return TFS_CLIENT_QUIT;
}

int cmd_show_block(VSTRING& param)
{
  int8_t flag = CMD_NOP;
  int32_t num = MAX_READ_NUM;
  uint32_t block_id = -1;
  parse_block_param(param, flag, num, block_id);
  g_show_info.show_block(flag, num, block_id);
  return TFS_SUCCESS;
}

int cmd_show_server(VSTRING& param)
{
  int8_t flag = CMD_NOP;
  int32_t num = MAX_READ_NUM;
  parse_server_param(param, flag, num);
  g_show_info.show_server(flag, num);
  return TFS_SUCCESS;
}

int cmd_show_machine(VSTRING& param)
{
  int8_t flag = CMD_ALL;
  int32_t num = MAX_READ_NUM;
  parse_machine_param(param, flag);
  g_show_info.show_machine(flag, num);
  return TFS_SUCCESS;
}

int usage(const char *name)
{
  fprintf(stderr, "Usage: %s -s ns_ip_port\n", name);
  fprintf(stderr, "\n");
  exit(TFS_ERROR);
}
static void sign_handler(int32_t sig)
{
  switch (sig)
  {
    case SIGINT:
    case SIGTERM:
      fprintf(stderr, "showssm tool exit.\n");
      exit(TFS_ERROR);
      break;
  }
}
inline bool is_whitespace(char c)
{
  return (' ' == c || '\t' == c);
}
inline char* strip_line(char* line)
{
  while (is_whitespace(*line))
  {
    line++;
  }
  int32_t end = strlen(line);
  while (end && (is_whitespace(line[end-1]) || '\n' == line[end-1] || '\r' == line[end-1]))
  {
    end--;
  }
  line[end] = '\0';
  return line;
}

int main(int argc,char** argv)
{
  //TODO readline
  int32_t i;
  std::string ns_ip_port;
  while ((i = getopt(argc, argv, "s:lh:")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_ip_port = optarg;
        break;
      case 'h':
      default:
        usage(argv[0]);
    }
  }

  if (ns_ip_port.empty())
  {
    fprintf(stderr, "please input nameserver ip and port.\n");
    usage(argv[0]);
  }

  init();
  g_show_info.set_ns_ip(ns_ip_port);

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop();
  }
  else
  {
    usage(argv[0]);
  }
}
int main_loop()
{
#ifdef _WITH_READ_LINE
  char* cmd_line = NULL;
  rl_attempted_completion_function = admin_cmd_completion;
#else
  char cmd_line[CMD_MAX_LEN];
#endif
  int ret = TFS_ERROR;
  while (1)
  {
#ifdef _WITH_READ_LINE
    cmd_line = readline("show > ");
    if (!cmd_line)
#else
    fprintf(stderr, "show > ");
    if (NULL == fgets(cmd_line, CMD_MAX_LEN, stdin))
#endif
    {
      continue;
    }
    ret = do_cmd(cmd_line);
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

int32_t do_cmd(char* key)
{
  key = strip_line(key);
  if (!key[0])
  {
    return TFS_SUCCESS;
  }
#ifdef _WITH_READ_LINE
  // not blank line, add to history
  add_history(key);
#endif

  char* token = strchr(key, ' ');
  if (token != NULL)
  {
    *token = '\0';
  }

  MSTR_FUNC_ITER it = g_cmd_map.find(Func::str_to_lower(key));

  if (it == g_cmd_map.end())
  {
    fprintf(stderr, "unknown command. \n");
    return TFS_ERROR;
  }
  // ok this is current command
  g_cur_cmd = key;

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

  //check param count
  int32_t min_param_count = g_cmd_map[g_cur_cmd].min_param_count_;
  int32_t max_param_count = g_cmd_map[g_cur_cmd].max_param_count_;
  int32_t param_size = static_cast<int32_t>(param.size());
  if ((param_size < min_param_count) || (param_size) > max_param_count)
  {
    fprintf(stderr, "%s\n\n", g_cmd_map[g_cur_cmd].info_);
    return TFS_ERROR;
  }

  return it->second.func_(param);
}
