#include <stdio.h>
#include <pthread.h>
#include <signal.h>
#include <vector>
#include <string>
#include <map>
#include <tbsys.h>

#include "common/internal.h"
#include "new_client/fsname.h"
#include "common/client_manager.h"
#include "message/server_status_message.h"
#include "message/client_cmd_message.h"
#include "message/message_factory.h"
#include "common/config_item.h"
#include "new_client/tfs_client_api.h"
#include "common/status_message.h"


using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;

typedef int (*cmd_function)(TfsClient*, VSTRING&);
struct CmdNode
{
  int32_t param_count_;
  cmd_function func_;
  const char* help_info_;

  CmdNode()
  {
  }

  CmdNode(int32_t param_count, cmd_function func, char* help_info) :
    param_count_(param_count), func_(func), help_info_(help_info)
  {
  }
};

// the reflect between command and function
typedef map<string, cmd_function> MSTR_FUNC;
typedef MSTR_FUNC::iterator MSTR_FUNC_ITER;

static const int TFS_CLIENT_QUIT = 0xfff1234;
static MSTR_FUNC g_cmd_map;
static char* g_cur_cmd;
static uint64_t local_server_ip = 0;
static std::string ns_ip_port;
static uint64_t ns_id;

int usage(const char *name);
static void sign_handler(int32_t sig);
int main_loop(TfsClient* tfs_client);
int do_cmd(TfsClient* tfs_client, char*);
void init();
int send_message_to_server(uint64_t server, NewClient* client, tbnet::Packet* msg, string& err_msg, tbnet::Packet** output = NULL/*not free*/, const int64_t timeout = 3000);
uint64_t trans_to_ns_id(char* ipport);
int get_file_retry(TfsClient* tfs_client, char* tfs_name, char* local_file);

/* cmd func */
int set_run_param(TfsClient* tfs_client, VSTRING& param);
int add_block(TfsClient* tfs_client, VSTRING& param);
int remove_block(TfsClient* tfs_client, VSTRING& param);
int expire_block(TfsClient* tfs_client, VSTRING& param);
int unexpire_block(TfsClient* tfs_client, VSTRING& param);
int compact_block(TfsClient* tfs_client, VSTRING& param);
int repair_lose_block(TfsClient* tfs_client, VSTRING& param);
int repair_group_block(TfsClient* tfs_client, VSTRING& param);
int repair_crc(TfsClient* tfs_client, VSTRING& param);
int access_stat_info(TfsClient*, VSTRING& param);
int access_control_flag(TfsClient*, VSTRING& param);
int show_help(TfsClient*, VSTRING&);
int quit(TfsClient*, VSTRING&);
int rotate_log(TfsClient* tfs_client, VSTRING& param);

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

void init()
{
  g_cmd_map["param"] = set_run_param;
  g_cmd_map["removeblock"] = remove_block;
  g_cmd_map["expblk"] = expire_block;
  g_cmd_map["ueblk"] = unexpire_block;
  g_cmd_map["compact"] = compact_block;
  g_cmd_map["repairblk"] = repair_lose_block;
  g_cmd_map["repairgrp"] = repair_group_block;
  g_cmd_map["repaircrc"] = repair_crc;
  g_cmd_map["aci"] = access_stat_info;
  g_cmd_map["setacl"] = access_control_flag;
  g_cmd_map["help"] = show_help;
  g_cmd_map["quit"] = quit;
  g_cmd_map["exit"] = quit;
  g_cmd_map["rotatelog"] = rotate_log;

}

int set_run_param(TfsClient* tfs_client, VSTRING& param)
{
  const char* param_str[] = {
    "min_replication",
    "max_replication",
    "max_write_file_count",
    "max_use_capacity_ratio",
    "heart_interval",
    "replicate_wait_time",
    "compact_delete_ratio",
    "compact_max_load",
    "plan_run_flag",
    "run_plan_expire_interval",
    "run_plan_ratio",
    "object_dead_max_time",
    "balance_max_diff_block_num",
    "log_level",
    "add_primary_block_count",
    "build_plan_interval",
    "replicate_ratio",
    "max_wait_write_lease",
    "cluster_index",
    "build_plan_default_wait_time"
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
  req_cc_msg.set_cmd(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_value3(index);
  req_cc_msg.set_value1(value);
  string err_msg;

  //uint64_t nsip_port = tfs_client.get_ns_ip_port();
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  fprintf(stderr, "%s\n\n", err_msg.c_str());
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int remove_block(TfsClient* tfs_client, VSTRING& param)
{
/*  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "removeblock block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
  VUINT64 ds_list;
  ds_list.clear();
  int32_t ret = tfs_client.get_block_info(block_id, ds_list);
  if (ret != TFS_SUCCESS)
  {
    fprintf(stderr, "block no exist in nameserver, blockid:%u.\n", block_id);
    return ret;
  }
  fprintf(stdout, "------block: %u, has %d replicas------\n", block_id, static_cast<int32_t> (ds_list.size()));
  //remove meta
  ds_list.push_back(0);
  for (uint32_t i = 0; i < ds_list.size(); ++i)
  {
    if (i < ds_list.size() - 1)
    {
      fprintf(stdout, "removeblock: %u, (%d)th server: %s \n", block_id, i,
          tbsys::CNetUtil::addrToString(ds_list[i]).c_str());
    }

    //uint64_t nsip_port = tfs_client.get_ns_ip_port();
 
    ClientCmdMessage req_cc_msg;
    req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
    req_cc_msg.set_value1(ds_list[i]);
    req_cc_msg.set_value3(block_id);
    req_cc_msg.set_value4(0);
    req_cc_msg.set_value2(local_server_ip);
    string err_msg;
    NewClient* client = NewClientManager::get_instance().create_client();
    if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
    {
      fprintf(stderr, "%s\n\n", err_msg.c_str());
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }
    NewClientManager::get_instance().destroy_client(client);
  }
*/  return TFS_SUCCESS;
}

int expire_block(TfsClient* tfs_client, VSTRING& param)
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

  //uint64_t nsip_port = tfs_client.get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_EXPBLK);
  req_cc_msg.set_value1(server_id);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(0);
  req_cc_msg.set_value2(local_server_ip);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int unexpire_block(TfsClient* tfs_client, VSTRING& param)
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
//  uint64_t nsip_port = tfs_client.get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_LOADBLK);
  req_cc_msg.set_value1(server_id);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(0);
  req_cc_msg.set_value2(local_server_ip);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int compact_block(TfsClient* tfs_client, VSTRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "compact block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), reinterpret_cast<char**> (NULL), 10);
//  uint64_t nsip_port = tfs_client.get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_COMPACT);
  req_cc_msg.set_value1(0);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(0);
  req_cc_msg.set_value2(local_server_ip);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int repair_lose_block(TfsClient* tfs_client, VSTRING& param)
{
  int32_t size = param.size();
  int32_t action = 2;
  if (size != 1 && size != 4)
  {
    fprintf(stderr, "repairblk block_id [source] [dest] [action]\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), (char**) NULL, 10);
  uint64_t src_ns_id = 0;
  uint64_t dest_ns_id = 0;
  if (4 == size)
  {
    src_ns_id = trans_to_ns_id(const_cast<char*> (param[1].c_str()));
    dest_ns_id = trans_to_ns_id(const_cast<char*> (param[2].c_str()));
    action = atoi(param[3].c_str());
  }

  //uint64_t nsip_port = tfs_client.get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_IMMEDIATELY_REPL);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(action);
  req_cc_msg.set_value2(src_ns_id);
  req_cc_msg.set_value1(dest_ns_id);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int repair_group_block(TfsClient* tfs_client, VSTRING& param)
{
  int32_t size = param.size();
  if (size != 1)
  {
    fprintf(stderr, "repairgrp block_id\n\n");
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(param[0].c_str(), (char**) NULL, 10);
  //uint64_t nsip_port = tfs_client.get_ns_ip_port();

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_REPAIR_GROUP);
  req_cc_msg.set_value3(block_id);
  req_cc_msg.set_value4(0);
  req_cc_msg.set_value1(local_server_ip);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int repair_crc(TfsClient* tfs_client, VSTRING& param)
{
/*  int32_t size = param.size();
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
    ret = tfs_client.save_file(local_file, tfs_name, NULL);
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

  tfs_client.tfs_close();
  return ret;
*/
  return 0;
}

int access_stat_info(TfsClient*, VSTRING& param)
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
  uint64_t ns_id = Func::str_to_addr(ip, port);
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
    tbnet::Packet* ret_message = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    send_message_to_server(ns_id, client, &req_gss_msg, err_msg, &ret_message);
    if (ret_message == NULL)
    {
      NewClientManager::get_instance().destroy_client(client);
      return TFS_ERROR;
    }
    if (ret_message->getPCode() == ACCESS_STAT_INFO_MESSAGE)
    {
      AccessStatInfoMessage* req_cb_msg = reinterpret_cast<AccessStatInfoMessage*> (ret_message);
      const AccessStatInfoMessage::COUNTER_TYPE & m = req_cb_msg->get();
      for (AccessStatInfoMessage::COUNTER_TYPE::const_iterator it = m.begin(); it != m.end(); ++it)
      {
        printf("%15s : %14" PRI64_PREFIX "u %14s %14" PRI64_PREFIX "u %14s\n",
            tbsys::CNetUtil::addrToString(it->first).c_str(),
            it->second.read_file_count_, Func::format_size(it->second.read_byte_).c_str(),
            it->second.write_file_count_, Func::format_size(it->second.write_byte_).c_str());
      }

      has_next = req_cb_msg->has_next();

    }
    else if (ret_message->getPCode() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*> (ret_message);
      if (s_msg->get_error() != NULL)
      {
        printf("%s\n", s_msg->get_error());
      }
      break;
    }
    NewClientManager::get_instance().destroy_client(client);

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
int access_control_flag(TfsClient*, VSTRING& param)
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
  uint64_t ns_id = Func::str_to_addr(ip, port);
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
  req_cc_msg.set_cmd(CLIENT_CMD_SET_PARAM);
  req_cc_msg.set_value3(op_type); // param type == 1 as set acl flag.
  req_cc_msg.set_value1(v1); // ns_id as flag
  req_cc_msg.set_value4(v2);
  req_cc_msg.set_value2(local_server_ip);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(ns_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int rotate_log(TfsClient* tfs_client, VSTRING& param)
{
  int32_t size = param.size();
  if (size > 1)
  {
    fprintf(stderr, "rotatelog ip:port\n\n");
    return TFS_ERROR;
  }
  //uint64_t ns_id = tfs_client.get_ns_ip_port(); 
  uint64_t server_id = ns_id;

  if (size == 1)
  {
    server_id = trans_to_ns_id(const_cast<char*> (param[0].c_str()));
  }

  ClientCmdMessage req_cc_msg;
  req_cc_msg.set_cmd(CLIENT_CMD_ROTATE_LOG);
  string err_msg;
  NewClient* client = NewClientManager::get_instance().create_client();
  if (send_message_to_server(server_id, client, &req_cc_msg, err_msg) == TFS_ERROR)
  {
    fprintf(stderr, "%s\n\n", err_msg.c_str());
    NewClientManager::get_instance().destroy_client(client);
    return TFS_ERROR;
  }
  NewClientManager::get_instance().destroy_client(client);

  return TFS_SUCCESS;
}

int show_help(TfsClient*, VSTRING&)
{
  fprintf(stdout, "\nsupported command:\n");
  fprintf(stdout,
      "param param_name [set value]\n"
      "removeblock block_id\n"
      "expblk ip:port block_id\n"
      "ueblk ip:port block_id\n"
      "compact block_id\n"
      "repairblk block_id [source] [dest] [action]\n"
      "repairgrp block_id\n"
      "repaircrc filename\n"
      "aci ip:port [startrow returnrow]\n"
      "setacl ip:port type [v1 v2]\n"
      "rotatelog [ip:port]\n");      
  fprintf(stdout, "quit      quit\n");
  fprintf(stdout, "exit      exit\n");
  fprintf(stdout, "help      show help info\n\n");
  return TFS_SUCCESS;
}

int quit(TfsClient*, VSTRING&)
{
  return TFS_CLIENT_QUIT;
}
int send_message_to_server(uint64_t server, NewClient* client, tbnet::Packet* msg, string& err_msg, tbnet::Packet** output/*not free*/, const int64_t timeout)
{
  tbnet::Packet* ret_msg = NULL;
  int32_t iret = tfs::common::send_msg_to_server(server, client, msg, ret_msg);
  if (TFS_SUCCESS == iret)
  {
    if (output != NULL)
    {
      *output = ret_msg;
    }
    else
    {
      if (ret_msg->getPCode() == STATUS_MESSAGE)
      {
        StatusMessage* s_msg = dynamic_cast<StatusMessage*>(ret_msg);
        if (STATUS_MESSAGE_OK != s_msg->get_status())
        {
          iret = s_msg->get_status();
        }
        if (s_msg->get_error() != NULL)
        {
          err_msg = s_msg->get_error();
        }
      }
      else
      {
        iret = TFS_ERROR;
        TBSYS_LOG(ERROR, "send message to server(%s) error, message(%d:%d) is invalid",
            tbsys::CNetUtil::addrToString(server).c_str(), msg->getPCode(), ret_msg->getPCode());
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "send message to server(%s) error: receive message error or timeout",
        tbsys::CNetUtil::addrToString(server).c_str());

  }
  return iret;
}

uint64_t trans_to_ns_id(char* ipport)
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

int get_file_retry(TfsClient* tfs_client, char* tfs_name, char* local_file)
{
  fprintf(stderr, "filename: %s\n", tfs_name);
  fflush( stderr);
  int tfs_fd = 0;
  tfs_fd = tfs_client->open(tfs_name, NULL, T_READ);
  if (tfs_fd < 0)
  {
    fprintf(stderr, "open tfs_client fail\n");
    return TFS_ERROR;
  }
  TfsFileStat file_info;
  if (tfs_client->fstat(tfs_fd, &file_info) == TFS_ERROR)
  {
    tfs_client->close(tfs_fd);
    fprintf(stderr, "fstat tfs_client fail\n");
    return TFS_ERROR;
  }

  int32_t done = 0;
  while (done >= 0 && done <= 10)
  {
    int64_t t1 = Func::curr_time();
    int32_t fd = open(local_file, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (-1 == fd)
    {
      fprintf(stderr, "open local file fail: %s\n", local_file);
      tfs_client->close(tfs_fd);
      return TFS_ERROR;
    }

    char data[MAX_READ_SIZE];
    uint32_t crc = 0;
    int32_t total_size = 0;
    for (;;)
    {
      int32_t read_len = tfs_client->read(tfs_fd, data, MAX_READ_SIZE);
      if (read_len < 0)
      {
        fprintf(stderr, "read tfs_client fail\n");
        break;
      }
      if (0 == read_len)
      {
        break;
      }
      if (write(fd, data, read_len) != read_len)
      {
        fprintf(stderr, "write local file fail: %s\n", local_file);
        tfs_client->close(tfs_fd);
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
      tfs_client->close(tfs_fd);
      return TFS_SUCCESS;
    }
    //TBSYS_LOG(ERROR, "%s %s", tbsys::CNetUtil::addrToString(tfs_client->get_last_elect_ds_id()).c_str(), tfs_name);
    int64_t t2 = Func::curr_time();
    if (t2 - t1 > 500000)
    {
      //fprintf(stderr, "filename: %s, time: %" PRI64_PREFIX "d, server: %s, done: %d\n", tfs_name, t2 - t1,
       //   tbsys::CNetUtil::addrToString(tfs_client->get_last_elect_ds_id()).c_str(), done);
      fflush(stderr);
    }
    done++;
    /*if (tfs_client->tfs_reset_read() <= done)
    {
      break;
    }*/
  }
  tfs_client->close(tfs_fd);
  return TFS_ERROR;
}

int main(int argc,char** argv)
{
  //TODO readline
  int32_t i;
  std::string dev_name;
  bool directly = false;
  while ((i = getopt(argc, argv, "s:d:ih")) != EOF)
  {
    switch (i)
    {
      case 's':
        ns_ip_port = optarg;
        break;
      case 'd':
        dev_name = optarg;
        break;
      case 'i':
        directly = true;
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

  TfsClient* tfs_client = TfsClient::Instance();
  int iret = tfs_client->initialize(ns_ip_port.c_str());
  if (iret != TFS_SUCCESS)
  {
    return TFS_ERROR;
  }
  ns_id = trans_to_ns_id(const_cast<char*> (ns_ip_port.c_str()));

  static MessageFactory Factory;
  static BasePacketStreamer Streamer;
  Streamer.set_packet_factory(&Factory);
  NewClientManager::get_instance().initialize(&Factory, &Streamer);

  local_server_ip = tbsys::CNetUtil::getLocalAddr(dev_name.c_str());

  if (optind >= argc)
  {
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);
    main_loop(tfs_client);
  }
  else
  {
    if (directly)
    {
      for (i = optind; i < argc; i++)
      {
        do_cmd(tfs_client, argv[i]);
      }
    }
    else
    {
      usage(argv[0]);
    }
  }
}

int usage(const char *name)
{
  fprintf(stderr, "\n****************************************************************************** \n");
  fprintf(stderr, "You can operate nameserver by this tool.\n");
  fprintf(stderr, "Usage: \n");
  fprintf(stderr, "  %s -s ns_ip_port [-d dev_name] [-i 'command'] [-h help]\n", name);
  fprintf(stderr, "****************************************************************************** \n");
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
    default:
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

int main_loop(TfsClient* tfs_client)
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
    std::string tips = "";
    tips = "TFS > ";
#ifdef _WITH_READ_LINE
    cmd_line = readline(tips.c_str());
    if (!cmd_line)
#else
    fprintf(stderr, tips.c_str());

    if (NULL == fgets(cmd_line, CMD_MAX_LEN, stdin))
#endif
    {
      break;
    }
    ret = do_cmd(tfs_client, cmd_line);
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

int32_t do_cmd(TfsClient* tfs_client, char* key)
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
      break;
    }
    param.push_back(token);
  }

  return it->second(tfs_client, param);
}
