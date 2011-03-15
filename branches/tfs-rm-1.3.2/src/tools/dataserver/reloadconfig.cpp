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
 *
 */
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string>

#include "common/define.h"
#include "common/config.h"
#include "message/client.h"
#include "message/client_pool.h"
#include "message/message.h"
#include "message/message_factory.h"

using namespace tfs::common;
using namespace tfs::message;

int reload_config(const uint64_t server_ip, const int32_t flag)
{
  Client* client = CLIENT_POOL.get_client(server_ip);
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }

  printf("server_ip: %s,  flag: %u\n", tbsys::CNetUtil::addrToString(server_ip).c_str(), flag);

  int ret_status = TFS_ERROR;
  ReloadConfigMessage req_rc_msg;

  req_rc_msg.set_switch_cluster_flag(flag);

  Message* message = client->call(&req_rc_msg);
  if (message != NULL)
  {
    printf("getmessagetype: %d\n", message->get_message_type());
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*>(message);
      printf("getMessageType: %d %d==%d\n", message->get_message_type(), s_msg->get_status(), STATUS_MESSAGE_OK);
      if (STATUS_MESSAGE_OK == s_msg->get_status())
      {
        ret_status = TFS_SUCCESS;
      }
    }
    delete message;
  }
  CLIENT_POOL.release_client(client);
  return ret_status;
}

uint64_t get_ip_addr(const char* ip)
{
  char* port_str = strchr(const_cast<char*>(ip), ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "%s invalid format ip:port\n", ip);
    return 0;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  return Func::str_to_addr(ip, port);
}

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    printf("Usage: %s srcip:port switchclusterflag(default:1 switch. 0: no switch)\n\n", argv[0]);
    return TFS_ERROR;
  }

  uint64_t server_ip = get_ip_addr(argv[1]);
  if (0 == server_ip)
  {
    return TFS_ERROR;
  }
  int32_t flag = 1;
  if (argc >= 3)
  {
    flag = strtoul(argv[2], reinterpret_cast<char**>(NULL), 10);
  }

  int ret = reload_config(server_ip, flag);
  if (ret == TFS_SUCCESS)
  {
    printf("reload Config Success.\n\n");
  }
  else
  {
    printf("reload Config Fail ret=%d.\n\n", ret);
  }

  return ret;
}
