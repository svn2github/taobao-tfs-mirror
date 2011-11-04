/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: replblk.cpp 432 2011-06-08 07:06:11Z nayan@taobao.com $
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
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/replicate_block_message.h"

using namespace tfs::common;
using namespace tfs::message;

int replicate_block(uint64_t server_ip, uint64_t dest_ip, uint32_t block_id)
{

  ReplicateBlockMessage req_rb_msg;
  int32_t ret_status = TFS_ERROR;

  ReplBlock repl_block;
  memset(&repl_block, 0, sizeof(ReplBlock));
  repl_block.source_id_ = server_ip;
  repl_block.destination_id_ = dest_ip;
  repl_block.block_id_ = block_id;
  repl_block.server_count_ = 1;
  printf("server_ip: %s, descip: %s, block_id: %u\n", tbsys::CNetUtil::addrToString(repl_block.source_id_).c_str(),
      tbsys::CNetUtil::addrToString(repl_block.destination_id_).c_str(), repl_block.block_id_);

  req_rb_msg.set_command(PLAN_STATUS_BEGIN);
  req_rb_msg.set_repl_block(&repl_block);

  NewClient* client = NewClientManager::get_instance().create_client();
  tbnet::Packet* ret_msg = NULL;
  ret_status = send_msg_to_server(server_ip, client, &req_rb_msg, ret_msg);

  if (TFS_SUCCESS == ret_status)
  {
    printf("getMessageType: %d\n", ret_msg->getPCode());
    if (STATUS_MESSAGE == ret_msg->getPCode())
    {
      StatusMessage* s_msg = dynamic_cast<StatusMessage*>(ret_msg);
      if (STATUS_MESSAGE_OK == s_msg->get_status())
      {
        ret_status = TFS_SUCCESS;
      }
    }
  }
  NewClientManager::get_instance().destroy_client(client);
  return ret_status;
}

uint64_t get_ip_addr(char* ip)
{
  char* port_str = strchr(ip, ':');
  if (NULL == port_str)
  {
    fprintf(stderr, "%s is not correct, ip:port\n", ip);
    return 0;
  }
  *port_str = '\0';
  int32_t port = atoi(port_str + 1);
  return Func::str_to_addr(ip, port);
}

int main(int argc, char* argv[])
{
  if (argc != 4)
  {
    printf("Usage: %s srcip:port destip:port block_id\n\n", argv[0]);
    return TFS_ERROR;
  }

  uint64_t server_ip = get_ip_addr(argv[1]);
  uint64_t dest_ip = get_ip_addr(argv[2]);
  if (0 == server_ip || 0 == dest_ip)
  {
    return TFS_ERROR;
  }
  uint32_t block_id = strtoul(argv[3], reinterpret_cast<char**>(NULL), 10);

  int32_t ret = replicate_block(server_ip, dest_ip, block_id);
  if (ret == TFS_SUCCESS)
  {
    printf("ret=Success.\n\n");
  }
  else
  {
    printf("ret=Failure.\n\n");
  }

  return TFS_SUCCESS;
}
