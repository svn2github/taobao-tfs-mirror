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
#include "common/func.h"
#include "message/client.h"
#include "message/client_pool.h"

using namespace tfs::common;
using namespace tfs::message;

int ping(const uint64_t ip)
{
  Client* client = CLIENT_POOL.get_client(ip);
  if (client->connect() == TFS_ERROR)
  {
    CLIENT_POOL.release_client(client);
    TBSYS_LOG(ERROR, "connect fail: %s", tbsys::CNetUtil::addrToString(ip).c_str());
    return TFS_ERROR;
  }
  int ret_status = TFS_ERROR;
  StatusMessage ping_message(STATUS_MESSAGE_PING);
  Message* message = client->call(&ping_message);
  if (message != NULL)
  {
    if (message->get_message_type() == STATUS_MESSAGE && (dynamic_cast<StatusMessage*> (message))->get_status() == STATUS_MESSAGE_PING)
    {
      ret_status = TFS_SUCCESS;
    }
    delete message;
  }
  CLIENT_POOL.release_client(client);
  return ret_status;
}

int main(int argc, char* argv[])
{
  if (argc != 3)
  {
    printf("%s ip port\n\n", argv[0]);
    return TFS_ERROR;
  }

  int32_t port = strtoul(argv[2], NULL, 10);
  uint64_t ip;
  IpAddr* adr = reinterpret_cast<IpAddr*>(&ip);
  adr->ip_ = Func::get_addr(argv[1]);
  adr->port_ = port;

  int ret = ping(ip);
  if (TFS_SUCCESS == ret)
  {
    printf("%s SUCCESS.\n", tbsys::CNetUtil::addrToString(ip).c_str());
  }
  else
  {
    printf("%s FAILURE.\n", tbsys::CNetUtil::addrToString(ip).c_str());
  }
  return ret;
}
