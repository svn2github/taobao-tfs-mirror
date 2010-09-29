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
#include <Memory.hpp>

#include "common/func.h"
#include "message/client.h"
#include "message/client_pool.h"
#include "nameserver/ns_define.h"

using namespace tfs::message;
using namespace tfs::common;
using namespace tfs::nameserver;
namespace tfs
{
  namespace monitor
  {
    static NsStatus get_name_server_running_status(const uint64_t ip_port, const int32_t switch_flag)
    {
      Client *client = CLIENT_POOL.get_client(ip_port);
      if (client->connect() == EXIT_FAILURE)
      {
        CLIENT_POOL.release_client(client);
        TBSYS_LOG(ERROR, "connect nameserver(%s) failed", tbsys::CNetUtil::addrToString(ip_port).c_str());
        return NS_STATUS_NONE;
      }
      NsStatus status = NS_STATUS_NONE;
      HeartBeatAndNSHeartMessage heart_msg;
      heart_msg.set_ns_switch_flag_and_status(switch_flag, 0);
      Message *ret_msg = client->call(&heart_msg);
      if (ret_msg != NULL)
      {
        if (ret_msg->get_message_type() == HEARTBEAT_AND_NS_HEART_MESSAGE)
        {
          status = static_cast<NsStatus> (dynamic_cast<HeartBeatAndNSHeartMessage*> (ret_msg)->get_ns_status());
        }
        tbsys::gDelete(ret_msg);
      }
      CLIENT_POOL.release_client(client);
      return status;
    }
  }
}

using namespace tfs::monitor;

static void usage(const char* bin)
{
  fprintf(stderr, "usage: %s -i ipaddr -p port [-s swith flag on=1, off= 0] [-n]\n", bin);
}

int main(int argc, char *argv[])
{
  std::string ip;
  int32_t port = 0;
  int32_t i = 0;
  int32_t switch_flag = 0;
  bool set_log_level = false;
  while ((i = getopt(argc, argv, "ni:p:s:")) != EOF)
  {
    switch (i)
    {
    case 'i':
      ip = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'n':
      set_log_level = true;
      break;
    case 's':
      switch_flag = atoi(optarg);
      break;
    default:
      usage(argv[0]);
      return TFS_ERROR;
    }
  }

  if (ip.empty() || ip == " ")
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  if (port <= 0 || port > 0xffff)
  {
    usage(argv[0]);
    return TFS_ERROR;
  }
  if (set_log_level)
  {
    TBSYS_LOGGER.setLogLevel("ERROR");
  }
  uint64_t hostid = tbsys::CNetUtil::strToAddr(const_cast<char*> (ip.c_str()), port);
  NsStatus status = get_name_server_running_status(hostid, switch_flag);
  if ((status > NS_STATUS_UNINITIALIZE) && (status <= NS_STATUS_INITIALIZED))
  {
    return 0;
  }
  else
  {
    TBSYS_LOG(ERROR, "ping nameserver failed, get status(none)");
    return status;
  }
}
