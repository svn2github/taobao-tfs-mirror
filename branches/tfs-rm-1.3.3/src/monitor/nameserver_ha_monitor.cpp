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
#include "message/client_manager.h"
#include "message/new_client.h"
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
      const int64_t NETWORK_PACKET_TIMEOUT = 500;//500ms
      int iCount = 0;
      NsStatus status = NS_STATUS_NONE;
      do
      {
        Message* rmsg = NULL;
        HeartBeatAndNSHeartMessage heartMessage;    
        heartMessage.set_ns_switch_flag_and_status(switch_flag, 0);
        tfs::message::NewClient* client = NewClientManager::get_instance().create_client();
        int32_t iret = send_msg_to_server(ip_port, client, &heartMessage, rmsg, NETWORK_PACKET_TIMEOUT);
        if (TFS_SUCCESS == iret)
        {
          assert(NULL != rmsg);
          if (rmsg->get_message_type() == HEARTBEAT_AND_NS_HEART_MESSAGE)
          {
            HeartBeatAndNSHeartMessage* msg = dynamic_cast<HeartBeatAndNSHeartMessage*>(rmsg);
            status = static_cast<NsStatus>(msg->get_ns_status());
            break;
          }
        }
        ++iCount;
        TBSYS_LOG(ERROR, "call nameserver(%s) fail, count(%d)", tbsys::CNetUtil::addrToString(ip_port).c_str(), iCount);
        NewClientManager::get_instance().destroy_client(client);
      }
      while (iCount < 0x03);
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
  int32_t iret = NewClientManager::get_instance().initialize();
  if (TFS_SUCCESS == iret)
  {
    uint64_t hostid = tbsys::CNetUtil::strToAddr(const_cast<char*> (ip.c_str()), port);
    NsStatus status = get_name_server_running_status(hostid, switch_flag);
    iret = (status > NS_STATUS_UNINITIALIZE) && (status <= NS_STATUS_INITIALIZED)
          ? 0 : status;
    if (0 != iret)
    {
      TBSYS_LOG(ERROR, "ping nameserver failed, get status(none)");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "initialize client manager fail");
  }
  return iret;
}
