/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#include "common/expire_define.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "message/message_factory.h"
#include "new_client/client_config.h"
#include "common/func.h"

using namespace tfs::message;
using namespace tfs::client;
using namespace tfs::common;

static tfs::message::MessageFactory gfactory;
static tfs::common::BasePacketStreamer gstreamer;

int usage(const char *s)
{
  printf("%s lifecycle_rootserver(xx.xx.xx.xx:xx) expire_server(xx.xx.xx.xx:xx) stat_time('2013-08-05 13-00-00') type(1: raw, 2: meta, 3: s3)", s);
  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 5)
  {
    usage(argv[0]);
    return 1;
  }

  gstreamer.set_packet_factory(&gfactory);
  NewClientManager::get_instance().initialize(&gfactory, &gstreamer);

  const char *root_ipport = argv[1];
  const char *local_ipport = argv[2];
  char *stat_time = argv[3];
  ExpireTaskType type = static_cast<ExpireTaskType>(atoi(argv[4]));

  uint64_t root_id = Func::get_host_ip(root_ipport);
  uint64_t local_id = Func::get_host_ip(local_ipport);
  int32_t spec_time = tbsys::CTimeUtil::strToTime(stat_time);

  printf("root_id: %"PRI64_PREFIX"d, es_id: %"PRI64_PREFIX"d, spec_time: %d, type: %d\n", root_id, local_id, spec_time, type);

  int32_t percent = 0;
  ReqQueryProgressMessage req_qp_msg;
  req_qp_msg.set_es_id(local_id);
  req_qp_msg.set_es_num(0);
  req_qp_msg.set_task_time(spec_time);
  req_qp_msg.set_hash_bucket_id(0);
  req_qp_msg.set_expire_task_type(type);

  tbnet::Packet *rsp = NULL;
  NewClient *client = NewClientManager::get_instance().create_client();
  int ret = send_msg_to_server(root_id, client, &req_qp_msg, rsp, ClientConfig::wait_timeout_);

  if (TFS_SUCCESS != ret)
  {
    printf("call query progress fail, ret: %d\n", ret);
    ret = EXIT_NETWORK_ERROR;
  }
  else if (RSP_QUERY_PROGRESS_MESSAGE == rsp->getPCode())
  {
    RspQueryProgressMessage *rsp_qp_msg = dynamic_cast<RspQueryProgressMessage*>(rsp);
    percent = rsp_qp_msg->get_current_percent();
    printf("query progress success, current percent: %d\%%\n", percent);
  }
  else
  {
    ret = EXIT_UNKNOWN_MSGTYPE;
    printf("query progress error, ret: %d, msg type: %d\n", ret, rsp->getPCode());
  }
  NewClientManager::get_instance().destroy_client(client);

  return ret;
}
