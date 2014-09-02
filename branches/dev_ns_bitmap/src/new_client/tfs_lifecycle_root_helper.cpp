/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *      qixiao.zs(qixiao.zs@alibaba-inc.com)
 *      - initial release
 *
 */
#include "tfs_lifecycle_root_helper.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/expire_message.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;


int LifeCycleHelper::do_query_task(const uint64_t server_id, const uint64_t es_id,
  std::vector<common::ServerExpireTask>* p_res_task)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_LIFECYCLE_ROOT_SERVER;
  }
  else
  {
    ReqQueryTaskMessage req_qt_msg;
    req_qt_msg.set_es_id(es_id);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_qt_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call query task fail,"
          "server_addr: %s,"
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_QUERY_TASK_MESSAGE == rsp->getPCode())
    {
      RspQueryTaskMessage *rsp_qt_msg = dynamic_cast<RspQueryTaskMessage*>(rsp);
      *p_res_task = *(rsp_qt_msg->get_mutable_running_tasks_info());
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "query task return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "query task fail,"
          "server_addr: %s,"
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

