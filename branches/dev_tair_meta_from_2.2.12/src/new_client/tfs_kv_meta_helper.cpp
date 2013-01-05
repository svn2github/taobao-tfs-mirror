/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_meta_helper.cpp 918 2011-10-14 03:16:21Z daoan@taobao.com $
 *
 * Authors:
 *      mingyan(mingyan.zc@taobao.com)
 *      - initial release
 *
 */
#include "tfs_kv_meta_helper.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "message/kv_meta_message.h"
#include "client_config.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

int KvMetaHelper::do_put_bucket(const uint64_t server_id, const char *bucket_name)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  int ret = TFS_SUCCESS;
  return ret;
}

int KvMetaHelper::do_get_bucket(const uint64_t server_id, const char *bucket_name)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  int ret = TFS_SUCCESS;
  return ret;
}

int KvMetaHelper::do_del_bucket(const uint64_t server_id, const char *bucket_name)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  int ret = TFS_SUCCESS;
  return ret;
}

int KvMetaHelper::do_put_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name, const TfsFileInfo &tfs_file_info)
{
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaPutObjectMessage req_po_msg;
    req_po_msg.set_bucket_name(bucket_name);
    //req_po_msg.set_object_name(object_name);
    req_po_msg.set_file_name(object_name);
    req_po_msg.set_file_info(tfs_file_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_po_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call put object fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "object_name: %s, ret: %d",
          server_id, bucket_name, object_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "put object return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "put object fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          server_id, bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_get_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name, TfsFileInfo &tfs_file_info)
{
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaGetObjectMessage req_go_msg;
    req_go_msg.set_bucket_name(bucket_name);
    //req_go_msg.set_object_name(object_name);
    req_go_msg.set_file_name(object_name);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_go_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call get object fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "object_name: %s, ret: %d",
          server_id, bucket_name, object_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_GET_OBJECT_MESSAGE == rsp->getPCode())
    {
      RspKvMetaGetObjectMessage* rsp_msg = dynamic_cast<RspKvMetaGetObjectMessage*>(rsp);
      tfs_file_info = rsp_msg->get_file_info();
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "get object return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "get object fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          server_id, bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_del_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  UNUSED(object_name);
  int ret = TFS_SUCCESS;
  return ret;
}
