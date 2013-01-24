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
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaPutBucketMessage req_pb_msg;
    req_pb_msg.set_bucket_name(bucket_name);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_pb_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call put bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d",
          server_id, bucket_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "put bucket return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "put bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d, msg type: %d",
          server_id, bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_get_bucket(const uint64_t server_id, const char *bucket_name,
                                const char *prefix, const char *start_key, char delimiter,
                                const int32_t limit, vector<ObjectMetaInfo> *v_object_meta_info,
                                vector<string> *v_object_name, set<string> *s_common_prefix,
                                int8_t *is_truncated)
{
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaGetBucketMessage req_gb_msg;
    req_gb_msg.set_bucket_name(bucket_name);
    req_gb_msg.set_prefix(NULL == prefix ? "" : string(prefix));
    req_gb_msg.set_start_key(NULL == start_key ? "" : string(start_key));
    req_gb_msg.set_delimiter(delimiter);
    req_gb_msg.set_limit(limit);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_gb_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call get bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d",
          server_id, bucket_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_GET_BUCKET_MESSAGE == rsp->getPCode())
    {
      RspKvMetaGetBucketMessage* rsp_gb_msg = dynamic_cast<RspKvMetaGetBucketMessage*>(rsp);
      *v_object_meta_info = *(rsp_gb_msg->get_mutable_v_object_meta_info());
      *v_object_name = *(rsp_gb_msg->get_mutable_v_object_name());
      *s_common_prefix = *(rsp_gb_msg->get_mutable_s_common_prefix());
      *is_truncated = *(rsp_gb_msg->get_mutable_truncated());
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "get bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d, msg type: %d",
          server_id, bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_del_bucket(const uint64_t server_id, const char *bucket_name)
{
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaDelBucketMessage req_db_msg;
    req_db_msg.set_bucket_name(bucket_name);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_db_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call del bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d",
          server_id, bucket_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "del bucket return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "del bucket fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "ret: %d, msg type: %d",
          server_id, bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;

}

int KvMetaHelper::do_head_bucket(const uint64_t server_id, const char *bucket_name, BucketMetaInfo *bucket_meta_info)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  UNUSED(bucket_meta_info);
 //todo
 return TFS_SUCCESS;

}


int KvMetaHelper::do_put_object(const uint64_t server_id, const char *bucket_name,const char *object_name,
    const ObjectInfo &object_info)
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
    req_po_msg.set_file_name(object_name);
    req_po_msg.set_object_info(object_info);


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

int KvMetaHelper::do_get_object(const uint64_t server_id, const char *bucket_name, const char *object_name,
const int64_t offset, const int64_t length, ObjectInfo *object_info, bool *still_have)
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
    req_go_msg.set_file_name(object_name);
    req_go_msg.set_offset(offset);
    req_go_msg.set_length(length);
    TBSYS_LOG(DEBUG, "bucket_name %s object_name %s "
            "offset %ld length %ld still_have %d", bucket_name, object_name, offset, length, *still_have);
    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_go_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call get object fail,"
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_GET_OBJECT_MESSAGE == rsp->getPCode())
    {
      RspKvMetaGetObjectMessage* rsp_go_msg = dynamic_cast<RspKvMetaGetObjectMessage*>(rsp);
      *object_info = rsp_go_msg->get_object_info();
      *still_have = rsp_go_msg->get_still_have();
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
      TBSYS_LOG(ERROR, "get object fail, "
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
  int ret = TFS_SUCCESS;
  if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaDelObjectMessage req_do_msg;
    req_do_msg.set_bucket_name(bucket_name);
    req_do_msg.set_file_name(object_name);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_do_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call del object fail,"
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
        TBSYS_LOG(ERROR, "del object return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "del object fail,"
          "server_id: %"PRI64_PREFIX"u, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          server_id, bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_head_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name, ObjectInfo *object_info)
{
  UNUSED(server_id);
  UNUSED(bucket_name);
  UNUSED(object_name);
  UNUSED(object_info);
 //todo
 return TFS_SUCCESS;
}

