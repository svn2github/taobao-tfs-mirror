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

int KvMetaHelper::get_table(const uint64_t server_id,
    KvMetaTable &table)
{
  GetTableFromKvRtsMessage req_gtfkr_msg;

  tbnet::Packet* rsp = NULL;
  NewClient *client = NewClientManager::get_instance().create_client();

  int iret = send_msg_to_server(server_id, client, &req_gtfkr_msg, rsp, ClientConfig::wait_timeout_);

  if (TFS_SUCCESS != iret)
  {
    TBSYS_LOG(ERROR, "call get kv table fail,"
        "server_addr: %s, ret: %d",
        tbsys::CNetUtil::addrToString(server_id).c_str(), iret);
    iret = EXIT_NETWORK_ERROR;
  }
  else if (RSP_KV_RT_GET_TABLE_MESSAGE == rsp->getPCode())
  {
    GetTableFromKvRtsResponseMessage* rsp_get_table = dynamic_cast<GetTableFromKvRtsResponseMessage*>(rsp);
    table.v_meta_table_ = rsp_get_table->get_mutable_table().v_meta_table_;
  }
  else
  {
    iret = EXIT_UNKNOWN_MSGTYPE;
    TBSYS_LOG(ERROR, "call get kv table fail,"
        "server_addr: %s, ret: %d: msg type: %d",
        tbsys::CNetUtil::addrToString(server_id).c_str(), iret, rsp->getPCode());
  }
  NewClientManager::get_instance().destroy_client(client);
  return iret;
}

int KvMetaHelper::do_put_bucket(const uint64_t server_id, const char *bucket_name,
    const BucketMetaInfo& bucket_meta_info, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaPutBucketMessage req_pb_msg;
    req_pb_msg.set_bucket_name(bucket_name);
    req_pb_msg.set_bucket_meta_info(bucket_meta_info);
    req_pb_msg.set_user_info(user_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_pb_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call put bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret);
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
          "server_addr: %s, bucket_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_get_bucket(const uint64_t server_id, const char *bucket_name,
                                const char *prefix, const char *start_key, char delimiter,
                                const int32_t limit, vector<ObjectMetaInfo> *v_object_meta_info,
                                vector<string> *v_object_name, set<string> *s_common_prefix,
                                int8_t *is_truncated, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name)
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
    req_gb_msg.set_user_info(user_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_gb_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call get bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret);
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
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "get bucket return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "get bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_del_bucket(const uint64_t server_id, const char *bucket_name, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaDelBucketMessage req_db_msg;
    req_db_msg.set_bucket_name(bucket_name);
    req_db_msg.set_user_info(user_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_db_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call del bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret);
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
          "server_addr: %s, bucket_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;

}

int KvMetaHelper::do_head_bucket(const uint64_t server_id, const char *bucket_name, BucketMetaInfo *bucket_meta_info, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaHeadBucketMessage req_hb_msg;
    req_hb_msg.set_bucket_name(bucket_name);
    req_hb_msg.set_user_info(user_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_hb_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call head bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_HEAD_BUCKET_MESSAGE == rsp->getPCode())
    {
      RspKvMetaHeadBucketMessage *resp_hb_msg = dynamic_cast<RspKvMetaHeadBucketMessage*>(rsp);
      *bucket_meta_info = *(resp_hb_msg->get_bucket_meta_info());
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "head bucket return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "head bucket fail,"
          "server_addr: %s, bucket_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}


int KvMetaHelper::do_put_object(const uint64_t server_id, const char *bucket_name,const char *object_name,
    const ObjectInfo &object_info, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaPutObjectMessage req_po_msg;
    req_po_msg.set_bucket_name(bucket_name);
    req_po_msg.set_file_name(object_name);
    req_po_msg.set_object_info(object_info);
    req_po_msg.set_user_info(user_info);


    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_po_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call put object fail,"
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret);
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
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_get_object(const uint64_t server_id, const char *bucket_name, const char *object_name,
const int64_t offset, const int64_t length, ObjectInfo *object_info, bool *still_have, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name || NULL == object_name)
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
    req_go_msg.set_user_info(user_info);
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
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_del_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name, ObjectInfo *object_info, bool *still_have, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaDelObjectMessage req_do_msg;
    req_do_msg.set_bucket_name(bucket_name);
    req_do_msg.set_file_name(object_name);
    req_do_msg.set_user_info(user_info);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_do_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call del object fail,"
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_DEL_OBJECT_MESSAGE == rsp->getPCode())
    {
      RspKvMetaDelObjectMessage* rsp_do_msg = dynamic_cast<RspKvMetaDelObjectMessage*>(rsp);
      *object_info = rsp_do_msg->get_object_info();
      *still_have = rsp_do_msg->get_still_have();
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
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(),
          bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_head_object(const uint64_t server_id, const char *bucket_name,
    const char *object_name, ObjectInfo *object_info, const UserInfo &user_info)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == bucket_name || NULL == object_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaHeadObjectMessage req_ho_msg;
    req_ho_msg.set_bucket_name(bucket_name);
    req_ho_msg.set_file_name(object_name);
    req_ho_msg.set_user_info(user_info);
    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_ho_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call head object fail,"
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), bucket_name, object_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_HEAD_OBJECT_MESSAGE == rsp->getPCode())
    {
      RspKvMetaHeadObjectMessage* resp_ho_msg = dynamic_cast<RspKvMetaHeadObjectMessage*>(rsp);
      *object_info = *(resp_ho_msg->get_object_info());
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "head object return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "head object fail,"
          "server_addr: %s, bucket_name: %s, "
          "object_name: %s, ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(),
          bucket_name, object_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_set_life_cycle(const uint64_t server_id,
                                    const int32_t file_type, const char *file_name,
                                    const int32_t invalid_time_s, const char *app_key)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == file_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaSetLifeCycleMessage req_set_lifecycle_msg;
    req_set_lifecycle_msg.set_file_type(file_type);
    req_set_lifecycle_msg.set_file_name(file_name);
    req_set_lifecycle_msg.set_invalid_time_s(invalid_time_s);
    req_set_lifecycle_msg.set_app_key(app_key);
    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_set_lifecycle_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call set lifecycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "set lifecycle return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "set lifecycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_get_life_cycle(const uint64_t server_id, const int32_t file_type,
                                       const char* file_name, int32_t* invalid_time_s)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == file_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaGetLifeCycleMessage req_glc_msg;
    req_glc_msg.set_file_type(file_type);
    req_glc_msg.set_file_name(file_name);


    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_glc_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call get lifecycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (RSP_KVMETA_GET_LIFE_CYCLE_MESSAGE == rsp->getPCode())
    {
      RspKvMetaGetLifeCycleMessage* rsp_glc_msg = dynamic_cast<RspKvMetaGetLifeCycleMessage*>(rsp);
      *invalid_time_s = rsp_glc_msg->get_invalid_time_s();
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "get lifecycle return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "get lifecycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;
}

int KvMetaHelper::do_rm_life_cycle(const uint64_t server_id, const int32_t file_type, const char *file_name)
{
  int ret = TFS_SUCCESS;
  if (0 == server_id)
  {
    ret = EXIT_INVALID_KV_META_SERVER;
  }
  else if (NULL == file_name)
  {
    ret = EXIT_INVALID_FILE_NAME;
  }
  else
  {
    ReqKvMetaRmLifeCycleMessage req_rlc_msg;
    req_rlc_msg.set_file_type(file_type);
    req_rlc_msg.set_file_name(file_name);

    tbnet::Packet* rsp = NULL;
    NewClient* client = NewClientManager::get_instance().create_client();
    ret = send_msg_to_server(server_id, client, &req_rlc_msg, rsp, ClientConfig::wait_timeout_);
    if (TFS_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "call rm life cycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret);
      ret = EXIT_NETWORK_ERROR;
    }
    else if (STATUS_MESSAGE == rsp->getPCode())
    {
      StatusMessage* resp_status_msg = dynamic_cast<StatusMessage*>(rsp);
      if ((ret = resp_status_msg->get_status()) != STATUS_MESSAGE_OK)
      {
        TBSYS_LOG(ERROR, "rm life cycle return error, ret: %d", ret);
      }
    }
    else
    {
      ret = EXIT_UNKNOWN_MSGTYPE;
      TBSYS_LOG(ERROR, "rm life cycle fail,"
          "server_addr: %s, file_name: %s, "
          "ret: %d, msg type: %d",
          tbsys::CNetUtil::addrToString(server_id).c_str(), file_name, ret, rsp->getPCode());
    }
    NewClientManager::get_instance().destroy_client(client);
  }
  return ret;

}
