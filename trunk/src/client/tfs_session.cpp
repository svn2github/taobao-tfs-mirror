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
#include <tbsys.h>
#include <Memory.hpp>
#include "tfs_session.h"
#include "message/client_pool.h"
#include "message/client_cmd_message.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;
using namespace std;

TfsSession::TfsSession(const std::string& nsip, const int32_t cache_time, const int32_t cache_items)
  :ns_ip_port_(0), ns_ip_port_str_(nsip), block_cache_time_(cache_time), block_cache_items_(cache_items),
		cluster_id_(0), use_cache_(USE_CACHE_FLAG_YES)
{

}

TfsSession::~TfsSession()
{
  block_cache_map_.clear();
}

int TfsSession::get_cluster_id_from_ns()
{
  ClientCmdMessage ccmessage;
  ccmessage.set_type(CLIENT_CMD_SET_PARAM);
  ccmessage.set_block_id(20);

  Message* ret_msg = NULL;
  int32_t iret = send_message_to_server(ns_ip_port_, &ccmessage, &ret_msg);
  if (ret_msg == NULL || iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get cluster id from ns failed");
    return TFS_ERROR;
  }

  if (ret_msg->get_message_type() == STATUS_MESSAGE)
  {
    StatusMessage* status_msg = dynamic_cast<StatusMessage*> (ret_msg);
    if (status_msg->get_status() == STATUS_MESSAGE_OK)
    {
      const uint32_t len = strlen(status_msg->get_error());
      if (len > 0)
      {
        char cluster_id = static_cast<char> (atoi(status_msg->get_error()));
        if (isdigit(cluster_id) || isalpha(cluster_id))
        {
          cluster_id_ = cluster_id - '0';
        }
      }
    }
  }
  tbsys::gDelete(ret_msg);
  return TFS_SUCCESS;
}

int TfsSession::initialize()
{
  if ((ns_ip_port_str_.empty()) || (ns_ip_port_str_.compare(" ") == 0))
  {
    TBSYS_LOG(ERROR, "nameserver ip port(%s) invalid", ns_ip_port_str_.c_str());
    return TFS_ERROR;
  }
  IpAddr* addr = reinterpret_cast<IpAddr*> (&ns_ip_port_);
  memset(addr, 0, sizeof(IpAddr));
  string::size_type pos = ns_ip_port_str_.find_first_of(":");
  if (pos != string::npos)
  {
    string ip = ns_ip_port_str_.substr(0, pos);
    if (!ip.empty() && ip.size() > 0 && ip.compare(" ") != 0)
    {
      addr->ip_ = Func::get_addr(ip.c_str());
    }
  }
  string::size_type pos2 = ns_ip_port_str_.find_first_of(":", pos + 1);
  string port = ns_ip_port_str_.substr(pos + 1, pos2 - pos + 1);
  if (!port.empty() && port.size() > 0 && port != " ")
  {
    addr->port_ = atoi(port.c_str());
  }

  if (addr->ip_ <= 0 || addr->port_ <= 0)
  {
    TBSYS_LOG(ERROR, "nameserver ip port(%s) invalid", ns_ip_port_str_.c_str());
    return TFS_ERROR;
  }

  int32_t iret = get_cluster_id_from_ns();
  if (iret != TFS_SUCCESS || cluster_id_ <= 0)
  {
    TBSYS_LOG(ERROR, "cluster id: %d invalid, ret: %d", cluster_id_, iret);
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}

void TfsSession::destroy()
{
  tbutil::Mutex::Lock lock(mutex_);
  block_cache_map_.clear();
}

int TfsSession::get_block_info(uint32_t& block_id, VUINT64 &rds)
{
  if (block_id == 0)
  {
    TBSYS_LOG(ERROR, "block(%u) invalid", block_id);
    return TFS_ERROR;
  }

  // search in the local cache
  if (use_cache_ == USE_CACHE_FLAG_YES)
  {
    tbutil::Mutex::Lock lock(mutex_);
    BlockCache* block_cache = block_cache_map_.find(block_id);
    if (block_cache != NULL)
    {
      rds = block_cache->ds_;
      if (block_cache->last_time_ >= time(NULL) - block_cache_time_)
      {
        return TFS_SUCCESS;
      }
    }
  }

  BlockCache block_cache;
  VUINT64 fail_ds_list;
  if (get_block_info_ex(block_id, block_cache.ds_, BLOCK_READ, fail_ds_list) == TFS_SUCCESS)
  {
    block_cache.last_time_ = time(NULL);
    rds = block_cache.ds_;
    if (rds.size() <= 0)
    {
      TBSYS_LOG(ERROR, "get block(%u) info failed, dataserver size(%u) <= 0", block_id, rds.size());
      return TFS_ERROR;
    }

    tbutil::Mutex::Lock lock(mutex_);
    block_cache_map_.insert(block_id, block_cache);
    return TFS_SUCCESS;
  }
  return TFS_ERROR;
}

int TfsSession::create_block_info(uint32_t& block_id, VUINT64 &rds, const int32_t flag, VUINT64& fail_ds_list)
{
  return get_block_info_ex(block_id, rds, flag, fail_ds_list);
}

int TfsSession::get_unlink_block_info(uint32_t& block_id, VUINT64 &rds)
{
  VUINT64 fail_ds_list;
  return get_block_info_ex(block_id, rds, BLOCK_WRITE | BLOCK_NOLEASE, fail_ds_list);
}

int TfsSession::get_block_info_ex(uint32_t& block_id, VUINT64 &rds, const int32_t mode, VUINT64& fail_ds_list)
{
  Client *client = CLIENT_POOL.get_client(ns_ip_port_);
  if (client == NULL)
  {
    TBSYS_LOG(ERROR, "get client by (%s) failed", ns_ip_port_str_.c_str());
    return TFS_ERROR;
  }
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    TBSYS_LOG(ERROR, "connect naemserver(%s) failed", ns_ip_port_str_.c_str());
    return TFS_ERROR;
  }

  GetBlockInfoMessage dsmessage(mode);
  dsmessage.set_block_id(block_id);
  const uint32_t fail_ds_list_size = fail_ds_list.size();
  for (uint32_t i = 0; i < fail_ds_list_size; ++i)
  {
    dsmessage.add_fail_server(fail_ds_list.at(i));
  }

  Message *message = client->call(&dsmessage);
  if (message == NULL)
  {
    TBSYS_LOG(ERROR, "get block(%u) info failed", block_id);
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }
  if (message->get_message_type() != SET_BLOCK_INFO_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      TBSYS_LOG(ERROR, "get block(%u) info error(%s)", block_id, dynamic_cast<StatusMessage*> (message)->get_error());
    }
    tbsys::gDelete(message);
    CLIENT_POOL.release_client(client);
    return TFS_ERROR;
  }
  SetBlockInfoMessage* block_info_msg = dynamic_cast<SetBlockInfoMessage*> (message);
  rds = block_info_msg->get_block_ds();
  block_id = block_info_msg->get_block_id();
  if (block_info_msg->get_has_lease())
  {
    rds.push_back(ULONG_LONG_MAX);
    rds.push_back(block_info_msg->get_block_version());
    rds.push_back(block_info_msg->get_lease_id());
  }
  tbsys::gDelete(message);
  CLIENT_POOL.release_client(client);
  return TFS_SUCCESS;
}
