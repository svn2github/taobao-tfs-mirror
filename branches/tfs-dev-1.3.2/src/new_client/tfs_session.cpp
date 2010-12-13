/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_session.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
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

TfsSession::TfsSession(const std::string& nsip, const int32_t cache_time, const int32_t cache_items)
  :ns_addr_(0), ns_addr_str_(nsip), block_cache_time_(cache_time), block_cache_items_(cache_items),
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
  int ret = send_message_to_server(ns_addr_, &ccmessage, &ret_msg);
  if (!ret_msg)
  {
    TBSYS_LOG(ERROR, "get cluster id from ns failed, null message recieved, ret: %d");
  }
  else if (ret_msg->get_message_type() == STATUS_MESSAGE)
  {
    StatusMessage* status_msg = dynamic_cast<StatusMessage*> (ret_msg);
    if ((ret = status_msg->get_status()) == STATUS_MESSAGE_OK &&
        strlen(status_msg->get_error()) > 0)
    {
      int cluster_id = atoi(status_msg->get_error());
      if (cluster_id <= 0)
      {
        TBSYS_LOG(ERROR, "get cluster id from nameserver fail cluster id: %d", cluster_id);
        ret = TFS_ERROR;
      }
      else
      {
        cluster_id_ = cluster_id;
      }
    }
  }

  tbsys::gDelete(ret_msg);
  return ret;
}

int TfsSession::initialize()
{
  if ((ns_addr_str_.empty()) || (ns_addr_str_.compare(" ") == 0))
  {
    TBSYS_LOG(ERROR, "nameserver address %s invalid", ns_addr_str_.c_str());
    return TFS_ERROR;
  }

  ns_addr_ = Func::get_host_ip(ns_addr_str_.c_str());
  if (ns_addr_ <= 0)
  {
    TBSYS_LOG(ERROR, "nameserver address invalid", ns_addr_str_.c_str());
    return TFS_ERROR;
  }

  return get_cluster_id_from_ns();
}

int TfsSession::get_block_info(uint32_t& block_id, VUINT64 &rds, const int32_t flag)
{
  // insert to cache ?
  if (flag & T_WRITE)
  {
    return get_block_info_ex(block_id, rds, flag);
  }

  // read || unlink
  if (block_id == 0)
  {
    TBSYS_LOG(ERROR, "block %u zero error for mode(%d)", block_id, flag);
    return TFS_ERROR;
  }

  // search in the local cache
  if (use_cache_ == USE_CACHE_FLAG_YES)
  {
    tbutil::Mutex::Lock lock(mutex_);
    BlockCache* block_cache = block_cache_map_.find(block_id);
    if (block_cache &&
        (block_cache->last_time_ >= time(NULL) - block_cache_time_))
    {
      rds = block_cache->ds_;
      return TFS_SUCCESS;
    }
  }

  BlockCache block_cache;
  int ret = TFS_SUCCESS;
  if ((ret = get_block_info_ex(block_id, block_cache.ds_, T_READ)) == TFS_SUCCESS)
  {
    if (block_cache.ds_.size() <= 0)
    {
      TBSYS_LOG(ERROR, "get block %u info failed, dataserver size %u <= 0", block_id, rds.size());
      ret = TFS_ERROR;
    }
    else
    {
      block_cache.last_time_ = time(NULL);
      rds = block_cache.ds_;
      tbutil::Mutex::Lock lock(mutex_);
      block_cache_map_.insert(block_id, block_cache);
    }
  }
  return ret;
}

int TfsSession::get_block_info_ex(uint32_t& block_id, VUINT64 &rds, const int32_t flag)
{
  Client *client = CLIENT_POOL.get_client(ns_addr_);
  if (client == NULL)
  {
    TBSYS_LOG(ERROR, "get client by (%s) failed", ns_addr_str_.c_str());
    return TFS_ERROR;
  }
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    TBSYS_LOG(ERROR, "connect naemserver(%s) failed", ns_addr_str_.c_str());
    return TFS_ERROR;
  }

  GetBlockInfoMessage dsmessage(flag);
  dsmessage.set_block_id(block_id);

  Message *message = client->call(&dsmessage);
  int ret = TFS_ERROR;
  if (!message)
  {
    TBSYS_LOG(ERROR, "get block %u info failed: null message recieved", block_id);
  }
  else if (message->get_message_type() != SET_BLOCK_INFO_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      // TFS_ERROR ?
      ret = dynamic_cast<StatusMessage*>(message)->get_status();
      TBSYS_LOG(ERROR, "get block %d info fail, ret: %d, error: %s",
                block_id, ret, dynamic_cast<StatusMessage*>(message)->get_error());
    }
  }
  else
  {
    SetBlockInfoMessage* block_info_msg = dynamic_cast<SetBlockInfoMessage*>(message);
    rds = block_info_msg->get_block_ds();
    block_id = block_info_msg->get_block_id();
    if (rds.size() && block_id > 0)
    {
      if (block_info_msg->get_has_lease())
      {
        rds.push_back(ULONG_LONG_MAX);
        rds.push_back(block_info_msg->get_block_version());
        rds.push_back(block_info_msg->get_lease_id());
      }
      ret = TFS_SUCCESS;
    }
  }
  tbsys::gDelete(message);
  CLIENT_POOL.release_client(client);
  return ret;
}

int TfsSession::batch_get_block_info(vector<SegmentData*>& seg_list, const int32_t flag)
{
  // insert to cache ?
  if (flag & T_WRITE)
  {
    return batch_get_block_info_ex(seg_list, flag);
  }

  // search in the local cache
  if (use_cache_ == USE_CACHE_FLAG_YES)
  {
    for (int32_t i = 0; i < seg_list.size(); i++)
    {
      int32_t block_id = seg_list[i]->seg_info_.block_id_;

      if (!block_id)
      {
        TBSYS_LOG(ERROR, "block %u zero error for mode(%d)", block_id, flag);
        return TFS_ERROR;
      }

      tbutil::Mutex::Lock lock(mutex_);
      BlockCache* block_cache = block_cache_map_.find(block_id);
      if (block_cache &&
          (block_cache->last_time_ >= time(NULL) - block_cache_time_))
      {
        seg_list[i]->ds_ = block_cache->ds_;
      }
    }
  }

  BlockCache block_cache;
  int ret = TFS_SUCCESS;
  if ((ret = batch_get_block_info_ex(seg_list, T_READ)) == TFS_SUCCESS)
  {
    for (int32_t i = 0; i < seg_list.size(); i++)
    {
      if (seg_list[i]->ds_.size() <= 0)
      {
        TBSYS_LOG(ERROR, "get block %u info failed, dataserver size %u <= 0",
                  seg_list[i]->seg_info_.block_id_, seg_list[i]->ds_.size());
        ret = TFS_ERROR;
        break;
      }
      else
      {
        block_cache.last_time_ = time(NULL);
        block_cache.ds_ = seg_list[i]->ds_; // TODO, check already have
        tbutil::Mutex::Lock lock(mutex_);
        block_cache_map_.insert(seg_list[i]->seg_info_.block_id_, block_cache);
      }
    }
  }
  return ret;
}

int TfsSession::batch_get_block_info_ex(vector<SegmentData*>& seg_list, const int32_t flag)
{
  Client *client = CLIENT_POOL.get_client(ns_addr_);
  if (client == NULL)
  {
    TBSYS_LOG(ERROR, "get client by (%s) failed", ns_addr_str_.c_str());
    return TFS_ERROR;
  }
  if (client->connect() != TFS_SUCCESS)
  {
    CLIENT_POOL.release_client(client);
    TBSYS_LOG(ERROR, "connect naemserver(%s) failed", ns_addr_str_.c_str());
    return TFS_ERROR;
  }

  int32_t block_count = 0;

  BatchGetBlockInfoMessage dsmessage(flag);
  if (flag & T_WRITE)
  {
    block_count = seg_list.size();
    dsmessage.set_block_count(block_count);
  }
  else
  {
    for (int32_t i = 0; i < seg_list.size(); i++)
    {
      if (seg_list[i]->ds_.size() == 0)
      {
        dsmessage.add_block_id(seg_list[i]->seg_info_.block_id_);
        block_count++;
      }
    }
  }

  Message *message = client->call(&dsmessage);
  int ret = TFS_ERROR;
  if (!message)
  {
    TBSYS_LOG(ERROR, "batch get block info failed: null message recieved");
  }
  else if (message->get_message_type() != BATCH_SET_BLOCK_INFO_MESSAGE)
  {
    if (message->get_message_type() == STATUS_MESSAGE)
    {
      // TFS_ERROR ?
      ret = dynamic_cast<StatusMessage*>(message)->get_status();
      TBSYS_LOG(ERROR, "batch get block info fail, ret: %d, error: %s",
                ret, dynamic_cast<StatusMessage*>(message)->get_error());
    }
  }
  else
  {
    BatchSetBlockInfoMessage* block_info_msg = dynamic_cast<BatchSetBlockInfoMessage*>(message);
    const map<uint32_t, BlockInfoSeg>* block_info = block_info_msg->get_infos();
    if (block_info->size() != block_count)
    {
      TBSYS_LOG(ERROR, "batch get block info fail, get count conflict, request:%d, response:%d",
                block_count, block_info->size());
      return TFS_ERROR;
    }

  //   std::map<uint32_t, BlockInfoSeg>::iterator it;

  //   if (flag & T_READ)
  //   {
  //     for (int32_t i = 0; i < seg_list.size(); i++)
  //     {
  //       if ((it = block_info->find(seg_list[i]->seg_info_.block_id_)) == block_info->end())
  //       {
  //         // retry ?
  //         TBSYS_LOG(ERROR, "get block %d info fail", seg_list[i]->seg_info_.block_id_);
  //         return TFS_ERROR;
  //       }
  //       seg_list[i]->ds_ = it->second.ds_;
  //     }
  //   }
  //   else if (flag & T_WRITE)
  //   {
  //     it = block_info->begin();
  //     for (int32_t i = 0; i < seg_list.size(); i++, it++)
  //     {
  //       seg_list[i]->seg_info_.block_id_ = it->first;
  //       seg_list[i]->ds_ = it->second.ds_;
  //       if (it->second.has_lease_) // should have
  //       {
  //         seg_list[i]->ds_.push_back(ULONG_LONG_MAX);
  //         seg_list[i]->ds_.push_back(it->second.version_);
  //         seg_list[i]->ds_.push_back(it->second.lease_);
  //       }
  //     }
  //   }
  //   else
  //   {
  //     TBSYS_LOG(ERROR, "unknown flag %d", flag);
  //     return TFS_ERROR;
  //   }
  }

  tbsys::gDelete(message);
  CLIENT_POOL.release_client(client);
  return ret;
}
