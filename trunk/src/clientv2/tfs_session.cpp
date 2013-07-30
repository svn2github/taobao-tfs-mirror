/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-07-12
 *
 */
#include <tbsys.h>
#include <Memory.hpp>
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/error_msg.h"
#include "common/status_message.h"
#include "message/block_info_message_v2.h"
#include "message/client_cmd_message.h"

#include "tfs_file.h"
#include "tfs_session.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace std;

namespace tfs
{
  namespace clientv2
  {

#ifdef WITH_TAIR_CACHE
    TairCacheHelper* TfsSession::remote_cache_helper_ = NULL;
#endif

    TfsSession::TfsSession(const string& ns_addr,
        const int64_t cache_time, const int64_t cache_items):
      ns_addr_(ns_addr),
      block_cache_time_(cache_time),
      block_cache_items_(cache_items)
    {
      if (cache_items <= 0)
      {
        ClientConfig::use_cache_ &= ~USE_CACHE_FLAG_LOCAL;
      }
      else
      {
        ClientConfig::use_cache_ |= USE_CACHE_FLAG_LOCAL;
      }

      if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
      {
        block_cache_map_.resize(block_cache_items_);
      }
    }

    TfsSession::~TfsSession()
    {
    }

    int TfsSession::initialize()
    {
      int ret = TFS_SUCCESS;
      if (!ns_addr_.empty())
      {
        ns_id_ = Func::get_host_ip(ns_addr_.c_str());
        ret = (0 != ns_id_) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = get_cluster_id_from_ns();
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "nameserver address %s invalid", ns_addr_.c_str());
        ret = EXIT_PARAMETER_ERROR;
      }

      return ret;
    }

    void TfsSession::destroy()
    {
#ifdef WITH_TAIR_CACHE
      tbsys::gDelete(remote_cache_helper_);
#endif
    }

#ifdef WITH_TAIR_CACHE
    int TfsSession::init_remote_cache_helper()
    {
      int ret = TFS_SUCCESS;
      tbutil::Mutex::Lock lock(mutex_);

      if (NULL == remote_cache_helper_)
      {
        remote_cache_helper_ = new TairCacheHelper();
        ret = remote_cache_helper_->initialize(ClientConfig::remote_cache_master_addr_.c_str(),
            ClientConfig::remote_cache_slave_addr_.c_str(), ClientConfig::remote_cache_group_name_.c_str(),
            ClientConfig::remote_cache_area_);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(remote_cache_helper_);
        }

        TBSYS_LOG(DEBUG, "init remote cache helper(master: %s, slave: %s, group_name: %s, area: %d) %s",
            ClientConfig::remote_cache_master_addr_.c_str(), ClientConfig::remote_cache_slave_addr_.c_str(),
            ClientConfig::remote_cache_group_name_.c_str(), ClientConfig::remote_cache_area_,
            TFS_SUCCESS == ret ? "success" : "fail");
      }
      else
      {
        TBSYS_LOG(DEBUG, "remote cache helper already init");
      }

      return ret;
    }

    bool TfsSession::check_remote_cache_init()
    {
      return (NULL != remote_cache_helper_);
    }

    void TfsSession::insert_remote_block_cache(const uint64_t block_id,
        const common::VUINT64& ds, const common::FamilyInfoExt& info)
    {
      if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
      {
        int ret = TFS_SUCCESS;
        if (!check_remote_cache_init())
        {
          ret = init_remote_cache_helper();
        }
        if (TFS_SUCCESS == ret)
        {
          BlockCacheKey block_cache_key;
          BlockCacheValue block_cache_value;
          block_cache_key.set_key(ns_id_, block_id);
          block_cache_value.set_value(ds, info);
          ret = remote_cache_helper_->put(block_cache_key, block_cache_value);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "remote cache insert, blockid: %"PRI64_PREFIX"u", block_id);
          }
          else
          {
            TBSYS_LOG(WARN, "remote cache insert fail, blockid: %"PRI64_PREFIX"u, ret: %d", block_id, ret);
          }
        }
      }
    }

    bool TfsSession::query_remote_block_cache(const uint64_t block_id,
        common::VUINT64& ds, common::FamilyInfoExt& info)
    {
      int ret = TFS_SUCCESS;
      if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
      {
        if (!check_remote_cache_init())
        {
          ret = init_remote_cache_helper();
        }
        if (TFS_SUCCESS == ret)
        {
          BlockCacheKey block_cache_key;
          BlockCacheValue block_cache_value;
          block_cache_key.set_key(ns_id_, block_id);
          ret = remote_cache_helper_->get(block_cache_key, block_cache_value);
          if (TFS_SUCCESS == ret)
          {
            ds = block_cache_value.ds_;
            info = block_cache_value.info_;
            TBSYS_LOG(DEBUG, "query remote cache, blockid: %"PRI64_PREFIX"u", block_id);
          }
        }
      }
      else
      {
        ret = TFS_ERROR;
      }
      return (TFS_SUCCESS == ret) ? true : false;
    }

    void TfsSession::remove_remote_block_cache(const uint64_t block_id)
    {
      if (USE_CACHE_FLAG_REMOTE & ClientConfig::use_cache_)
      {
        int ret = TFS_SUCCESS;
        if (!check_remote_cache_init())
        {
          ret = init_remote_cache_helper();
        }
        if (TFS_SUCCESS == ret)
        {
          BlockCacheKey block_cache_key;
          block_cache_key.set_key(ns_id_, block_id);
          int ret = remote_cache_helper_->remove(block_cache_key);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "remote cache remove, blockid: %"PRI64_PREFIX"u", block_id);
          }
        }
      }
    }
#endif

    int TfsSession::get_block_info(uint64_t& block_id, File& file)
    {
      int ret = TFS_SUCCESS;
      int32_t& mode = file.mode_;
      int32_t& version = file.version_;
      VUINT64& ds = file.ds_;
      FamilyInfoExt& info = file.family_info_;
      const CacheHitStatus last_cache_hit = file.cache_hit_;
      CacheHitStatus& cache_hit = file.cache_hit_;
      cache_hit = CACHE_HIT_NONE;  // reset cache hit status

      if (mode & T_UNLINK)
      {
        ret = get_block_info_ex(mode, block_id, version, ds, info);
      }
      else if (mode & (T_CREATE | T_WRITE))
      {
        if ((mode & T_NEWBLK) == 0)
        {
          mode |= T_CREATE;
        }
        ret = get_block_info_ex(mode, block_id, version, ds, info);
      }
      else // only read request use block cache
      {
        if (INVALID_BLOCK_ID == block_id)
        {
          TBSYS_LOG(ERROR, "blockid %"PRI64_PREFIX"u invalid, mode: %d", block_id, mode);
          ret = EXIT_PARAMETER_ERROR;
        }
        else
        {
          if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL) &&
              (last_cache_hit < CACHE_HIT_LOCAL) &&
              (cache_hit == CACHE_HIT_NONE))
          {
            // search in the local cache
            if (query_local_block_cache(block_id, ds, info))
            {
              cache_hit = CACHE_HIT_LOCAL;
              TBSYS_LOG(DEBUG, "local cache hit, blockid: %"PRI64_PREFIX"u", block_id);
            }
            else
            {
              TBSYS_LOG(DEBUG, "local cache miss, blockid: %"PRI64_PREFIX"u", block_id);
            }
          }

#ifdef WITH_TAIR_CACHE
          if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE) &&
              (last_cache_hit < CACHE_HIT_REMOTE) &&
              (cache_hit == CACHE_HIT_NONE))
          {
            if (CACHE_HIT_NONE == cache_hit)
            {
              // query remote tair cache
              if (query_remote_block_cache(block_id, ds, info))
              {
                cache_hit = CACHE_HIT_REMOTE;
                TBSYS_LOG(DEBUG, "remote cache hit, blockid: %"PRI64_PREFIX"u", block_id);
                if (ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL)
                {
                  insert_local_block_cache(block_id, ds, info);
                }
              }
              else
              {
                TBSYS_LOG(DEBUG, "remote cache miss, blockid: %"PRI64_PREFIX"u", block_id);
              }
            }
          }
#endif

          // get block info from ns
          if (CACHE_HIT_NONE == cache_hit)
          {
            ret = get_block_info_ex(T_READ, block_id, version, ds, info);
            TBSYS_LOG(DEBUG, "query block from ns %s, blockid: %"PRI64_PREFIX"u",
                ns_addr_.c_str(), block_id);
            if (TFS_SUCCESS == ret)
            {
              // update cache
              if (ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL)
              {
                insert_local_block_cache(block_id, ds, info);
              }

#ifdef WITH_TAIR_CACHE
              if (ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE)
              {
                insert_remote_block_cache(block_id, ds, info);
              }
#endif
            }
          }
        }
      }

      return ret;
    }

    bool TfsSession::query_local_block_cache(const uint64_t block_id,
        common::VUINT64& ds, common::FamilyInfoExt& info)
    {
      bool hit = false;
      if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        BlockCache* block_cache = block_cache_map_.find(block_id);
        if ((NULL != block_cache) &&
          (block_cache->last_time_ + block_cache_time_ >= Func::get_monotonic_time()))
        {
          hit = true;
          ds = block_cache->ds_;
          info = block_cache->info_;
          TBSYS_LOG(DEBUG, "query local cache, blockid: %"PRI64_PREFIX"u", block_id);
        }
      }
      return hit;
    }

     void TfsSession::insert_local_block_cache(const uint64_t block_id,
            const common::VUINT64& ds, const common::FamilyInfoExt& info)
     {
       if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
       {
         BlockCache block_cache;
         block_cache.last_time_ =  Func::get_monotonic_time();
         block_cache.ds_ = ds;
         block_cache.info_ = info;
         tbutil::Mutex::Lock lock(mutex_);
         block_cache_map_.insert(block_id, block_cache);
         TBSYS_LOG(DEBUG, "local cache insert, blockid: %"PRI64_PREFIX"u", block_id);
       }
     }

     void TfsSession::remove_local_block_cache(const uint64_t block_id)
     {
       if (USE_CACHE_FLAG_LOCAL & ClientConfig::use_cache_)
       {
         tbutil::Mutex::Lock lock(mutex_);
         block_cache_map_.remove(block_id);
         TBSYS_LOG(DEBUG, "local cache remove, blockid: %"PRI64_PREFIX"u", block_id);
       }
     }

     void TfsSession::expire_block_cache(const uint64_t block_id,
         const CacheHitStatus cache_hit, const int ret)
     {
       // block has been transfered to other dataservers
       if (EXIT_NO_LOGICBLOCK_ERROR == ret)
       {
         if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL) &&
             (CACHE_HIT_LOCAL == cache_hit))
         {
           remove_local_block_cache(block_id);
         }

#ifdef WITH_TAIR_CACHE
         if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE) &&
             (CACHE_HIT_REMOTE == cache_hit))
         {
           remove_local_block_cache(block_id);
           remove_remote_block_cache(block_id);
         }
#endif
       }
     }

     int TfsSession::get_block_info_ex(const int32_t flag, uint64_t& block_id,
        int32_t& version, VUINT64& ds, FamilyInfoExt& info)
     {
       int ret = TFS_SUCCESS;
       GetBlockInfoMessageV2 gbi_message;
       gbi_message.set_block_id(block_id);
       gbi_message.set_mode(flag);

       tbnet::Packet* rsp = NULL;
       NewClient* client = NewClientManager::get_instance().create_client();
       ret = send_msg_to_server(ns_id_, client, &gbi_message, rsp, ClientConfig::wait_timeout_);
       if (TFS_SUCCESS == ret)
       {
         if (GET_BLOCK_INFO_RESP_MESSAGE_V2 == rsp->getPCode())
         {
           GetBlockInfoRespMessageV2* resp_msg = dynamic_cast<GetBlockInfoRespMessageV2*>(rsp);
           const BlockMeta& block_meta = resp_msg->get_block_meta();
           block_id = block_meta.block_id_;
           version = block_meta.version_;
           for (int i = 0; i < block_meta.size_; i++)
           {
             ds.push_back(block_meta.ds_[i]);
           }
           info = block_meta.family_info_;
         }
         else if (STATUS_MESSAGE == rsp->getPCode())
         {
           StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(rsp);
           ret = resp_msg->get_status();
           TBSYS_LOG(WARN, "get block %"PRI64_PREFIX"u info fail, ret: %d, error: %s",
               block_id, ret, resp_msg->get_error());
         }
         else
         {
           ret = EXIT_UNKNOWN_MSGTYPE;
         }
       }
       NewClientManager::get_instance().destroy_client(client);

       return ret;
     }

    int TfsSession::get_cluster_id_from_ns()
    {
      ClientCmdMessage cc_message;
      cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
      cc_message.set_value3(20);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = send_msg_to_server(ns_id_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get cluster id from ns fail, ret: %d", ret);
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
        //ugly use error msg
        if (status_msg->get_status() == STATUS_MESSAGE_OK &&
            strlen(status_msg->get_error()) > 0)
        {
          char cluster_id = static_cast<char> (atoi(status_msg->get_error()));
          if (isdigit(cluster_id) || isalpha(cluster_id))
          {
            cluster_id_ = cluster_id - '0';
            TBSYS_LOG(INFO, "get cluster id from nameserver success. cluster id: %d", cluster_id_);
          }
          else
          {
            TBSYS_LOG(ERROR, "get cluster id from nameserver fail. cluster id: %c", cluster_id);
            ret = TFS_ERROR;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster id from ns failed, msg type error. type: %d", rsp->getPCode());
        ret = EXIT_UNKNOWN_MSGTYPE;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    int TfsSession::get_cluster_group_count_from_ns()
    {
      ClientCmdMessage cc_message;
      cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
      cc_message.set_value3(22);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = send_msg_to_server(ns_id_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get cluster group count from ns fail, ret: %d", ret);
        ret = -1;
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
        if (status_msg->get_status() == STATUS_MESSAGE_OK &&
            strlen(status_msg->get_error()) > 0)
        {
          ret = atoi(status_msg->get_error());
          if (ret > 0)
          {
            TBSYS_LOG(INFO, "get cluster group count from nameserver success. cluster group count: %d", ret);
          }
          else
          {
            TBSYS_LOG(WARN, "get cluster group count from nameserver fail.");
            return DEFAULT_CLUSTER_GROUP_COUNT;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster group count from nameserver failed, msg type error. type: %d", rsp->getPCode());
        ret = -1;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }

    int TfsSession::get_cluster_group_seq_from_ns()
    {
      ClientCmdMessage cc_message;
      cc_message.set_cmd(CLIENT_CMD_SET_PARAM);
      cc_message.set_value3(23);

      tbnet::Packet* rsp = NULL;
      NewClient* client = NewClientManager::get_instance().create_client();
      int ret = send_msg_to_server(ns_id_, client, &cc_message, rsp, ClientConfig::wait_timeout_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get cluster group seq from ns fail, ret: %d", ret);
        ret = -1;
      }
      else if (STATUS_MESSAGE == rsp->getPCode())
      {
        StatusMessage* status_msg = dynamic_cast<StatusMessage*>(rsp);
        if (status_msg->get_status() == STATUS_MESSAGE_OK &&
            strlen(status_msg->get_error()) > 0)
        {
          ret = atoi(status_msg->get_error());
          if (ret >= 0)
          {
            TBSYS_LOG(INFO, "get cluster group seq from nameserver success. cluster group seq: %d", ret);
          }
          else
          {
            TBSYS_LOG(WARN, "get cluster group seq from nameserver fail.");
            return DEFAULT_CLUSTER_GROUP_SEQ;
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "get cluster group seq from nameserver failed, msg type error. type: %d", rsp->getPCode());
        ret = -1;
      }
      NewClientManager::get_instance().destroy_client(client);
      return ret;
    }
  }
}
