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
#include "Timer.h"
#include "common/client_manager.h"
#include "common/new_client.h"
#include "common/error_msg.h"
#include "common/status_message.h"
#include "message/block_info_message_v2.h"
#include "message/client_cmd_message.h"
#include "message/server_status_message.h"

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

    ServerStat::ServerStat():
      id_(0), use_capacity_(0), total_capacity_(0), current_load_(0), block_count_(0),
      last_update_time_(0), startup_time_(0), current_time_(0)
    {
      memset(&total_tp_, 0, sizeof(total_tp_));
      memset(&last_tp_, 0, sizeof(last_tp_));
    }

    ServerStat::~ServerStat()
    {
    }

    int ServerStat::deserialize(tbnet::DataBuffer& input, const int32_t length, int32_t& offset)
    {
      if (input.getDataLen() <= 0 || offset >= length)
      {
        return TFS_ERROR;
      }
      int32_t len = input.getDataLen();
      id_ = input.readInt64();
      use_capacity_ = input.readInt64();
      total_capacity_ = input.readInt64();
      current_load_ = input.readInt32();
      block_count_  = input.readInt32();
      last_update_time_ = input.readInt64();
      startup_time_ = input.readInt64();
      total_tp_.write_byte_ = input.readInt64();
      total_tp_.read_byte_ = input.readInt64();
      total_tp_.write_file_count_ = input.readInt64();
      total_tp_.read_file_count_ = input.readInt64();
      total_tp_.unlink_file_count_ = input.readInt64();
      total_tp_.fail_write_byte_ = input.readInt64();
      total_tp_.fail_read_byte_ = input.readInt64();
      total_tp_.fail_write_file_count_ = input.readInt64();
      total_tp_.fail_read_file_count_ = input.readInt64();
      total_tp_.fail_unlink_file_count_ = input.readInt64();
      current_time_ = input.readInt64();
      status_ = (DataServerLiveStatus)input.readInt32();
      offset += (len - input.getDataLen());

      return TFS_SUCCESS;
    }

    TfsSession::TfsSession(tbutil::TimerPtr timer, const string& ns_addr,
        const int64_t cache_time, const int64_t cache_items):
      timer_(timer),
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

      if (TFS_SUCCESS == ret)
      {
        ret = update_dst();
      }

      if (TFS_SUCCESS == ret)
      {
        ret = stat_mgr_.initialize(timer_);
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t current = tbsys::CTimeUtil::getTime();

        StatEntry<string, string>::StatEntryPtr access_stat_ptr =
          new StatEntry<string, string>(StatItem::client_access_stat_, current, false);
        access_stat_ptr->add_sub_key(StatItem::read_success_);
        access_stat_ptr->add_sub_key(StatItem::read_fail_);
        access_stat_ptr->add_sub_key(StatItem::write_success_);
        access_stat_ptr->add_sub_key(StatItem::write_fail_);
        access_stat_ptr->add_sub_key(StatItem::stat_success_);
        access_stat_ptr->add_sub_key(StatItem::stat_fail_);
        access_stat_ptr->add_sub_key(StatItem::unlink_success_);
        access_stat_ptr->add_sub_key(StatItem::unlink_fail_);

        StatEntry<string, string>::StatEntryPtr cache_stat_ptr =
          new StatEntry<string, string>(StatItem::client_cache_stat_, current, false);
        cache_stat_ptr->add_sub_key(StatItem::local_cache_hit_);
        cache_stat_ptr->add_sub_key(StatItem::local_cache_miss_);

#ifdef WITH_TAIR_CACHE
        cache_stat_ptr->add_sub_key(StatItem::remote_cache_hit_);
        cache_stat_ptr->add_sub_key(StatItem::remote_cache_miss_);
#endif

        stat_mgr_.add_entry(access_stat_ptr, ClientConfig::stat_interval_ * 1000);
        stat_mgr_.add_entry(cache_stat_ptr, ClientConfig::stat_interval_ * 1000);
      }

      return ret;
    }

    bool TfsSession::need_update_dst()
    {
      uint32_t success_ops =
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::read_success_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::write_success_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::stat_success_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::unlink_success_);

      uint64_t fail_ops =
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::read_fail_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::write_fail_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::stat_fail_) +
        stat_mgr_.get_stat_value(StatItem::client_access_stat_, StatItem::unlink_fail_);

      uint64_t total_ops = success_ops + fail_ops;
      return  (total_ops % ClientConfig::update_dst_interval_count_ == 0) ||
        (fail_ops % ClientConfig::update_dst_fail_count_ == 0);
    }

    void TfsSession::update_stat(const StatType type, bool success)
    {
      switch (type)
      {
        case ST_READ:
        {
          string& target = success ? StatItem::read_success_ : StatItem::read_fail_;
          stat_mgr_.update_entry(StatItem::client_access_stat_, target, 1);
          break;
        }
        case ST_WRITE:
        {
          string& target = success ? StatItem::write_success_ : StatItem::write_fail_;
          stat_mgr_.update_entry(StatItem::client_access_stat_, target, 1);
          break;
        }
        case ST_STAT:
        {
          string& target = success ? StatItem::stat_success_ : StatItem::stat_fail_;
          stat_mgr_.update_entry(StatItem::client_access_stat_, target, 1);
          break;
        }
        case ST_UNLINK:
        {
          string& target = success ? StatItem::unlink_success_ : StatItem::unlink_fail_;
          stat_mgr_.update_entry(StatItem::client_access_stat_, target, 1);
          break;
        }
        case ST_LOCAL_CACHE:
        {
          string& target = success ? StatItem::local_cache_hit_ : StatItem::local_cache_miss_;
          stat_mgr_.update_entry(StatItem::client_cache_stat_, target, 1);
          break;
        }
#ifdef WITH_TAIR_CACHE
        case ST_REMOTE_CACHE:
        {
          string& target = success ? StatItem::remote_cache_hit_ : StatItem::remote_cache_miss_;
          stat_mgr_.update_entry(StatItem::client_cache_stat_, target, 1);
          break;
        }
#endif
        default:
          break; // do nothing
      }

      if (need_update_dst())
      {
        update_dst();
      }
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
      int32_t& version = file.version_;
      VUINT64& ds = file.ds_;
      FamilyInfoExt& info = file.family_info_;
      const CacheHitStatus last_cache_hit = file.cache_hit_;
      CacheHitStatus& cache_hit = file.cache_hit_;
      cache_hit = CACHE_HIT_NONE;  // reset cache hit status

      if (INVALID_BLOCK_ID == block_id)
      {
        uint64_t server = select_server_from_dst();
        ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_NO_DATASERVER;
        if (TFS_SUCCESS == ret)
        {
          ds.push_back(server);
        }
      }
      else
      {
        if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_LOCAL) &&
            (last_cache_hit < CACHE_HIT_LOCAL) &&
            (cache_hit == CACHE_HIT_NONE))
        {
          // search in the local cache
          bool local_cache_hit = query_local_block_cache(block_id, ds, info);
          if (local_cache_hit)
          {
            cache_hit = CACHE_HIT_LOCAL;
            TBSYS_LOG(DEBUG, "local cache hit, blockid: %"PRI64_PREFIX"u", block_id);
          }
          else
          {
            TBSYS_LOG(DEBUG, "local cache miss, blockid: %"PRI64_PREFIX"u", block_id);
          }
          update_stat(ST_LOCAL_CACHE, cache_hit);
        }

#ifdef WITH_TAIR_CACHE
        if ((ClientConfig::use_cache_ & USE_CACHE_FLAG_REMOTE) &&
            (last_cache_hit < CACHE_HIT_REMOTE) &&
            (cache_hit == CACHE_HIT_NONE))
        {
          if (CACHE_HIT_NONE == cache_hit)
          {
            // query remote tair cache
            bool remote_cache_hit = query_remote_block_cache(block_id, ds, info);
            if (remote_cache_hit)
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
            update_stat(ST_REMOTE_CACHE, cache_hit);
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

     uint64_t TfsSession::select_server_from_dst() const
     {
       uint64_t server = INVALID_SERVER_ID;
       if (ds_table_.size() > 0)
       {
         uint32_t index = random() % ds_table_.size();
         server = ds_table_[index];
       }
       return server;
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

    int TfsSession::update_dst()
    {
      ShowServerInformationMessage msg;
      SSMScanParameter& param = msg.get_param();
      param.type_ = SSM_TYPE_SERVER;
      param.child_type_ = SSM_CHILD_SERVER_TYPE_INFO;
      param.start_next_position_ = 0x0;
      param.should_actual_count_= (100 << 16);  // get 100 ds every turn
      param.end_flag_ = SSM_SCAN_CUTOVER_FLAG_YES;

      if (false == NewClientManager::get_instance().is_init())
      {
        TBSYS_LOG(ERROR, "new client manager not init.");
        return TFS_ERROR;
      }

      bool need_clear_table = true;
      while (!((param.end_flag_ >> 4) & SSM_SCAN_END_FLAG_YES))
      {
        param.data_.clear();
        tbnet::Packet* ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        int ret = send_msg_to_server(ns_id_, client, &msg, ret_msg);
        if (TFS_SUCCESS != ret || ret_msg == NULL)
        {
          TBSYS_LOG(ERROR, "get server info error, ret: %d", ret);
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }
        if(ret_msg->getPCode() != SHOW_SERVER_INFORMATION_MESSAGE)
        {
          TBSYS_LOG(ERROR, "get invalid message type, pcode: %d", ret_msg->getPCode());
          NewClientManager::get_instance().destroy_client(client);
          return TFS_ERROR;
        }

        // success, clear table first
        if (need_clear_table)
        {
          ds_table_.clear();
          need_clear_table = false;
        }

        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(ret_msg);
        SSMScanParameter& ret_param = message->get_param();

        int32_t data_len = ret_param.data_.getDataLen();
        int32_t offset = 0;
        while (data_len > offset)
        {
          ServerStat server;
          if (TFS_SUCCESS == server.deserialize(ret_param.data_, data_len, offset))
          {
            ds_table_.push_back(server.id_);
            std::string ip_port = Func::addr_to_str(server.id_, true);
          }
        }
        param.addition_param1_ = ret_param.addition_param1_;
        param.addition_param2_ = ret_param.addition_param2_;
        param.end_flag_ = ret_param.end_flag_;
        NewClientManager::get_instance().destroy_client(client);
      }

      TBSYS_LOG(DEBUG, "dump dataserver table, size: %zd", ds_table_.size());
      for (uint32_t i = 0; i < ds_table_.size(); i++)
      {
        TBSYS_LOG(DEBUG, tbsys::CNetUtil::addrToString(ds_table_[i]).c_str());
      }

      return TFS_SUCCESS;
    }

  }
}
