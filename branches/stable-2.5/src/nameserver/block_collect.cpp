/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include "common/parameter.h"
#include "block_collect.h"
#include "server_collect.h"
#include "layout_manager.h"

using namespace tfs::common;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    const int8_t BlockCollect::BLOCK_CREATE_FLAG_NO = 0;
    const int8_t BlockCollect::BLOCK_CREATE_FLAG_YES = 1;
    BlockCollect::BlockCollect(const uint32_t block_id, const time_t now):
      GCObject(now),
      family_id_(INVALID_FAMILY_ID),
      server_size_(0),
      create_flag_(BLOCK_CREATE_FLAG_NO),
      in_replicate_queue_(BLOCK_IN_REPLICATE_QUEUE_NO)
    {
      servers_ = new (std::nothrow)uint64_t[SYSPARAM_NAMESERVER.max_replication_];
      assert(servers_);
      memset(servers_, 0, sizeof(uint64_t) * SYSPARAM_NAMESERVER.max_replication_);
      memset(&info_, 0, sizeof(info_));
      info_.block_id_ = block_id;
      info_.seq_no_ = 1;
    }

    BlockCollect::BlockCollect(const uint32_t block_id):
      GCObject(0),
      servers_(NULL),
      family_id_(INVALID_FAMILY_ID),
      server_size_(0),
      create_flag_(BLOCK_CREATE_FLAG_NO),
      in_replicate_queue_(BLOCK_IN_REPLICATE_QUEUE_NO)
    {
      info_.block_id_ = block_id;
      //for query
    }

    BlockCollect::~BlockCollect()
    {
      tbsys::gDeleteA(servers_);
    }

    int BlockCollect::add(bool& writable, bool& master, const uint64_t server, const time_t now)
    {
      master = false;
      writable  = false;
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        update_last_time(now);
        dump(TBSYS_LOG_LEVEL(DEBUG));
        writable = !is_full();
        ret = exist(server) ? EXIT_SERVER_EXISTED : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          int8_t index = 0;
          bool   complete = false;
          int8_t random_index = random() % SYSPARAM_NAMESERVER.max_replication_;
          TBSYS_LOG(DEBUG, "random_index : %d, servers_size: %d", random_index, get_servers_size());
          for (int8_t i = 0; i < SYSPARAM_NAMESERVER.max_replication_ && !complete; ++i, ++random_index)
          {
            index = random_index % SYSPARAM_NAMESERVER.max_replication_;
            if (INVALID_SERVER_ID == servers_[index])
            {
              complete = true;
              servers_[index] = server;
            }
          }
          ret = complete ? TFS_SUCCESS : EXIT_INSERT_SERVER_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ++server_size_;
            master = is_master(server);
          }
        }
      }
      return ret;
    }

    int BlockCollect::remove(const uint64_t server, const time_t now)
    {
      update_last_time(now);
      int32_t ret = ((NULL != servers_) && (INVALID_SERVER_ID != server) && (server_size_ > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = EXIT_DATASERVER_NOT_FOUND;
        for (int8_t index = 0; index < SYSPARAM_NAMESERVER.max_replication_ && TFS_SUCCESS != ret; ++index)
        {
          ret = servers_[index] == server ? TFS_SUCCESS : EXIT_DATASERVER_NOT_FOUND;
          if (TFS_SUCCESS == ret)
          {
            --server_size_;
            servers_[index] = INVALID_SERVER_ID;
          }
        }
      }
      return ret;
    }

    bool BlockCollect::exist(const uint64_t server) const
    {
      bool exist = (NULL != servers_ && INVALID_SERVER_ID != server && server_size_ > 0);
      if (exist)
      {
        exist = false;
        for (int8_t index = 0; index < SYSPARAM_NAMESERVER.max_replication_ && !exist; ++index)
        {
          exist = servers_[index] == server;
        }
      }
      return exist;
    }

    void BlockCollect::get_servers(ArrayHelper<uint64_t>& servers) const
    {
      if ((servers.get_array_size() >= SYSPARAM_NAMESERVER.max_replication_)
         && (server_size_ > 0) && (NULL != servers_))
      {
        uint64_t server = INVALID_SERVER_ID;
        for (int8_t index = 0; index < SYSPARAM_NAMESERVER.max_replication_; ++index)
        {
          server = servers_[index];
          if (INVALID_SERVER_ID != server)
            servers.push_back(server);
        }
      }
    }

    void BlockCollect::get_servers(std::vector<uint64_t>& servers) const
    {
      if ((server_size_ > 0) && (NULL != servers_))
      {
        uint64_t server = INVALID_SERVER_ID;
        for (int8_t index = 0; index < SYSPARAM_NAMESERVER.max_replication_; ++index)
        {
          server = servers_[index];
          if (INVALID_SERVER_ID != server)
            servers.push_back(server);
        }
      }
    }

    uint64_t BlockCollect::get_server(const int8_t index) const
    {
      uint64_t server = INVALID_SERVER_ID;
      if (NULL != servers_ && server_size_ > 0 && index >= 0 && index < SYSPARAM_NAMESERVER.max_replication_)
      {
        for (int8_t i = index; i < SYSPARAM_NAMESERVER.max_replication_ && INVALID_SERVER_ID == server; ++i)
          server = servers_[i];
      }
      return server;
    }

    int BlockCollect::check_version(LayoutManager& manager, common::ArrayHelper<uint64_t>& removes,
        bool& expire_self, common::ArrayHelper<uint64_t>& other_expires, const uint64_t server,
        const int8_t role, const bool isnew, const common::BlockInfo& info, const time_t now)
    {
      expire_self = false;
      int32_t ret = (INVALID_SERVER_ID != server && info.block_id_ == id() && (NULL != servers_)
                     && removes.get_array_size() > 0 && other_expires.get_array_size() > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = exist(server) ? EXIT_SERVER_EXISTED : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          //check block version
          ret = (info.version_ >= info_.version_) ? TFS_SUCCESS : EXIT_BLOCK_VERSION_ERROR;
          if (TFS_SUCCESS != ret)
          {
            if (server_size_ > 0)
              expire_self = (role == NS_ROLE_MASTER);//i'm master, we're going to expire blocks
            else
              ret = TFS_SUCCESS;
          }
          if (TFS_SUCCESS == ret)
          {
            if (info.version_ == info_.version_)//version argeed
            {
              if (server_size_ >= SYSPARAM_NAMESERVER.max_replication_)
              {
                uint64_t servers[SYSPARAM_NAMESERVER.max_replication_ + 1];
                ArrayHelper<uint64_t> helper(SYSPARAM_NAMESERVER.max_replication_ + 1, servers);
                get_servers(helper);
                helper.push_back(server);
                ServerCollect* result = NULL;
                manager.get_server_manager().choose_excess_backup_server(result, helper);
                ret = server == result->id() ? EXIT_EXPIRE_SELF_ERROR : TFS_SUCCESS;
                if (TFS_SUCCESS != ret)
                {
                  expire_self = (role == NS_ROLE_MASTER);//i'm master, we're going to expire blocks
                }
                else
                {
                  remove(result->id(), now);
                  removes.push_back(result->id());//解除与dataserver的关系
                  if (role == NS_ROLE_MASTER)
                    other_expires.push_back(result->id());
                }
              }
            }

            if (info.version_ > info_.version_)
            {
              info_ = info;
              if (!isnew)//release dataserver
              {
                TBSYS_LOG(INFO, "block: %u in dataserver: %s version error %d:%d,replace ns version, current dataserver size: %u",
                    info.block_id_, tbsys::CNetUtil::addrToString(server).c_str(),
                    info_.version_, info.version_, server_size_);
                if (role == NS_ROLE_MASTER)
                {
                  update_last_time(now);
                  cleanup(removes, other_expires);
                }
              }
            }

            if (info.version_ < info_.version_)
            {
              ret = server_size_ <= 0 ? TFS_SUCCESS : EXIT_EXPIRE_SELF_ERROR;
              if (TFS_SUCCESS != ret)
                expire_self = (role == NS_ROLE_MASTER);//i'm master, we're going to expire blocks
              if (TFS_SUCCESS == ret)
                info_ = info;
            }
          }
        }
      }
      return ret;
    }

    /**
     * to check a block if replicate
     * @return: -1: none, 0: normal, 1: emergency
     */
    PlanPriority BlockCollect::check_replicate(const time_t now) const
    {
      PlanPriority priority = PLAN_PRIORITY_NONE;
      if (BLOCK_CREATE_FLAG_YES != create_flag_ && !is_in_family())
      {
        if (server_size_ <= 0)
        {
          TBSYS_LOG(WARN, "block: %u has been lost, do not replicate", info_.block_id_);
        }
        else
        {
          if (last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_ <= now)
          {
            if (server_size_ < SYSPARAM_NAMESERVER.max_replication_)
              priority = PLAN_PRIORITY_NORMAL;
            if (1 == server_size_ && SYSPARAM_NAMESERVER.max_replication_ > 1)
              priority = PLAN_PRIORITY_EMERGENCY;
          }
        }
      }
      return priority;
    }

    /**
     * to check a block if compact
     * @return: return true if need compact
     */
    bool BlockCollect::check_compact() const
    {
      bool bret = false;
      if (BLOCK_CREATE_FLAG_YES != create_flag_ && !is_in_family())
      {
        if ((server_size_ == SYSPARAM_NAMESERVER.max_replication_) && is_full())
        {
          if ((info_.file_count_ > 0)
              && (info_.size_ > 0)
              && (info_.del_file_count_ > 0)
              && (info_.del_size_ > 0))
          {
            int32_t delete_file_num_ratio = get_delete_file_num_ratio();
            int32_t delete_size_ratio = get_delete_file_size_ratio();
            if ((delete_file_num_ratio >  SYSPARAM_NAMESERVER.compact_delete_ratio_)
                || (delete_size_ratio > SYSPARAM_NAMESERVER.compact_delete_ratio_))
            {
              TBSYS_LOG(DEBUG, "block: %u need compact", info_.block_id_);
              bret = true;
            }
          }
        }
      }
      return bret;
    }

    bool BlockCollect::check_balance() const
    {
      return server_size_ >= SYSPARAM_NAMESERVER.max_replication_;
    }

    bool BlockCollect::check_marshalling() const
    {
      bool ret = (BLOCK_CREATE_FLAG_YES != create_flag_ && !is_in_family());
      if (ret)
      {
        ret = (server_size_ >= SYSPARAM_NAMESERVER.max_replication_  && is_full() && size() <= MAX_MARSHALLING_BLOCK_SIZE_LIMIT);
        if (ret)
        {
          int32_t delete_file_num_ratio = get_delete_file_num_ratio();
          int32_t delete_size_ratio = get_delete_file_size_ratio();
          ret = (delete_file_num_ratio <= SYSPARAM_NAMESERVER.marshalling_delete_ratio_
                && delete_size_ratio <= SYSPARAM_NAMESERVER.marshalling_delete_ratio_);
        }
      }
      return ret;
    }

    bool BlockCollect::check_reinstate(const time_t now) const
    {
      bool ret = (BLOCK_CREATE_FLAG_YES != create_flag_ && is_in_family());
      if (ret)
      {
        ret = ((server_size_ <= 0) && (last_update_time_ + SYSPARAM_NAMESERVER.replicate_wait_time_ <= now));
      }
      return ret;
    }

    int BlockCollect::scan(SSMScanParameter& param) const
    {
      int16_t child_type = param.child_type_;
      bool has_dump = (child_type & SSM_CHILD_BLOCK_TYPE_FULL) ? is_full() : true;
      if (has_dump)
      {
        if (child_type & SSM_CHILD_BLOCK_TYPE_INFO)
        {
          param.data_.writeBytes(&info_, sizeof(info_));
        }
        if (child_type & SSM_CHILD_BLOCK_TYPE_SERVER)
        {
          param.data_.writeInt8(server_size_);
          for (int8_t index = 0; index < SYSPARAM_NAMESERVER.max_replication_; ++index)
          {
            uint64_t server = servers_[index];
            if (INVALID_SERVER_ID != server)
              param.data_.writeInt64(server);
          }
        }
        param.data_.writeInt64(family_id_);
      }
      return has_dump ? TFS_SUCCESS : TFS_ERROR;
    }

    void BlockCollect::dump(int32_t level, const char* file, const int32_t line, const char* function) const
    {
      if (level >= TBSYS_LOGGER._level)
      {
        std::string str;
        uint64_t server;
        for (int8_t index = 0; index < common::SYSPARAM_NAMESERVER.max_replication_; ++index)
        {
          server = servers_[index];
          if (INVALID_SERVER_ID != server)
          {
            str += CNetUtil::addrToString(server);
            str += "/";
          }
        }
        TBSYS_LOGGER.logMessage(level, file, line, function,
            "family id: %"PRI64_PREFIX"d,block_id: %u, version: %d, file_count: %d, size: %d, del_file_count: %d, del_size: %d, seq_no: %d, servers: %s",
            family_id_, info_.block_id_, info_.version_, info_.file_count_,
            info_.size_, info_.del_file_count_, info_.del_size_,
            info_.seq_no_, str.c_str());
      }
    }

    void BlockCollect::callback(LayoutManager& manager)
    {
      if (NULL != servers_)
      {
        uint64_t servers[SYSPARAM_NAMESERVER.max_replication_];
        ArrayHelper<uint64_t> helper(SYSPARAM_NAMESERVER.max_replication_, servers);
        uint64_t other[SYSPARAM_NAMESERVER.max_replication_];
        ArrayHelper<uint64_t> other_helper(SYSPARAM_NAMESERVER.max_replication_, other);
        manager.get_block_manager().get_mutex_(id()).wrlock();
        cleanup(helper, other_helper);
        manager.get_block_manager().get_mutex_(id()).unlock();
        for (int64_t index = 0; index < helper.get_array_index(); ++index)
        {
          uint64_t server = *helper.at(index);
          if (INVALID_SERVER_ID != server)
            manager.get_server_manager().relieve_relation(server, id());
        }
      }
    }

    void BlockCollect::cleanup(ArrayHelper<uint64_t>& removes, ArrayHelper<uint64_t>& expires)
    {
      if ((NULL != servers_ && server_size_ > 0))
      {
        server_size_ = 0;
        for (int8_t index = 0; index < common::SYSPARAM_NAMESERVER.max_replication_; ++index)
        {
          uint64_t server = servers_[index];
          servers_[index] = INVALID_SERVER_ID;
          if (INVALID_SERVER_ID != server)
          {
            removes.push_back(server);
            expires.push_back(server);
          }
        }
      }
    }
  }/** end namesapce nameserver **/
}/** end namesapce tfs **/
