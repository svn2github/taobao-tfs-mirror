/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_manager.cpp 746 2013-09-02 07:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <string>
#include "migrate_manager.h"
#include "common/error_msg.h"
#include "common/array_helper.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/client_cmd_message.h"
#include "message/get_dataserver_all_blocks_header.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace migrateserver
  {
    MigrateManager::MigrateManager(const uint64_t ns_vip_port) :
      work_thread_(0),
      ns_vip_port_(ns_vip_port)
    {

    }

    MigrateManager::~MigrateManager()
    {

    }

    int MigrateManager::initialize()
    {
      work_thread_ = new (std::nothrow) WorkThreadHelper(*this);
      assert(work_thread_!= 0);
      return TFS_SUCCESS;
    }

    int MigrateManager::destroy()
    {
      if (0 != work_thread_)
      {
        work_thread_->join();
        work_thread_ = 0;
      }
      return TFS_SUCCESS;
    }

    int MigrateManager::keepalive(common::DataServerStatInfo& server)
    {
      int32_t ret = (INVALID_SERVER_ID  == server.id_) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        tbutil::Mutex::Lock lock(mutex_);
        SERVER_MAP_ITER iter = servers_.find(server.id_);
        if (servers_.end() != iter)
        {
          iter->second = server;
          iter->second.last_update_time_ = Func::get_monotonic_time();
        }
        else
        {
          servers_.insert(SERVER_MAP::value_type(server.id_, server));
        }
      }
      return ret;
    }

    void MigrateManager::timeout(const int64_t now)
    {
      const int32_t MAX_TIMEOUT_TIME = 60;//60s
      tbutil::Mutex::Lock lock(mutex_);
      SERVER_MAP_ITER iter = servers_.begin();
      while (iter != servers_.end())
      {
        if (now - iter->second.last_update_time_ > MAX_TIMEOUT_TIME)
        {
          servers_.erase(iter++);
        }
        else
        {
          ++iter;
        }
      }
    }

    void MigrateManager::run_()
    {
      int64_t index  = 0;
      const int32_t MAX_SLEEP_TIME = 5;//5s
      const int32_t MAX_ARRAY_SIZE = 128;
      const int32_t CHECK_COMPLETE_WAIT_TIME = 60;//120s

      std::pair<uint64_t, int32_t> array[MAX_ARRAY_SIZE];
      common::ArrayHelper<std::pair<uint64_t, int32_t> >helper(MAX_ARRAY_SIZE, array);
      migrateserver::MsRuntimeGlobalInformation& mrgi= migrateserver::MsRuntimeGlobalInformation::instance();
      while (!mrgi.is_destroyed())
      {
        helper.clear();
        blocks_[0].clear();
        blocks_[1].clear();

        MigrateEntry entry;
        memset(&entry, 0, sizeof(entry));
        calc_system_disk_migrate_info_(entry);
        if (entry.source_addr_ != INVALID_SERVER_ID
            || entry.dest_addr_ != INVALID_SERVER_ID)
        {
          get_all_servers_(helper);
          for (index = 0; index < helper.get_array_index(); ++index)
          {
            std::pair<uint64_t, int32_t>* item = helper.at(index);
            get_index_header_(item->first, item->second);
          }
          int32_t ret = choose_migrate_entry_(entry);
          if (TFS_SUCCESS == ret)
          {
            ret = do_migrate_(entry);
          }
          if (TFS_SUCCESS == ret)
          {
            Func::sleep(CHECK_COMPLETE_WAIT_TIME, mrgi.is_destroy_);
          }
        }
        Func::sleep(MAX_SLEEP_TIME, mrgi.is_destroy_);
      }
    }

    int MigrateManager::get_index_header_(const uint64_t server, const int32_t type)
    {
      int32_t retry_times = 3;
      const int32_t TIMEOUT_MS = 2000;
      int32_t ret = (INVALID_SERVER_ID != server) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        GetAllBlocksHeaderMessage req_msg;
        do
        {
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            tbnet::Packet* result = NULL;
            ret = send_msg_to_server(server, client, &req_msg, result, TIMEOUT_MS);
            if (TFS_SUCCESS == ret)
            {
              ret = GET_ALL_BLOCKS_HEADER_RESP_MESSAGE == result->getPCode() ? TFS_SUCCESS : EXIT_GET_ALL_BLOCK_HEADER_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              int32_t index = DATASERVER_DISK_TYPE_SYSTEM == type ? 0 : 1;
              GetAllBlocksHeaderRespMessage* rsp = dynamic_cast<GetAllBlocksHeaderRespMessage*>(result);
              std::vector<IndexHeaderV2>& blocks = rsp->get_all_blocks_header();
              std::vector<common::IndexHeaderV2>::const_iterator iter = blocks.begin();
              for (; iter != blocks.end(); ++iter)
              {
                int64_t weights = calc_block_weight_((*iter), type);
                blocks_[index].insert(BLOCK_MAP::value_type(weights, std::make_pair(server,(*iter).info_.block_id_)));
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        while (retry_times-- > 0 && TFS_SUCCESS != ret);
      }
      return ret;
    }

    int64_t MigrateManager::calc_block_weight_(const common::IndexHeaderV2& info, const int32_t type) const
    {
      int64_t weights = INT64_MAX -1;
      const int32_t TWO_MONTH = 2 * 31 * 86400;
      const int32_t LAST_ACCESS_TIME_RATIO = common::DATASERVER_DISK_TYPE_SYSTEM == type ? 5 : 75;
      const int32_t READ_RATIO   = common::DATASERVER_DISK_TYPE_SYSTEM == type ? 50 : 10;
      const int32_t WRITE_RATIO  = common::DATASERVER_DISK_TYPE_SYSTEM == type ? 30 : 5;
      const int32_t UPDATE_RATIO = common::DATASERVER_DISK_TYPE_SYSTEM == type ? 10 : 5;
      const int32_t UNLINK_RATIO = common::DATASERVER_DISK_TYPE_SYSTEM == type ? 5  : 5;
      const ThroughputV2& th = info.throughput_;
      const int64_t now = Func::get_monotonic_time();
      if (th.last_statistics_time_+ TWO_MONTH < now)
      {
        weights = th.last_statistics_time_ * LAST_ACCESS_TIME_RATIO+ th.read_visit_count_ * READ_RATIO
          + th.write_visit_count_ * WRITE_RATIO + th.update_visit_count_ * UPDATE_RATIO
          + th.unlink_visit_count_* UNLINK_RATIO;
      }
      return weights;
    }

    void MigrateManager::get_all_servers_(common::ArrayHelper<std::pair<uint64_t, int32_t> >& servers) const
    {
      tbutil::Mutex::Lock lock(mutex_);
      CONST_SERVER_MAP_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        if (iter->second.id_ != INVALID_SERVER_ID)
        {
          servers.push_back(std::make_pair(iter->second.id_, iter->second.type_));
        }
      }
    }

    void MigrateManager::calc_system_disk_migrate_info_(MigrateEntry& entry) const
    {
      memset(&entry, 0, sizeof(entry));
      const double balance_percent = 0.05;
      int64_t total_capacity = 0, use_capacity = 0;
      statistic_all_server_info_(total_capacity, use_capacity);
      if (total_capacity > 0 && use_capacity > 0)
      {
        tbutil::Mutex::Lock lock(mutex_);
        CONST_SERVER_MAP_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          const common::DataServerStatInfo& info = iter->second;
          if (INVALID_SERVER_ID != info.id_ && common::DATASERVER_DISK_TYPE_SYSTEM == info.type_)
          {
            double avg_ratio = static_cast<double>(use_capacity)/static_cast<double>(total_capacity);
            double curr_ratio= static_cast<double>(info.use_capacity_) / static_cast<double>(info.total_capacity_);

            if (curr_ratio < avg_ratio - balance_percent)
            {
              entry.dest_addr_ = info.id_;
            }
            else if ((curr_ratio > (avg_ratio + balance_percent))
                || curr_ratio >= 1.0)
            {
              entry.source_addr_ = info.id_;
            }
          }
        }
      }
    }

    int MigrateManager::choose_migrate_entry_(MigrateEntry& entry) const
    {
      int32_t ret = (entry.dest_addr_ != INVALID_SERVER_ID
          || entry.source_addr_ != INVALID_SERVER_ID) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        bool target = entry.dest_addr_ != INVALID_SERVER_ID;
        int32_t index = target ? 1 : 0;
        CONST_BLOCK_MAP_ITER iter = blocks_[index].begin();
        entry.block_id_    = (*iter).second.second;
        if (target)
          ret = choose_source_server_(entry.dest_addr_, entry.source_addr_) ? TFS_SUCCESS : EXIT_CHOOSE_SOURCE_SERVER_ERROR;
        else
          ret = choose_target_server_(entry.source_addr_, entry.dest_addr_) ? TFS_SUCCESS : EXIT_CHOOSE_TARGET_SERVER_INSUFFICIENT_ERROR;
      }
      return ret;
    }

    int MigrateManager::do_migrate_(MigrateEntry& current)
    {
      char msg[256] = {'\0'};
      int32_t ret = (current.block_id_ != INVALID_BLOCK_ID
          && current.source_addr_ != INVALID_SERVER_ID
          && current.dest_addr_ != INVALID_SERVER_ID) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ClientCmdMessage req_msg;
        req_msg.set_value1(current.source_addr_);
        req_msg.set_value2(current.dest_addr_);
        req_msg.set_value3(current.block_id_);
        req_msg.set_cmd(CLIENT_CMD_IMMEDIATELY_REPL);
        int32_t retry_times = 3;
        const int32_t TIMEOUT_MS = 2000;
        do
        {
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            tbnet::Packet* result = NULL;
            ret = send_msg_to_server(ns_vip_port_, client, &req_msg, result, TIMEOUT_MS);
            if (TFS_SUCCESS == ret)
            {
              ret = STATUS_MESSAGE == result->getPCode() ? TFS_SUCCESS : EXIT_SEND_MIGRATE_MSG_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              StatusMessage* rsp = dynamic_cast<StatusMessage*>(result);
              int32_t len = std::min(static_cast<int32_t>(rsp->get_error_msg_length()), 256);
              len = std::max(0, len);
              strncpy(msg, rsp->get_error(), len);
              ret = STATUS_MESSAGE_OK == rsp->get_status() ? TFS_SUCCESS : EXIT_SEND_MIGRATE_MSG_ERROR;
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        while (retry_times-- > 0 && TFS_SUCCESS != ret);
      }
      TBSYS_LOG(INFO, "send migrate message %s, ret: %d,error msg: %s block: %"PRI64_PREFIX"u, source: %s, dest: %s , ns_vip: %s",
        TFS_SUCCESS == ret ? "successful" : "failed", ret, msg, current.block_id_, tbsys::CNetUtil::addrToString(current.source_addr_).c_str(),
        tbsys::CNetUtil::addrToString(current.dest_addr_).c_str(), tbsys::CNetUtil::addrToString(ns_vip_port_).c_str());
      return ret;
    }

    bool MigrateManager::choose_target_server_(const uint64_t source_addr, uint64_t& dest_addr) const
    {
      dest_addr = INVALID_SERVER_ID;
      tbutil::Mutex::Lock lock(mutex_);
      int32_t random_index = -1, retry_times = servers_.size();
      while (retry_times-- > 0 && INVALID_SERVER_ID == dest_addr)
      {
        random_index = random() % servers_.size();
        CONST_SERVER_MAP_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          if (source_addr != iter->second.id_)
            dest_addr = iter->second.id_;
        }
      }
      return dest_addr == INVALID_SERVER_ID;
    }

    bool MigrateManager::choose_source_server_(const uint64_t dest_addr, uint64_t& source_addr) const
    {
      source_addr = INVALID_SERVER_ID;
      tbutil::Mutex::Lock lock(mutex_);
      int32_t random_index = -1, retry_times = servers_.size();
      while (retry_times-- > 0 && INVALID_SERVER_ID == source_addr)
      {
        random_index = random() % servers_.size();
        CONST_SERVER_MAP_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          if (dest_addr != iter->second.id_)
            source_addr = iter->second.id_;
        }
      }
      return source_addr == INVALID_SERVER_ID;
    }

    void MigrateManager::statistic_all_server_info_(int64_t& total_capacity, int64_t& use_capacity) const
    {
      tbutil::Mutex::Lock lock(mutex_);
      total_capacity = 0, use_capacity = 0;
      CONST_SERVER_MAP_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        if (iter->second.id_ != INVALID_SERVER_ID)
        {
          total_capacity += iter->second.total_capacity_;
          use_capacity   += iter->second.use_capacity_;
        }
      }
    }
  }/** end namespace migrateserver **/
}/** end namesapce tfs **/
