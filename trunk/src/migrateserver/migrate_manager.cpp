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
#include "common/config_item.h"
#include "common/client_manager.h"
#include "common/status_message.h"
#include "message/client_cmd_message.h"
#include "message/get_dataserver_all_blocks_header.h"
#include "requester/ns_requester.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace migrateserver
  {
    MigrateManager::MigrateManager(const uint64_t ns_vip_port, const double balance_percent,
        const int64_t hot_time_range, AccessRatio& full_disk_ratio, AccessRatio& system_disk_ratio) :
      work_thread_(0),
      ns_vip_port_(ns_vip_port),
      balance_percent_(balance_percent),
      hot_time_range_(hot_time_range),
      max_block_size_(0),
      full_disk_access_ratio_(full_disk_ratio),
      system_disk_access_ratio_(system_disk_ratio)
    {
    }

    MigrateManager::~MigrateManager()
    {

    }

    int MigrateManager::initialize()
    {
      int32_t ret = requester::NsRequester::get_max_block_size(ns_vip_port_, max_block_size_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get max block size from ns fail, ret: %d", ret);
      }
      else
      {
        work_thread_ = new (std::nothrow) WorkThreadHelper(*this);
        assert(work_thread_!= 0);
      }
      return ret;
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


    int MigrateManager::keepalive(const common::DataServerStatInfo& server)
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
          std::pair<SERVER_MAP_ITER, bool> res =
              servers_.insert(SERVER_MAP::value_type(server.id_, server));
          res.first->second.last_update_time_ = Func::get_monotonic_time();
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
          TBSYS_LOG(DEBUG, "dataserver %s heart beat timeout, will remove it",
              tbsys::CNetUtil::addrToString(iter->first).c_str());
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
      const int32_t MAX_SLEEP_TIME = 30;//30s
      const int32_t MAX_ARRAY_SIZE = 128;
      const int32_t CHECK_COMPLETE_WAIT_TIME = 120;//120s

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
                if (weights >= 0)
                {
                  blocks_[index].insert(BLOCK_MAP::value_type(weights, std::make_pair(server, iter->info_.block_id_)));
                }
              }
            }
            NewClientManager::get_instance().destroy_client(client);
          }
        } while (retry_times-- > 0 && TFS_SUCCESS != ret);
      }
      return ret;
    }

    int64_t MigrateManager::calc_block_weight_(const common::IndexHeaderV2& info, const int32_t type) const
    {
      int64_t weights = -1;
      const int64_t now = time(NULL);
      const AccessRatio &ar = DATASERVER_DISK_TYPE_SYSTEM == type ? system_disk_access_ratio_ : full_disk_access_ratio_;
      const ThroughputV2 &th = info.throughput_;
      bool calc = common::DATASERVER_DISK_TYPE_SYSTEM == type ? true :
          (th.last_statistics_time_ + hot_time_range_ < now && is_full(info.info_));
      if (calc)
      {
        weights = th.last_statistics_time_ * ar.last_access_time_ratio +
            th.read_visit_count_ * ar.read_ratio + th.write_visit_count_ * ar.write_ratio +
            th.update_visit_count_ * ar.update_ratio + th.unlink_visit_count_* ar.unlink_ratio;
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
      int64_t total_capacity = 0, use_capacity = 0;
      statistic_all_server_info_(total_capacity, use_capacity);
      if (total_capacity > 0 && use_capacity > 0)
      {
        double avg_ratio = static_cast<double>(use_capacity)/static_cast<double>(total_capacity);
        tbutil::Mutex::Lock lock(mutex_);
        CONST_SERVER_MAP_ITER iter = servers_.begin();
        for (; iter != servers_.end(); ++iter)
        {
          const common::DataServerStatInfo& info = iter->second;
          if (INVALID_SERVER_ID != info.id_ && common::DATASERVER_DISK_TYPE_SYSTEM == info.type_
              && info.total_capacity_ > 0)
          {
            double curr_ratio = static_cast<double>(info.use_capacity_) / static_cast<double>(info.total_capacity_);

            if (curr_ratio < avg_ratio - balance_percent_)
            {
              entry.dest_addr_ = info.id_;
            }
            else if ((curr_ratio > (avg_ratio + balance_percent_))
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
        // full disk -> system disk (entry.dest_addr_ != INVALID_SERVER_ID)
        // system disk -> full disk (entry.dest_addr_ == INVALID_SERVER_ID)
        bool target = entry.dest_addr_ != INVALID_SERVER_ID;
        int32_t index = target ? 1 : 0;
        if (!blocks_[index].empty())
        {
          if (target)
          {
            CONST_BLOCK_MAP_ITER iter = blocks_[index].begin();
            entry.block_id_ = (*iter).second.second;
            entry.source_addr_ = (*iter).second.first;
          }
          else
          {
            CONST_BLOCK_MAP_REVERSE_ITER iter = blocks_[index].rbegin();
            entry.block_id_ = (*iter).second.second;
            ret = choose_move_dest_server_(entry.source_addr_, entry.dest_addr_) ? TFS_SUCCESS : EXIT_CHOOSE_MIGRATE_DEST_SERVER_ERROR;
          }
        }
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
        req_msg.set_value4(REPLICATE_BLOCK_MOVE_FLAG_YES);
        req_msg.set_value5(MOVE_BLOCK_NO_CHECK_RACK_FLAG_YES);
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
      TBSYS_LOG(INFO, "send migrate message %s, ret: %d, error msg: %s block: %"PRI64_PREFIX"u, source: %s, dest: %s , ns_vip: %s",
          TFS_SUCCESS == ret ? "successful" : "failed", ret, msg, current.block_id_, tbsys::CNetUtil::addrToString(current.source_addr_).c_str(),
          tbsys::CNetUtil::addrToString(current.dest_addr_).c_str(), tbsys::CNetUtil::addrToString(ns_vip_port_).c_str());
      return ret;
    }

    bool MigrateManager::choose_move_dest_server_(const uint64_t source_addr, uint64_t& dest_addr) const
    {
      dest_addr = INVALID_SERVER_ID;
      tbutil::Mutex::Lock lock(mutex_);
      VUINT64 server_ids;
      CONST_SERVER_MAP_ITER iter = servers_.begin();
      for (; iter != servers_.end(); ++iter)
      {
        server_ids.push_back(iter->second.id_);
      }

      int32_t random_index = -1, retry_times = servers_.size();
      while (retry_times-- > 0 && INVALID_SERVER_ID == dest_addr)
      {
        random_index = random() % server_ids.size();
        if (source_addr != server_ids[random_index]) // not same with source
        {
          dest_addr = server_ids[random_index];
        }
      }
      return dest_addr != INVALID_SERVER_ID;
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
