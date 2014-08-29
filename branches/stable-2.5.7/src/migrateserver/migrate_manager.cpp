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
#include "message/client_ns_keepalive_message.h"
#include "requester/ns_requester.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace migrateserver
  {
    static const int64_t MAX_WEIGTH = 0x7FFFFFFFFFFFFFFF;

    MigrateManager::MigrateManager() :
      work_thread_(0),
      not_full_block_count_(0),
      max_block_size_(0),
      migrate_complete_wait_time_(120),
      update_statistic_interval_(3600)
    {
    }

    MigrateManager::~MigrateManager()
    {

    }

    int MigrateManager::initialize()
    {
      ns_vip_port_ = SYSPARAM_MIGRATESERVER.ns_vip_port_;
      balance_percent_ = SYSPARAM_MIGRATESERVER.balance_percent_;
      penalty_percent_ = SYSPARAM_MIGRATESERVER.penalty_percent_;
      update_statistic_interval_ = SYSPARAM_MIGRATESERVER.update_statistic_interval_;
      hot_time_range_ = SYSPARAM_MIGRATESERVER.hot_time_range_;
      full_disk_access_ratio_ = SYSPARAM_MIGRATESERVER.full_disk_access_ratio_;
      system_disk_access_ratio_ = SYSPARAM_MIGRATESERVER.system_disk_access_ratio_;

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
          TBSYS_LOG(INFO, "dataserver %s heart beat timeout, will remove it",
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
      int64_t last_get_index_time = 0;
      int64_t last_get_ns_parameter_time = 0;
      const int32_t MAX_SLEEP_TIME = 30;//30s
      const int32_t MAX_ARRAY_SIZE = 128;
      const int32_t WAIT_REPLICACTE_AND_REINSTATE_TIME = 300;// 5 min
      const int32_t MAX_UPDATE_INTERVAL = 300;// 5 min
      std::pair<uint64_t, int32_t> array[MAX_ARRAY_SIZE];
      common::ArrayHelper<std::pair<uint64_t, int32_t> >helper(MAX_ARRAY_SIZE, array);
      migrateserver::MsRuntimeGlobalInformation& mrgi= migrateserver::MsRuntimeGlobalInformation::instance();
      Func::sleep(MAX_SLEEP_TIME, mrgi.is_destroy_);// wait for ds to report finish
      while (!mrgi.is_destroyed())
      {
        int64_t now = Func::get_monotonic_time();
        // update sleep interval after migrate one block successfully
        if (now >= last_get_ns_parameter_time + MAX_UPDATE_INTERVAL)
        {
          get_ns_config_parameter_();
          last_get_ns_parameter_time = now;
        }

        // update all blocks statistic info
        if (now >= last_get_index_time + update_statistic_interval_)
        {
          helper.clear();
          blocks_[0].clear(); // system disk's hot blocks list
          blocks_[1].clear(); // data disk's cold blocks list
          not_full_block_count_ = 0;

          get_all_servers_(helper);
          for (index = 0; index < helper.get_array_index(); ++index)
          {
            std::pair<uint64_t, int32_t>* item = helper.at(index);
            get_index_header_(item->first, item->second);
          }
          last_get_index_time = now;
          TBSYS_LOG(INFO, "update %"PRI64_PREFIX"d dataservers blocks statistic info,"
              " full disks'cold blocks count: %zd, system disks' blocks count: %zd",
              helper.get_array_index(), blocks_[1].size(), blocks_[0].size());
        }

        MigrateEntry entry;
        memset(&entry, 0, sizeof(entry));
        calc_system_disk_migrate_info_(entry);
        if (entry.source_addr_ != INVALID_SERVER_ID
            || entry.dest_addr_ != INVALID_SERVER_ID)
        {
          int32_t ret = choose_migrate_entry_(entry);

          if (TFS_SUCCESS == ret)
          {
            ret = do_migrate_(entry);
          }

          if (TFS_SUCCESS == ret)
          {
            Func::sleep(migrate_complete_wait_time_, mrgi.is_destroy_);
          }
          else if (EXIT_CANNOT_MIGRATE_BLOCK_ERROR == ret)
          {
            // wait for ns's replcate or reinstate dissove queue deal finished
            Func::sleep(WAIT_REPLICACTE_AND_REINSTATE_TIME, mrgi.is_destroy_);
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
        do
        {
          create_msg_ref(GetAllBlocksHeaderMessage, req_msg);
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

    int64_t MigrateManager::calc_block_weight_(const common::IndexHeaderV2& iheader, const int32_t type)
    {
      int64_t weights = -1;
      const int64_t now = time(NULL);
      const AccessRatio &ar = DATASERVER_DISK_TYPE_SYSTEM == type ? system_disk_access_ratio_ : full_disk_access_ratio_;
      const ThroughputV2 &th = iheader.throughput_;
      bool calc = false;
      if (common::DATASERVER_DISK_TYPE_SYSTEM == type)
      { // the hot block can be migrated out of system disk
        if (!is_full(iheader.info_))
        {
          weights = MAX_WEIGTH; //compacted cold block migrate back to full disk for re-write firstly
          ++not_full_block_count_;
        }
        else if (iheader.info_.last_access_time_ + hot_time_range_/2 > now)
        {
          calc = true;
        }
      }
      else
      { // the cold block can be migrated into system disk
        if (iheader.info_.last_access_time_ + hot_time_range_ < now && is_full(iheader.info_))
        {
          calc = true;
        }
      }

      if (calc)
      {
        weights = iheader.info_.last_access_time_ * ar.last_access_time_ratio +
            th.read_visit_count_ * ar.read_ratio + th.write_visit_count_ * ar.write_ratio +
            th.update_visit_count_ * ar.update_ratio + th.unlink_visit_count_* ar.unlink_ratio;
        assert(MAX_WEIGTH != weights);// happened only statistic occur bug
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

            if (not_full_block_count_ > 0)
            {
              entry.source_addr_ = info.id_;
            }
            else if (curr_ratio < avg_ratio - balance_percent_)
            {
              entry.dest_addr_ = info.id_;
            }
            else if (!blocks_[0].empty() && !blocks_[1].empty())
            {
              uint64_t system_disk_max_weight = static_cast<uint64_t>(blocks_[0].rbegin()->first * penalty_percent_);
              uint64_t full_disk_min_weight = blocks_[1].begin()->first;
              if (system_disk_max_weight > full_disk_min_weight)
              {
                entry.source_addr_ = info.id_;
              }
              TBSYS_LOG(DEBUG, "system disk max block: %"PRI64_PREFIX"u(%"PRI64_PREFIX"u), "
                  "full disk min block: %"PRI64_PREFIX"u(%"PRI64_PREFIX"u)", blocks_[0].rbegin()->second.second, system_disk_max_weight,
                  blocks_[1].begin()->second.second, full_disk_min_weight);
            }
            TBSYS_LOG(DEBUG, "dataserver count: %zd, system disk curr_ratio: %.3lf%%, [%.3lf%%, %.3lf%%], "
                "will do migrate: %s, not full block count: %"PRI64_PREFIX"d", servers_.size(),
                curr_ratio * 100.0, (avg_ratio - balance_percent_) * 100.0, avg_ratio * 100.0,
                (entry.dest_addr_ != INVALID_SERVER_ID || entry.source_addr_ != INVALID_SERVER_ID) ?  "yes":"no", not_full_block_count_);
          }
        }
      }
    }

    int MigrateManager::choose_migrate_entry_(MigrateEntry& entry)
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
            BLOCK_MAP_ITER iter = blocks_[index].begin(); // point to min weight block in full disk
            entry.block_id_ = (*iter).second.second;
            entry.source_addr_ = (*iter).second.first;
            blocks_[index].erase(iter);
          }
          else
          {
            BLOCK_MAP_ITER iter = blocks_[index].end();
            --iter; // point to max weight block in system disk
            entry.block_id_ = (*iter).second.second;
            ret = choose_move_dest_server_(entry.source_addr_, entry.dest_addr_) ? TFS_SUCCESS : EXIT_CHOOSE_MIGRATE_DEST_SERVER_ERROR;
            if (TFS_SUCCESS == ret)
            {
              if (MAX_WEIGTH == (*iter).first)
              {
                assert(not_full_block_count_ > 0);
                --not_full_block_count_;
                TBSYS_LOG(DEBUG, "will migrate not full block: %"PRI64_PREFIX"u, from: %s to : %s",
                    entry.block_id_, tbsys::CNetUtil::addrToString(entry.source_addr_).c_str(),
                    tbsys::CNetUtil::addrToString(entry.dest_addr_).c_str());
              }
              blocks_[index].erase(iter);
            }
          }
        }
        else
        {
          ret = EXIT_NO_BLOCK_NEED_MIGRATE_ERROR;
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
        int32_t retry_times = 3;
        const int32_t TIMEOUT_MS = 2000;
        do
        {
          create_msg_ref(ClientCmdMessage, req_msg);
          req_msg.set_value1(current.source_addr_);
          req_msg.set_value2(current.dest_addr_);
          req_msg.set_value3(current.block_id_);
          req_msg.set_value4(REPLICATE_BLOCK_MOVE_FLAG_YES);
          req_msg.set_value5(MOVE_BLOCK_NO_CHECK_RACK_FLAG_YES);
          req_msg.set_cmd(CLIENT_CMD_IMMEDIATELY_REPL);
          NewClient* client = NewClientManager::get_instance().create_client();
          ret = (NULL != client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
          if (TFS_SUCCESS == ret)
          {
            tbnet::Packet* result = NULL;
            ret = send_msg_to_server(ns_vip_port_, client, &req_msg, result, TIMEOUT_MS);
            if (TFS_SUCCESS == ret)
            {
              ret = STATUS_MESSAGE == result->getPCode() ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
            }
            if (TFS_SUCCESS == ret)
            {
              StatusMessage* rsp = dynamic_cast<StatusMessage*>(result);
              int32_t len = std::min(static_cast<int32_t>(rsp->get_error_msg_length()), 256);
              len = std::max(0, len);
              strncpy(msg, rsp->get_error(), len);
              ret = rsp->get_status();
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        while (retry_times-- > 0 && TFS_SUCCESS != ret
            && EXIT_CANNOT_MIGRATE_BLOCK_ERROR != ret);
      }
      TBSYS_LOG(INFO, "send migrate message %s, ret: %d, error msg: %s, block: %"PRI64_PREFIX"u, source: %s, dest: %s, ns_vip: %s",
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

    void MigrateManager::get_ns_config_parameter_()
    {
      NewClient* new_client = NewClientManager::get_instance().create_client();
      int ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        create_msg_ref(ClientNsKeepaliveMessage, req_msg);
        req_msg.set_flag(DS_TABLE_NONE);

        tbnet::Packet* ret_msg = NULL;
        ret = send_msg_to_server(ns_vip_port_, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (CLIENT_NS_KEEPALIVE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            ClientNsKeepaliveResponseMessage* msg =
              dynamic_cast<ClientNsKeepaliveResponseMessage*>(ret_msg);
            ClusterConfig& config = msg->get_cluster_config();
            migrate_complete_wait_time_ = config.migrate_complete_wait_time_;
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }
      TBSYS_LOG(INFO, "get parameter from ns %s, migrate_complete_wait_time: %d(s), ret: %d",
          TFS_SUCCESS == ret ? "success" : "fail", migrate_complete_wait_time_, ret);
    }

  }/** end namespace migrateserver **/
}/** end namesapce tfs **/
