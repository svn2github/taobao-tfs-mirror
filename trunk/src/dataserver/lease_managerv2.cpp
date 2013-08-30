/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <Timer.h>
#include <Mutex.h>
#include "common/func.h"
#include "common/atomic.h"
#include "common/internal.h"
#include "dataservice.h"

#include "lease_managerv2.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace dataserver
  {
    LeaseManager::LeaseManager(DataService& service,
        const std::vector<uint64_t>& ns_ip_port):
      service_(service)
    {
      memset(lease_meta_, 0, sizeof(lease_meta_));
      memset(lease_status_, 0, sizeof(lease_status_));
      memset(last_renew_time_, 0, sizeof(last_renew_time_));
      for (uint32_t i = 0; i < ns_ip_port.size(); i++)
      {
        ns_ip_port_[i] = ns_ip_port[i];
      }

      for (int32_t i = 0; i < MAX_SINGLE_CLUSTER_NS_NUM; i++)
      {
        lease_thread_[i] = 0;
      }

      apply_block_thread_ = 0;
    }

    LeaseManager::~LeaseManager()
    {
    }

    int LeaseManager::initialize()
    {
      DataServerStatInfo& info = DsRuntimeGlobalInformation::instance().information_;
      info.startup_time_ = time(NULL);
      IpAddr* adr = reinterpret_cast<IpAddr*>(&info.id_);
      adr->ip_ = tbsys::CNetUtil::getAddr(service_.get_ip_addr());
      adr->port_ = service_.get_listen_port();
      for (int32_t i = 0; i < MAX_SINGLE_CLUSTER_NS_NUM; i++)
      {
        if (0 != ns_ip_port_[i])
        {
          lease_thread_[i] = new (std::nothrow)RunLeaseThreadHelper(*this, i);
          assert(0 != lease_thread_[i]);
        }
      }
      apply_block_thread_ = new (std::nothrow)RunApplyBlockThreadHelper(*this);
      assert(0 != apply_block_thread_);
      info.status_ = DATASERVER_STATUS_ALIVE;
      return TFS_SUCCESS;
    }

    void LeaseManager::destroy()
    {
      for (int32_t i = 0; i < MAX_SINGLE_CLUSTER_NS_NUM; i++)
      {
        if (0 != lease_thread_[i])
        {
          lease_thread_[i]->join();
        }
      }

      if (0 != apply_block_thread_)
      {
        apply_block_thread_->join();
      }
    }

    WritableBlockManager& LeaseManager::get_writable_block_manager()
    {
      return service_.get_writable_block_manager();
    }

    bool LeaseManager::has_valid_lease(const time_t now) const
    {
      bool valid = false;
      for (int i = 0; i < MAX_SINGLE_CLUSTER_NS_NUM && !valid; i++)
      {
        if (0 != ns_ip_port_[i])
        {
          valid = (lease_meta_[i].ns_role_ == NS_ROLE_MASTER) && !is_expired(now, i);
        }
      }
      return valid;
    }

    int LeaseManager::alloc_writable_block(WritableBlock*& block)
    {
      int ret = has_valid_lease(Func::get_monotonic_time()) ?
        TFS_SUCCESS : EXIT_BLOCK_LEASE_INVALID_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_writable_block_manager().alloc_writable_block(block);
      }
      return ret;
    }

    int LeaseManager::alloc_update_block(const uint64_t block_id, WritableBlock*& block)
    {
      int ret = has_valid_lease(Func::get_monotonic_time()) ?
        TFS_SUCCESS : EXIT_BLOCK_LEASE_INVALID_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = get_writable_block_manager().alloc_update_block(block_id, block);
      }
      return ret;
    }

    void LeaseManager::free_writable_block(const uint64_t block_id)
    {
      return get_writable_block_manager().free_writable_block(block_id);
    }

    void LeaseManager::expire_block(const uint64_t block_id)
    {
      return get_writable_block_manager().expire_one_block(block_id);
    }

    ///////////////////////////////////////////////////////////////////////////////

    int LeaseManager::apply(const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsApplyLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ns_ip_port_[who], new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (DS_APPLY_LEASE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            DsApplyLeaseResponseMessage* resp_msg = dynamic_cast<DsApplyLeaseResponseMessage* >(ret_msg);
            process_apply_response(resp_msg, who);
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = resp_msg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }

      return ret;
    }

    void LeaseManager::process_apply_response(DsApplyLeaseResponseMessage* response, const int32_t who)
    {
      assert(NULL != response);
      lease_meta_[who] = response->get_lease_meta();
      last_renew_time_[who] = Func::get_monotonic_time();
      if (NS_ROLE_MASTER == lease_meta_[who].ns_role_)
      {
        DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
        ds_info.max_mr_network_bandwidth_mb_ = lease_meta_[who].max_mr_network_bandwith_;
        ds_info.max_rw_network_bandwidth_mb_ = lease_meta_[who].max_rw_network_bandwith_;
        ds_info.max_block_size_ = lease_meta_[who].max_block_size_;
        ds_info.max_write_file_count_ = lease_meta_[who].max_write_file_count_;
      }
    }

    int LeaseManager::renew(const int32_t timeout_ms, const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsRenewLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);
      if (is_master(who))
      {
        BlockInfoV2* block_infos = req_msg.get_block_infos();
        ArrayHelper<BlockInfoV2> blocks(MAX_WRITABLE_BLOCK_COUNT, block_infos);
        get_writable_block_manager().get_blocks(blocks, BLOCK_WRITABLE);
        req_msg.set_size(blocks.get_array_index());
      }

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ns_ip_port_[who], new_client, &req_msg, ret_msg, timeout_ms);
        if (TFS_SUCCESS == ret)
        {
          if (DS_RENEW_LEASE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            DsRenewLeaseResponseMessage* resp_msg = dynamic_cast<DsRenewLeaseResponseMessage* >(ret_msg);
            process_renew_response(resp_msg, who);
          }
          else if (STATUS_MESSAGE == ret_msg->getPCode())
          {
            StatusMessage* resp_msg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = resp_msg->get_status();
          }
          else
          {
            ret = EXIT_UNKNOWN_MSGTYPE;
          }
        }
        NewClientManager::get_instance().destroy_client(new_client);
      }

      return ret;
    }

    void LeaseManager::process_renew_response(DsRenewLeaseResponseMessage* response, const int32_t who)
    {
      assert(NULL != response);
      lease_meta_[who] = response->get_lease_meta();
      last_renew_time_[who] = Func::get_monotonic_time();
      if (NS_ROLE_MASTER == lease_meta_[who].ns_role_)
      {
        DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
        ds_info.max_mr_network_bandwidth_mb_ = lease_meta_[who].max_mr_network_bandwith_;
        ds_info.max_rw_network_bandwidth_mb_ = lease_meta_[who].max_rw_network_bandwith_;
        ds_info.max_block_size_ = lease_meta_[who].max_block_size_;
        ds_info.max_write_file_count_ = lease_meta_[who].max_write_file_count_;
      }

      ArrayHelper<BlockLease> leases(response->get_size(),
          response->get_block_lease(), response->get_size());
      for (int index = 0; index < response->get_size(); index++)
      {
        BlockLease& lease = *leases.at(index);
        if (TFS_SUCCESS == lease.result_)
        {
          WritableBlock* block = get_writable_block_manager().get(lease.block_id_);
          if (NULL != block)
          {
            // update replica information
            ArrayHelper<uint64_t> helper(lease.size_, lease.servers_, lease.size_);
            block->set_servers(helper);
          }
        }
        else  // move to expired list
        {
          get_writable_block_manager().expire_one_block(lease.block_id_);
        }
        TBSYS_LOG(DEBUG, "renew block %"PRI64_PREFIX"u, replica: %d, ret: %d",
            lease.block_id_, lease.size_, lease.result_);
      }
    }

    int LeaseManager::giveup(const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsGiveupLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      ret = (NULL != new_client) ? TFS_SUCCESS : EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      if (TFS_SUCCESS == ret)
      {
        // no need to process return message
        ret = send_msg_to_server(ns_ip_port_[who], new_client, &req_msg, ret_msg);
        NewClientManager::get_instance().destroy_client(new_client);
      }

      return ret;
    }

    void LeaseManager::RunLeaseThreadHelper::run()
    {
      manager_.run_lease(who_);
    }

    void LeaseManager::RunApplyBlockThreadHelper::run()
    {
      manager_.run_apply_and_giveup();
    }

    void LeaseManager::update_stat(const int32_t who)
    {
      DataServerStatInfo& info = DsRuntimeGlobalInformation::instance().information_;
      if (0 == who % MAX_SINGLE_CLUSTER_NS_NUM)
      {
        service_.get_block_manager().get_space(info.total_capacity_, info.use_capacity_);
        info.block_count_ = service_.get_block_manager().get_all_logic_block_count();
        info.current_load_ = Func::get_load_avg();
        info.current_time_ = time(NULL);
      }
    }

    void LeaseManager::run_lease(const int32_t who)
    {
      int ret = TFS_SUCCESS;
      int32_t SLEEP_TIME_US = 1 * 1000 * 1000;  // 1 seconds
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        if (lease_status_[who] == LEASE_APPLY)
        {
          update_stat(who);
          ret = apply(who);
          if (TFS_SUCCESS == ret)
          {
            lease_status_[who] = LEASE_RENEW;
          }
          TBSYS_LOG(INFO, "apply lease from %s, who: %d, result: %d",
              tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, ret);
        }

        if ((lease_status_[who] == LEASE_RENEW)
            && need_renew(Func::get_monotonic_time(), who))
        {
          update_stat(who);
          // retry renew lease, if renew fail, switch to APPLY state
          for (int i = 0; i < lease_meta_[who].renew_retry_times_; i++)
          {
            ret = renew(lease_meta_[who].renew_retry_timeout_ * 1000, who);
            if (TFS_SUCCESS == ret)
            {
              break;
            }
          }

          if (TFS_SUCCESS == ret)
          {
            get_writable_block_manager().expire_update_blocks();
          }
          else
          {
            lease_status_[who] = LEASE_APPLY;
            get_writable_block_manager().expire_all_blocks();  // lease expired
          }
          TBSYS_LOG(INFO, "renew lease from %s, who: %d, result: %d",
              tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, ret);
        }

        usleep(SLEEP_TIME_US);
      }

      ret = giveup(who);
      TBSYS_LOG(INFO, "giveup lease from %s, who: %d, result: %d",
          tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, ret);
    }

    void LeaseManager::run_apply_and_giveup()
    {
      int ret = TFS_SUCCESS;
      const int32_t SLEEP_TIME_US = 1 * 1000 * 1000;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        int64_t now = Func::get_monotonic_time();
        if (has_valid_lease(now))
        {
          TBSYS_LOG(INFO, "writable block info, writable: %d, update: %d, expired: %d",
              get_writable_block_manager().size(BLOCK_WRITABLE),
              get_writable_block_manager().size(BLOCK_UPDATE),
              get_writable_block_manager().size(BLOCK_EXPIRED));

          // giveup expired block first
          int32_t expired = get_writable_block_manager().size(BLOCK_EXPIRED);
          if (expired > 0)
          {
            ret = get_writable_block_manager().giveup_writable_block();
            TBSYS_LOG(DEBUG, "giveup writable block, count: %d, ret: %d", expired, ret);
          }

          // sleep 100ms between giveup and apply, wait for giveup finish
          if (expired > 0)
          {
            usleep(100000);
          }

          // apply writable block
          int32_t need = ds_info.max_write_file_count_ -
            get_writable_block_manager().size(BLOCK_WRITABLE);
          if (need > 0)
          {
            ret = get_writable_block_manager().apply_writable_block(need);
            TBSYS_LOG(DEBUG, "apply writable block, count: %d, ret: %d", need, ret);
          }
        }

        usleep(SLEEP_TIME_US);
      }
    }

    int LeaseManager::timeout(const time_t now)
    {
      //if (is_expired(now))
      //{
      //  get_writable_block_manager().expire_all_blocks();
      //}
      UNUSED(now);
      return TFS_SUCCESS;
    }

  }/** end namespace dataserver**/
}/** end namespace tfs **/

