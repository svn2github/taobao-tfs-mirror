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
      memset(ns_ip_port_, 0, sizeof(ns_ip_port_));
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

      master_index_ = -1;
      need_renew_block_ = false;
      apply_block_thread_ = 0;
    }

    LeaseManager::~LeaseManager()
    {
    }

    int LeaseManager::initialize()
    {
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      ds_info.startup();
      DataServerStatInfo& info = ds_info.information_;
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
      return master_index_ >= 0 && !is_expired(now, master_index_);
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

    int LeaseManager::apply(const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsApplyLeaseMessage, req_msg);
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

    bool LeaseManager::update_global_config(const int32_t who, const LeaseMeta& lease_meta)
    {
      bool ns_switch = false;
      lease_meta_[who] = lease_meta;
      last_renew_time_[who] = Func::get_monotonic_time();
      if (NS_ROLE_MASTER == lease_meta_[who].ns_role_)
      {
        DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
        // check ns switch
        if (INVALID_SERVER_ID != ds_info.master_ns_ip_port_ &&
            ds_info.master_ns_ip_port_ != ns_ip_port_[who])
        {
          ns_switch = true;
          TBSYS_LOG(INFO, "ns switch, old_master: %s, new: master: %s, expire all blocks",
              tbsys::CNetUtil::addrToString(ds_info.master_ns_ip_port_).c_str(),
              tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str());
        }
        ds_info.master_ns_ip_port_ = ns_ip_port_[who];
        ds_info.max_mr_network_bandwidth_mb_ = lease_meta_[who].max_mr_network_bandwith_;
        ds_info.max_rw_network_bandwidth_mb_ = lease_meta_[who].max_rw_network_bandwith_;
        ds_info.max_block_size_ = lease_meta_[who].max_block_size_;
        ds_info.max_write_file_count_ = lease_meta_[who].max_write_file_count_;
        ds_info.enable_version_check_ = lease_meta_[who].enable_version_check_;
        ds_info.check_integrity_interval_days_ = lease_meta_[who].check_integrity_interval_days_;
        master_index_ = who;  // record master index for fast access
      }
      return ns_switch;
    }

    void LeaseManager::process_apply_response(DsApplyLeaseResponseMessage* response, const int32_t who)
    {
      assert(NULL != response);
      bool ns_switch = update_global_config(who, response->get_lease_meta());
      if (ns_switch)
      {
        get_writable_block_manager().expire_all_blocks();
      }
    }

    int LeaseManager::renew(const int32_t timeout_ms, const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsRenewLeaseMessage, req_msg);
      req_msg.set_ds_stat(ds_info.information_);

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
      bool ns_switch = update_global_config(who, response->get_lease_meta());
      if (ns_switch)
      {
        get_writable_block_manager().expire_all_blocks();
      }
    }

    int LeaseManager::giveup(const int32_t who)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      create_msg_ref(DsGiveupLeaseMessage, req_msg);
      req_msg.set_ds_stat(ds_info.information_);

      // ds will exit, aync giveup all blocks
      get_writable_block_manager().expire_all_blocks();
      get_writable_block_manager().giveup_writable_block();

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
      manager_.run_writable_blocks();
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
          TIMER_START();
          update_stat(who);
          ret = apply(who);
          if (TFS_SUCCESS == ret)
          {
            lease_status_[who] = LEASE_RENEW;
            if (is_master(who))
            {
              get_writable_block_manager().expire_all_blocks();
            }
          }
          TIMER_END();
          TBSYS_LOG(INFO, "apply lease from %s, who: %d, cost: %"PRI64_PREFIX"d, ret: %d",
              tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, TIMER_DURATION(), ret);
        }

        if ((lease_status_[who] == LEASE_RENEW)
            && need_renew(Func::get_monotonic_time(), who))
        {
          TIMER_START();
          update_stat(who);
          // retry renew lease, if renew fail, switch to APPLY state
          for (int i = 0; i < lease_meta_[who].renew_retry_times_; i++)
          {
            ret = renew(lease_meta_[who].renew_retry_timeout_ * 1000, who);
            if (TFS_SUCCESS == ret
              || EIXT_SERVER_OBJECT_NOT_FOUND == ret
              || EXIT_LEASE_EXPIRED == ret)
            {
              break;
            }
            else if (EXIT_TIMEOUT_ERROR != ret)
            {
              usleep(SLEEP_TIME_US);
            }
          }

          if (master_index_ == who)
          {
            if (TFS_SUCCESS == ret)
            {
              need_renew_block_ = true;
            }
            else
            {
              need_renew_block_ = false;
              get_writable_block_manager().expire_all_blocks();
            }
          }

          if (TFS_SUCCESS == ret)
          {
            if (is_master(who))
            {
              get_writable_block_manager().expire_update_blocks();
            }
          }
          else
          {
            lease_status_[who] = LEASE_APPLY;
          }
          TIMER_END();
          TBSYS_LOG(INFO, "renew lease from %s, who: %d, cost: %"PRI64_PREFIX"d, ret: %d",
              tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, TIMER_DURATION(), ret);
        }

        usleep(SLEEP_TIME_US);
      }

      TIMER_START();
      ret = giveup(who);
      TIMER_END();
      TBSYS_LOG(INFO, "giveup lease from %s, who: %d, cost: %"PRI64_PREFIX"d, ret: %d",
          tbsys::CNetUtil::addrToString(ns_ip_port_[who]).c_str(), who, TIMER_DURATION(), ret);
    }

    void LeaseManager::run_writable_blocks()
    {
      const int32_t SLEEP_TIME_US = 1 * 1000 * 1000;
      const int32_t APPLY_INTERVAL_S = 3;
      const int32_t GIVEUP_INTERVAL_S = 5;
      const int32_t DUMP_INTERVAL_S = 5;
      int64_t last_apply_time = 0;
      int64_t last_giveup_time = 0;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        int64_t now = Func::get_monotonic_time();
        int32_t writable = 0;
        int32_t update = 0;
        int32_t expired = 0;

        // traverse writable list, TODO: optimize
        get_writable_block_manager().size(writable, update, expired);

        // dump writable block info
        if (now % DUMP_INTERVAL_S == 0)
        {
          TBSYS_LOG(INFO, "writable block info, writable: %d, update: %d, expired: %d",
              writable, update, expired);
        }

        if (has_valid_lease(now))
        {
          // giveup expired block
          if (expired > 0 && now > last_giveup_time + GIVEUP_INTERVAL_S)
          {
            get_writable_block_manager().giveup_writable_block();
            last_giveup_time = now;
          }

          // apply writable block, system disk won't do apply
          if (ds_info.information_.type_ == DATASERVER_DISK_TYPE_FULL)
          {
            int32_t need = ds_info.max_write_file_count_ - writable;
            if (need > 0 && now > last_apply_time + APPLY_INTERVAL_S)
            {
              get_writable_block_manager().apply_writable_block(need);
              last_apply_time = now;
            }
          }

          // renew writable block
          if (writable > 0 && need_renew_block_)
          {
            get_writable_block_manager().renew_writable_block();
            need_renew_block_ = false;
          }
        }
        else if (INVALID_SERVER_ID != ds_info.master_ns_ip_port_)
        {
          TBSYS_LOG(INFO, "lease expired, expire all blocks");
          get_writable_block_manager().expire_all_blocks();
        }

        usleep(SLEEP_TIME_US);
      }
    }

  }/** end namespace dataserver**/
}/** end namespace tfs **/

