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
    LeaseManager::LeaseManager(DataService& service):
      service_(service),
      lease_status_(LEASE_APPLY),
      last_renew_time_(0)
    {
    }

    LeaseManager::~LeaseManager()
    {
    }

    WritableBlockManager& LeaseManager::get_writable_block_manager()
    {
      return service_.get_writable_block_manager();
    }

    int LeaseManager::apply()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsApplyLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (DS_APPLY_LEASE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            DsApplyLeaseResponseMessage* resp_msg = dynamic_cast<DsApplyLeaseResponseMessage* >(ret_msg);
            process_apply_response(resp_msg);
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
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      return ret;
    }

    void LeaseManager::process_apply_response(DsApplyLeaseResponseMessage* response)
    {
      assert(NULL != response);
      lease_meta_ = response->get_lease_meta();  // update lease info
      last_renew_time_ = Func::get_monotonic_time();
    }

    int LeaseManager::renew()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsRenewLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);
      BlockInfoV2* block_infos = req_msg.get_block_infos();
      ArrayHelper<BlockInfoV2> blocks(MAX_WRITABLE_BLOCK_COUNT, block_infos);

      // TODO get blocks from writable block manager
      req_msg.set_size(blocks.get_array_index());

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (TFS_SUCCESS == ret)
      {
        ret = send_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          if (DS_RENEW_LEASE_RESPONSE_MESSAGE == ret_msg->getPCode())
          {
            DsRenewLeaseResponseMessage* resp_msg = dynamic_cast<DsRenewLeaseResponseMessage* >(ret_msg);
            process_renew_response(resp_msg);
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
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      return ret;
    }

    void LeaseManager::process_renew_response(DsRenewLeaseResponseMessage* response)
    {
      assert(NULL != response);
      lease_meta_ = response->get_lease_meta();  // update lease info
      last_renew_time_ = Func::get_monotonic_time();
      ArrayHelper<BlockLease> leases(response->get_size(),
          response->get_block_lease(), response->get_size());
      for (int index = 0; index < response->get_size(); index++)
      {
        BlockLease& lease = *leases.at(index);
        if (TFS_SUCCESS == lease.result_)
        {
          WritableBlock* block = get_writable_block_manager().get(lease.block_id_, BLOCK_WRITABLE);
          if (NULL != block)
          {
            // update replica information
            ArrayHelper<uint64_t> helper(lease.size_, lease.servers_, lease.size_);
            block->set_servers(helper);
          }
        }
        else  // move to expired list
        {
          get_writable_block_manager().remove(lease.block_id_, BLOCK_WRITABLE);
        }
      }
    }

    int LeaseManager::giveup()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      DsGiveupLeaseMessage req_msg;
      req_msg.set_ds_stat(ds_info.information_);

      tbnet::Packet* ret_msg = NULL;
      NewClient* new_client = NewClientManager::get_instance().create_client();
      if (TFS_SUCCESS == ret)
      {
        // no need to process return message
        ret = send_msg_to_server(ds_info.ns_vip_port_, new_client, &req_msg, ret_msg);
        NewClientManager::get_instance().destroy_client(new_client);
      }
      else
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }

      return ret;
    }

    void LeaseManager::run_lease()
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      while (!ds_info.is_destroyed())
      {
        if (LEASE_APPLY == lease_status_)
        {
          ret = apply();
          if (TFS_SUCCESS == ret)
          {
            lease_status_ = LEASE_RENEW;
          }
          TBSYS_LOG(INFO, "apply lease result: %d", ret);
        }

        if ((LEASE_RENEW == lease_status_)
            && need_renew(Func::get_monotonic_time()))
        {
          ret = renew();
          TBSYS_LOG(INFO, "renew lease result: %d", ret);
        }

        usleep(100000); // TODO
      }

      ret = giveup();
      TBSYS_LOG(INFO, "giveup lease result: %d", ret);
    }

    int LeaseManager::timeout(const time_t now)
    {
      int ret = TFS_SUCCESS;
      if (is_expired(now))
      {
        ret = get_writable_block_manager().expire_all_blocks();
      }
      return ret;
    }

  }/** end namespace dataserver**/
}/** end namespace tfs **/

