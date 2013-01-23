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
 *
 */
#include <Memory.hpp>
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/client_manager.h"
#include "block_manager.h"
#include "dataservice.h"
#include "data_manager.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace message;
    using namespace std;

    DataManager::DataManager(DataService& service):
      service_(service)
    {
    }

    DataManager::~DataManager()
    {
    }

    inline BlockManager& DataManager::block_manager()
    {
      return service_.block_manager();
    }

    int DataManager::prepare_lease(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id,
        const LeaseType type, const VUINT64& servers)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if ((TFS_SUCCESS == ret) && (0 == (file_id & 0xFFFFFFFF)))
      {
        ret = block_manager().generation_file_id(file_id, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "create file id fail. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t now_us = Func::get_monotonic_time_us();
        int64_t now = now_us / 1000000;
        if (INVALID_LEASE_ID == lease_id)
        {
          lease_id = lease_manager_.gen_lease_id();
        }

        LeaseId lid(block_id, file_id, lease_id);
        Lease* lease = lease_manager_.get(lid, now);
        if (NULL == lease)
        {
          ret = lease_manager_.has_out_of_limit() ? EXIT_BLOCK_LEASE_OVERLOAD_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            lease_manager_.generation(lid, now, type, servers);
            lease = lease_manager_.get(lid, now);
            ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
          }
        }

        if (TFS_SUCCESS == ret)
        {
          lease->set_req_begin_time(now_us);
          lease->reset_member_status(); // reset on every reqeust
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    int DataManager::update_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        tbnet::Packet* packet)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) && (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id) || (NULL == packet)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        Lease* lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (SLAVE_DS_RESP_MESSAGE != packet->getPCode())
          {
            ret = lease->update_member_info();
          }
          else
          {
            SlaveDsRespMessage* smsg = dynamic_cast<SlaveDsRespMessage*>(packet);
            if (TFS_SUCCESS == ret)
            {
              ret = lease->update_member_info(smsg->get_server_id(),
                  smsg->get_block_info(), smsg->get_status());
            }
          }
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    bool DataManager::check_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        int32_t& status, int64_t& req_cost_time, int64_t& file_size, stringstream& err_msg)
    {
      bool all_finish = false;
      status = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == status)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now_us = Func::get_monotonic_time_us();
        int64_t now = now_us / 1000000;
        Lease* lease = lease_manager_.get(lid, now);
        status = (NULL == lease)? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == status)
        {
          all_finish = lease->check_all_finish();
          if (all_finish)
          {
            if (lease->check_has_version_conflict())
            {
              status = EXIT_VERSION_CONFLICT_ERROR;
            }

            if (TFS_SUCCESS == status)
            {
              status = lease->check_all_successful() ? TFS_SUCCESS : TFS_ERROR;
            }

            // if not all success, get error msg
            if (TFS_SUCCESS != status)
            {
              lease->dump(err_msg);
            }
            else
            {
              file_size = lease->get_file_size();
            }

            req_cost_time = now_us - lease->get_req_begin_time();
          }
          lease_manager_.put(lease);
        }
      }

      return all_finish;
    }

    int DataManager::remove_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) && (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        ret = lease_manager_.remove(lid);
      }
      return ret;
    }

    int DataManager::write_file(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        const char* buffer, const int32_t length, const int32_t offset,
        const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == block_id) ||
        (INVALID_LEASE_ID == lease_id ) || (NULL == buffer) || (offset < 0) || (length <= 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if ((TFS_SUCCESS == ret) && (remote_version >= 0))  // remote version < 0, don't check
      {
        ret = block_manager().check_block_version(local, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d, ret: %d",
              block_id, file_id, lease_id, remote_version, local.version_, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t now = Func::get_monotonic_time();
        Lease* lease = NULL;
        if (TFS_SUCCESS == ret)
        {
          LeaseId lid(block_id, file_id, lease_id);
          lease = lease_manager_.get(lid, now);
          ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
        }

        if (TFS_SUCCESS == ret)
        {
          DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
          FileInfoInDiskExt none;
          none.version_ = FILE_INFO_EXT_INIT_VERSION;
          ret = data_file.pwrite(none, buffer, length, offset);
          ret = (ret < 0) ? ret : TFS_SUCCESS; // transform return status
          lease->update_member_info(service_.get_ds_ipport(), local, ret);
          lease->update_last_time(now);
          lease_manager_.put(lease);
        }
      }

      return  ret;
    }

    int DataManager::close_file(const uint64_t block_id, const uint64_t attach_block_id,
        uint64_t& file_id, const uint64_t lease_id, const bool tmp, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      Lease* lease = NULL;
      if (TFS_SUCCESS == ret)
      {
        int64_t now = Func::get_monotonic_time();
        LeaseId lid(block_id, file_id, lease_id);
        lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
      }

      if (TFS_SUCCESS == ret)
      {
        DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
        ret = block_manager().write(file_id, data_file, block_id, attach_block_id, tmp);
        ret = (ret < 0) ? ret: TFS_SUCCESS;  // transform return status
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_block_info(local, block_id);
          lease->update_member_info(service_.get_ds_ipport(), local, ret);
        }
        lease_manager_.put(lease);
      }

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int DataManager::unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t lease_id, const int32_t action,
        const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().check_block_version(local, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d, ret: %d",
              block_id, file_id, lease_id, remote_version, local.version_, ret);
        }
      }

      int64_t file_size = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().unlink(file_size, file_id, action, block_id, attach_block_id);
        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_block_info(local, block_id);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        Lease* lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          lease->update_member_info(service_.get_ds_ipport(), local, ret);
          lease->set_file_size(file_size);
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    int DataManager::update_block_info(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        const common::UnlinkFlag unlink_flag)
    {
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      Lease* lease = NULL;
      BlockInfoV2 block_info;
      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          // get the blockinfo with highest version
          ret = lease->get_block_info(block_info);
          lease_manager_.put(lease);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        UpdateBlockInfoMessageV2 req_msg;
        req_msg.set_block_info(block_info);
        req_msg.set_unlink_flag(unlink_flag);
        req_msg.set_server_id(service_.get_ds_ipport());

        NewClient* client = NewClientManager::get_instance().create_client();
        if (NULL == client)
        {
          ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
        }
        else
        {
          tbnet::Packet* ret_msg = NULL;
          ret = send_msg_to_server(service_.get_ns_ipport(),  client, &req_msg, ret_msg);
          if (TFS_SUCCESS == ret)
          {
            ret = (STATUS_MESSAGE == ret_msg->getPCode())? TFS_SUCCESS : EXIT_COMMIT_BLOCK_UPDATE_ERROR;
            if (TFS_SUCCESS == ret)
            {
              StatusMessage* smsg = dynamic_cast<StatusMessage*>(ret_msg);
              ret = (STATUS_MESSAGE_OK == smsg->get_status()) ? TFS_SUCCESS : EXIT_COMMIT_BLOCK_UPDATE_ERROR;
              TBSYS_LOG(INFO, "update block info. blockid: %"PRI64_PREFIX"u, status: %d %s",
                  block_id, smsg->get_status(), smsg->get_error());
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
      }

      return ret;
    }

   int DataManager::resolve_block_version_conflict(const uint64_t block_id,
       const uint64_t file_id, const uint64_t lease_id)
   {
     int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
         (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

     Lease* lease = NULL;
     if (TFS_SUCCESS == ret)
     {
       LeaseId lid(block_id, file_id, lease_id);
       int64_t now = Func::get_monotonic_time();
       lease = lease_manager_.get(lid, now);
       ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
     }

     if (TFS_SUCCESS == ret)
     {
       lease->dump(TBSYS_LOG_LEVEL_INFO, "resolve block version conflict.");
       ResolveBlockVersionConflictMessage req_msg;
       int32_t member_size = 0;
       req_msg.set_block(block_id);
       ret = lease->get_member_info(req_msg.get_members(), member_size);
       lease_manager_.put(lease);
       if (TFS_SUCCESS == ret)
       {
         req_msg.set_size(member_size);
       }

       NewClient* client = NULL;
       if (TFS_SUCCESS == ret)
       {
         NewClient* client = NewClientManager::get_instance().create_client();
         if (NULL == client)
         {
           ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
         }
       }

       if (TFS_SUCCESS == ret)
       {
         tbnet::Packet* ret_msg = NULL;
         ret = send_msg_to_server(service_.get_ns_ipport(), client, &req_msg, ret_msg);
         if (TFS_SUCCESS == ret)
         {
           ret = (RSP_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE == ret_msg->getPCode())
             ? TFS_SUCCESS : EXIT_RESOLVE_BLOCK_VERSION_CONFLICT_ERROR;
           if (TFS_SUCCESS == ret)
           {
             ResolveBlockVersionConflictResponseMessage* msg =
               dynamic_cast<ResolveBlockVersionConflictResponseMessage*>(ret_msg);
             ret = (TFS_SUCCESS == msg->get_status()) ? TFS_SUCCESS : EXIT_RESOLVE_BLOCK_VERSION_CONFLICT_ERROR;
           }
         }
         NewClientManager::get_instance().destroy_client(client);
       }

     }
     return ret;

    }

    int DataManager::timeout(const time_t now)
    {
      return lease_manager_.timeout(now);
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
