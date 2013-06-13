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

    inline BlockManager& DataManager::get_block_manager()
    {
      return service_.get_block_manager();
    }

    int DataManager::prepare_lease(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id,
        const LeaseType type, const VUINT64& servers, const bool alloc)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if ((TFS_SUCCESS == ret) && (0 == (file_id & 0xFFFFFFFF)))
      {
        // should keep the high-32 bit of file_id unchanged
        uint64_t alloc_file_id = 0;
        ret = get_block_manager().generation_file_id(alloc_file_id, block_id);
        if (TFS_SUCCESS == ret)
        {
          file_id |= (alloc_file_id & 0xFFFFFFFF);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int64_t now_us = Func::get_monotonic_time_us();
        if ((INVALID_LEASE_ID == lease_id) && alloc)
        {
          lease_id = lease_manager_.gen_lease_id();
        }

        LeaseId lid(block_id, file_id, lease_id);
        Lease* lease = lease_manager_.get(lid, now_us);
        if ((NULL == lease) && alloc)
        {
          ret = lease_manager_.has_out_of_limit() ? EXIT_BLOCK_LEASE_OVERLOAD_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            lease_manager_.generation(lid, now_us, type, servers);
            lease = lease_manager_.get(lid, now_us);
          }
        }

        ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          lease->reset_member_info(now_us); // reset on every reqeust
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
        int64_t now_us = Func::get_monotonic_time_us();
        Lease* lease = lease_manager_.get(lid, now_us);
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

    int DataManager::update_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        const int32_t error_code, const BlockInfoV2& info)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) && (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now_us = Func::get_monotonic_time_us();
        Lease* lease = lease_manager_.get(lid, now_us);
        ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
          ret = lease->update_member_info(ds_info.information_.id_, info, error_code);
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
        Lease* lease = lease_manager_.get(lid, now_us);
        status = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == status)
        {
          all_finish = lease->check_all_finish();
          if (all_finish)
          {
            if (lease->check_has_version_conflict())
            {
              status = EXIT_BLOCK_VERSION_CONFLICT_ERROR;
            }

            if (TFS_SUCCESS == status)
            {
              status = lease->check_all_successful();
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

            req_cost_time  = lease->get_req_cost_time_us();
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

    int DataManager::write_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const uint64_t lease_id,
        const char* buffer, const int32_t length, const int32_t offset,
        const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id ) ||
          (NULL == buffer) || (offset < 0) || (length <= 0))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      if ((TFS_SUCCESS == ret) && (remote_version >= 0))
      {
        ret = get_block_manager().check_block_version(local, remote_version, block_id, attach_block_id);
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
        int64_t now_us = Func::get_monotonic_time_us();
        Lease* lease = NULL;
        if (TFS_SUCCESS == ret)
        {
          LeaseId lid(attach_block_id, file_id, lease_id);
          lease = lease_manager_.get(lid, now_us);
          ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        }

        if (TFS_SUCCESS == ret)
        {
          DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
          FileInfoInDiskExt none;
          none.version_ = FILE_INFO_EXT_INIT_VERSION;
          ret = data_file.pwrite(none, buffer, length, offset);
          ret = (ret < 0) ? ret : TFS_SUCCESS; // transform return status
          lease->update_last_time_us(now_us);
          lease_manager_.put(lease);
        }
      }

      return  ret;
    }

    int DataManager::close_file(const uint64_t block_id, const uint64_t attach_block_id,
        uint64_t& file_id, const uint64_t lease_id, const uint32_t crc, const int32_t status, const bool tmp, BlockInfoV2& local)
    {
      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      Lease* lease = NULL;
      if (TFS_SUCCESS == ret)
      {
        int64_t now_us = Func::get_monotonic_time_us();
        LeaseId lid(attach_block_id, file_id, lease_id);
        lease = lease_manager_.get(lid, now_us);
        ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
      }

      if ((TFS_SUCCESS == ret) && !tmp) // write tmp block won't check crc
      {
        DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
        if (crc != data_file.crc())
        {
          TBSYS_LOG(WARN, "check crc fail. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, crc: %u:%u",
              block_id, attach_block_id, file_id, lease_id, crc, data_file.crc());
          ret = EXIT_CHECK_CRC_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
        data_file.set_status(status);  // status will directly set to file
        ret = get_block_manager().write(file_id, data_file, block_id, attach_block_id, tmp);
        ret = (ret < 0) ? ret: TFS_SUCCESS;  // transform return status
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_block_info(local, block_id, tmp);
        }
        lease->set_file_size(data_file.length());
      }
      lease_manager_.put(lease);

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int DataManager::prepare_unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t lease_id, const int32_t action,
        const int32_t remote_version, BlockInfoV2& local)
    {
      UNUSED(action);
      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      if ((TFS_SUCCESS == ret) && (remote_version >= 0))
      {
        ret = get_block_manager().check_block_version(local, remote_version, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "unlink check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d, ret: %d",
              block_id, file_id, lease_id, remote_version, local.version_, ret);
        }
      }

      return ret;
    }

    int DataManager::unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
        const uint64_t file_id, const int64_t lease_id, const int32_t action,
        BlockInfoV2& local)
    {
      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      int64_t file_size = 0;
      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().unlink(file_size, file_id, action, block_id, attach_block_id);
        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_block_info(local, block_id);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(attach_block_id, file_id, lease_id);
        int64_t now_us = Func::get_monotonic_time_us();
        Lease* lease = lease_manager_.get(lid, now_us);
        ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          lease->set_file_size(file_size);
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    int DataManager::update_block_info(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        const common::UpdateBlockInfoType type)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
         (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      Lease* lease = NULL;
      BlockInfoV2 block_info;
      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now_us = Func::get_monotonic_time_us();
        lease = lease_manager_.get(lid, now_us);
        ret = (NULL == lease) ? EXIT_BLOCK_LEASE_INTERNAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          // get the blockinfo with highest version
          // commit blockinfo to ns only if at least one ds success
          if (lease->get_highest_version_block(block_info))
          {
            ret = update_block_info(block_info, type);
          }
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    int DataManager::update_block_info(const BlockInfoV2& block_info, const common::UpdateBlockInfoType type)
    {
      int ret = TFS_SUCCESS;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      UpdateBlockInfoMessageV2 req_msg;
      req_msg.set_block_info(block_info);
      req_msg.set_type(type);
      req_msg.set_server_id(ds_info.information_.id_);

      NewClient* client = NewClientManager::get_instance().create_client();
      if (NULL == client)
      {
        ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
      }
      else
      {
        tbnet::Packet* ret_msg = NULL;
        ret = send_msg_to_server(ds_info.ns_vip_port_, client, &req_msg, ret_msg);
        if (TFS_SUCCESS == ret)
        {
          ret = (STATUS_MESSAGE == ret_msg->getPCode())? TFS_SUCCESS : EXIT_COMMIT_BLOCK_UPDATE_ERROR;
          if (TFS_SUCCESS == ret)
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(ret_msg);
            ret = (STATUS_MESSAGE_OK == smsg->get_status()) ? TFS_SUCCESS : EXIT_COMMIT_BLOCK_UPDATE_ERROR;
            TBSYS_LOG(DEBUG, "update block info. blockid: %"PRI64_PREFIX"u, status: %d %s",
                block_info.block_id_, smsg->get_status(), smsg->get_error());
          }
        }
        NewClientManager::get_instance().destroy_client(client);
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
       int64_t now_us = Func::get_monotonic_time_us();
       lease = lease_manager_.get(lid, now_us);
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
         DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
         tbnet::Packet* ret_msg = NULL;
         ret = send_msg_to_server(ds_info.ns_vip_port_, client, &req_msg, ret_msg);
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

    int DataManager::timeout(const time_t now_us)
    {
      return lease_manager_.timeout(now_us);
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
