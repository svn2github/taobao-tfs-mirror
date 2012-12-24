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
        const LeaseType type, const VUINT64 servers)
    {
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if ((TFS_SUCCESS == ret) && (0 == (file_id & 0xFFFFFFFF)))
      {
        // TODO: add config item
        double threshold = 0.8;
        ret = block_manager().generation_file_id(file_id, threshold, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create file id fail. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id, ret);
        }

        if ((TFS_SUCCESS == ret) && (INVALID_LEASE_ID == lease_id))
        {
          lease_id = lease_manager_.gen_lease_id();
          LeaseId lid(block_id, file_id, lease_id);
          int64_t now = Func::get_monotonic_time();
          Lease* lease = lease_manager_.get(lid, now);
          if (NULL == lease)
          {
            //control lease size
            ret = lease_manager_.has_out_of_limit() ? EXIT_DATAFILE_OVERLOAD : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              lease_manager_.generation(lid, now, type, servers);
            }
          }
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
        if (SLAVE_DS_RESP_MESSAGE != packet->getPCode())
        {
          ret = EXIT_UNKNOWN_MSGTYPE;
        }
        else
        {
          SlaveDsRespMessage* smsg = dynamic_cast<SlaveDsRespMessage*>(packet);
          LeaseId lid(block_id, file_id, lease_id);
          int64_t now = Func::get_monotonic_time();
          Lease* lease = lease_manager_.get(lid, now);
          ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ret = lease->update_member_info(smsg->get_server_id(),
               smsg->get_block_info(), smsg->get_status());
            lease_manager_.put(lease);
          }
        }
      }

      return ret;
    }

    int DataManager::check_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) && (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        Lease* lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (lease->check_has_version_conflict())
          {
            ret = EXIT_VERSION_CONFLICT_ERROR;
          }

          if (TFS_SUCCESS == ret)
          {
            ret = lease->check_all_successful() ? TFS_SUCCESS: TFS_ERROR;
          }

          lease_manager_.put(lease);
        }
      }

      return ret;
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

    int DataManager::write_data(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
        const char* buffer, const int32_t length, const int32_t offset,
        const int32_t remote_version, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == block_id) ||
        (INVALID_LEASE_ID == lease_id ) || (NULL == buffer) || (offset < 0) || (length <= 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      ret = block_manager().check_block_version(local, remote_version, block_id);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "write check block version conflict. blockid: %"PRI64_PREFIX"u, "
            "remote version: %d, local version: %d",
            block_id, remote_version, local.version_);
      }
      else
      {
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
          DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
          FileInfoInDiskExt none;
          none.version_ = FILE_INFO_EXT_INIT_VERSION;
          ret = data_file.pwrite(none, buffer, length, offset);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "write datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u",
                block_id, file_id);
          }
          lease->update_member_info(service_.get_ds_ipport(), local, ret);
          lease_manager_.put(lease);
        }
      }

      return  ret;
    }

    int DataManager::close_file(const uint64_t block_id, uint64_t& file_id, const uint64_t lease_id)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      Lease* lease = NULL;
      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        Lease* lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
      }

      if (TFS_SUCCESS == ret)
      {
        DataFile& data_file = dynamic_cast<WriteLease* >(lease)->get_data_file();
        ret = block_manager().write(file_id, data_file, block_id, block_id);
        if (ret < 0)
        {
          TBSYS_LOG(ERROR, "close file fail. blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, ret);
        }
      }

      return (ret < 0) ? ret: TFS_SUCCESS;
    }

    int DataManager::unlink_file(const uint64_t block_id, const uint64_t file_id, const int64_t lease_id,
        const int32_t action, const int32_t remote_version, int64_t& size, BlockInfoV2& local)
    {
      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().check_block_version(local, remote_version, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "unlink check block version conflict. blockid: %"PRI64_PREFIX"u, "
              "remote version: %d, local version: %d, ret: %d",
              block_id, remote_version, local.version_, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().unlink(size, file_id, action, block_id, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "close file fail. blockid: %"PRI64_PREFIX"u fileid: %"PRI64_PREFIX"u, ret:%d",
              block_id, file_id, ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        LeaseId lid(block_id, file_id, lease_id);
        int64_t now = Func::get_monotonic_time();
        Lease* lease = lease_manager_.get(lid, now);
        ret = (NULL == lease)? EXIT_DATA_FILE_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          lease->update_member_info(service_.get_ds_ipport(), local, ret);
          lease_manager_.put(lease);
        }
      }

      return ret;
    }

    int DataManager::update_block_info(const uint64_t block_id, const common::UnlinkFlag unlink_flag)
    {
     int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
     BlockInfoV2 block_info;
     if (TFS_SUCCESS == ret)
     {
       ret = block_manager().get_block_info(block_info, block_id);
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
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

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
        lease->dump(TBSYS_LOG_LEVEL_INFO, "resolve block version conflict, information: ");
        ResolveBlockVersionConflictMessage req_msg;
        int32_t member_size = 0;
        req_msg.set_block(block_id);
        ret = lease->get_member_info(req_msg.get_members(), member_size);
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

        lease_manager_.put(lease);
      }
      return ret;

    }

  }
}
