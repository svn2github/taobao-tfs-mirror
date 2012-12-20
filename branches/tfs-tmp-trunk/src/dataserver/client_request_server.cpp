/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
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

#include "dataservice.h"
#include "client_request_server.h"

namespace tfs
{
  namespace dataserver
  {
    using namespace std;
    using namespace common;
    using namespace message;

    int ClientRequestServer::handle(tbnet::Packet* packet)
    {
      int ret = (NULL == packet) ? EXIT_POINTER_NULL: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        switch (pcode)
        {
          case STAT_FILE_MESSAGE_V2:
            ret = stat_file(dynamic_cast<StatFileMessageV2*>(packet));
            break;
          case READ_FILE_MESSAGE_V2:
            ret = read_file(dynamic_cast<ReadFileMessageV2*>(packet));
            break;
          case WRITE_FILE_MESSAGE_V2:
            ret = write_file(dynamic_cast<WriteFileMessageV2*>(packet));
            break;
          case CLOSE_FILE_MESSAGE_V2:
            ret = close_file(dynamic_cast<CloseFileMessageV2*>(packet));
            break;
          case UNLINK_FILE_MESSAGE_V2:
            ret = unlink_file(dynamic_cast<UnlinkFileMessageV2*>(packet));
            break;
          case NEW_BLOCK_MESSAGE_V2:
            ret = new_block(dynamic_cast<NewBlockMessageV2*>(packet));
            break;
          case REMOVE_BLOCK_MESSAGE_V2:
            ret = remove_block(dynamic_cast<RemoveBlockMessageV2*>(packet));
            break;
          default:
            TBSYS_LOG(ERROR, "process packet pcode: %d\n", pcode);
            ret = EXIT_UNKNOWN_MSGTYPE;
            break;
        }
      }

      return ret;
    }

    int ClientRequestServer::stat_file(StatFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t flag = message->get_flag();
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        FileInfoV2 file_info;
        ret = DataManager::instance().stat_file(block_id, file_id, flag, file_info);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "read file info fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, flag: %d, ret: %d",
              block_id, file_id, flag, ret);
        }
        else
        {
          StatFileRespMessageV2* resp_msg = new (std::nothrow) StatFileRespMessageV2();
          assert(NULL != resp_msg);
          resp_msg->set_file_info(file_info);
          message->reply(resp_msg);
        }
      }

      TIMER_END();

      // stat log
      TBSYS_LOG(INFO, "read fileinfo %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
          "peer ip: %"PRI64_PREFIX"u, cost: %"PRI64_PREFIX"d",
          (TFS_SUCCESS == ret) ? "success" : "fail",
          block_id, file_id, tbsys::CNetUtil::addrToString(peer_id).c_str(),
          TIMER_DURATION());

      return ret;
    }

    int ClientRequestServer::read_file(ReadFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t length = message->get_length();
      int32_t offset = message->get_offset();
      int8_t flag = message->get_flag();
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        FileInfoV2 file_info;
        ret = DataManager::instance().stat_file(block_id, file_id, flag, file_info);
        if (TFS_SUCCESS == ret)
        {
          // truncate read length
          if (offset + length > file_info.size_)
          {
            length = file_info.size_ - offset;
          }

          ret = (length <= 0) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ReadFileRespMessageV2* resp_msg = new (std::nothrow) ReadFileRespMessageV2();
            assert(NULL != message);
            char* buffer = resp_msg->alloc_data(length);
            assert(NULL != buffer);
            ret = DataManager::instance().read_file(block_id, file_id, buffer, offset, length, flag);
            if (TFS_SUCCESS != ret)
            {
              // upper layer will reply error packet to client
              tbsys::gDelete(resp_msg);
            }
            else
            {
              // every call of read will return file info
              // TODO: add a flag to denotes if fileinfo is needed
              resp_msg->set_file_info(file_info);
              message->reply(resp_msg);
            }
          }
        }
      }

      TIMER_END();

      // read log
      TBSYS_LOG(INFO, "read %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
          "length: %d, offset: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "success" : "fail",
          block_id, file_id, length, offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());

      return ret;
    }

    int ClientRequestServer::write_file(WriteFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t offset = message->get_offset();
      int32_t length = message->get_length();
      int32_t version = message->get_version();
      const VUINT64& servers = message->get_ds();
      const char* data = message->get_data();
      bool is_master = message->is_master();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t now = Func::get_monotonic_time();

      int ret = ((NULL == data) || (INVALID_BLOCK_ID == block_id) ||
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      // first write, create file id & lease id
      if (TFS_SUCCESS == ret)
      {
        ret = create_file_id_(block_id, file_id, lease_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create file id fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      // check lease
      LeaseId lid(lease_id, file_id, block_id);
      Lease* lease = NULL;
      if (TFS_SUCCESS == ret)
      {
        lease = lease_manager_.get(lid, now, LEASE_TYPE_WRITE, servers);
        ret = (NULL == lease)? EXIT_DATAFILE_OVERLOAD: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "generate lease fail. blockid: %"PRI64_PREFIX"u, "
              "lease nums is large than default: %d.",
              block_id, SYSPARAM_DATASERVER.max_datafile_nums_);
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && is_master)
      {
        message->set_slave();
        message->set_file_id(file_id);
        message->set_lease_id(lease_id);
        DataService* service = dynamic_cast<DataService*>(DataService::instance());
        int32_t result = service->post_message_to_server(message, servers);
        ret = (result == EXIT_POST_MSG_RET_POST_MSG_ERROR) ? EXIT_SENDMSG_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write data to slave fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, version: %d, ret: %d",
              block_id, file_id, lease_id, version, ret);
        }
      }

      // local write file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = DataManager::instance().write_data(dynamic_cast<WriteLease*>(lease),
            data, offset, length, version, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write data to datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, length: %d, offset: %d, version: %u, role: %s",
              block_id, file_id, lease_id, length, offset, version,
              is_master? "master": "slave");
        }
        lease->update_member_info(ds_ip_port_, local, ret);
        lease_manager_.put(lease);
      }

      // slave response to master
      if (!is_master)
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(ds_ip_port_);
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);
      }

      // only one server, master will reply client
      if (is_master && (servers.size() <= 1U))
      {
        write_file_callback_(message, lease);
      }

      TIMER_END();

      // slave write log
      if (!is_master)
      {
        TBSYS_LOG(INFO, "write data %s, block: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, length: %d, offset: %d, role: slave, "
            "peer ip: %s, cost: %"PRI64_PREFIX"d",
            (TFS_SUCCESS == ret) ? "success": "fail",
            block_id, file_id, lease_id, length, offset,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::close_file(CloseFileMessageV2* message)
    {
      TIMER_START();
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      bool is_master = message->is_master();
      const VUINT64& servers = message->get_ds();
      uint64_t peer_id = message->get_connection()->getPeerId();
      LeaseId lid(lease_id, file_id, block_id);
      Lease* lease = NULL;
      int64_t now = Func::get_monotonic_time();

      ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id))?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      // check lease
      if (TFS_SUCCESS == ret)
      {
        lease = lease_manager_.get(lid, now);
        if (NULL == lease)
        {
          ret = EXIT_DATA_FILE_ERROR;
          TBSYS_LOG(ERROR, "close check lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, file_id, lease_id, is_master? "master": "slave", ret);
        }
      }

      // close file
      if (TFS_SUCCESS == ret)
      {
        ret = DataManager::instance().close_file(block_id, file_id,
            dynamic_cast<WriteLease*>(lease)->get_data_file());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "close file fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, file_id, is_master? "master": "slave", ret);
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && is_master)
      {
        message->set_slave();
        DataService* service = dynamic_cast<DataService*>(DataService::instance());
        int32_t result = service->post_message_to_server(message, servers);
        ret = (result == EXIT_POST_MSG_RET_POST_MSG_ERROR) ? EXIT_SENDMSG_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "close file to slave fail. block: %u, fileid: %"PRI64_PREFIX"u, "
              "leaseid: %"PRI64_PREFIX"u, role: master, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      // close file
      if (TFS_SUCCESS == ret)
      {
        ret = DataManager::instance().close_file(block_id, file_id,
            dynamic_cast<WriteLease*>(lease)->get_data_file());
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "close file fail. block: %u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, file_id, lease_id,
              is_master? "master": "slave", ret);
        }
        BlockInfoV2 none;  // have no use
        lease->update_member_info(ds_ip_port_, none, ret);
        lease->update_last_time(Func::get_monotonic_time());
        lease_manager_.put(lease);
      }

      // slave response to master
      if (!is_master)
      {
        if (TFS_SUCCESS != ret)
        {
          message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "close file failed");
        }
        else
        {
          message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
        lease_manager_.remove(lid);
      }

      // only one server, reply to client
      if (is_master && (servers.size() <= 1U))
      {
        close_file_callback_(message, lease);
        lease_manager_.remove(lid);
      }

     TIMER_END();

     // slave close log
     if (!is_master)
     {
       TBSYS_LOG(INFO, "close file %s. leaseid: %" PRI64_PREFIX "u, blockid: %u, "
           "fileid: %"PRI64_PREFIX"u, peerip: %s, role: slave, cost: %"PRI64_PREFIX"d",
           TFS_SUCCESS == ret ? "success" : "fail",
           block_id, file_id, lease_id,
           tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());
     }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::unlink_file(UnlinkFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();
      const VUINT64& servers = message->get_ds();
      int32_t version = message->get_version();
      bool is_master = message->is_master();
      int64_t file_size = 0;
      lease_id = lease_manager_.gen_lease_id();
      LeaseId lid(lease_id, file_id, block_id);
      Lease* lease = NULL;

      int ret = (INVALID_BLOCK_ID == block_id || INVALID_FILE_ID == file_id)?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        time_t now = Func::get_monotonic_time();
        lease = lease_manager_.get(lid, now, LEASE_TYPE_UNLINK, servers);
        if (NULL == lease)
        {
          ret = EXIT_CANNOT_GET_LEASE;
          TBSYS_LOG(ERROR, "unlink get lease fail. blockid: %"PRI64_PREFIX"u, \
              fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      if ((TFS_SUCCESS == ret) && is_master)
      {
        message->set_slave();
        message->set_lease_id(lease_id);
        DataService* service = dynamic_cast<DataService*>(DataService::instance());
        int32_t result = service->post_message_to_server(message, servers);
        ret = (result  == EXIT_POST_MSG_RET_POST_MSG_ERROR) ?  EXIT_SENDMSG_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "unlink file to slave fail. block: %u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = DataManager::instance().unlink_file(block_id, file_id, action, version, file_size, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "unlink file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "leaseid: %"PRI64_PREFIX"u, action: %d, role: %s, ret: %d",
            block_id, file_id, action, is_master? "master": "slave", ret);
        }
        lease->update_member_info(ds_ip_port_, local, ret);
        lease_manager_.put(lease);
      }

      // slave response to master
      if (!is_master)
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(ds_ip_port_);
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);
        lease_manager_.remove(lid);
      }

      // only one server, derectly reply client
      if (is_master && (servers.size() <= 1U))
      {
        unlink_file_callback_(message, lease);
        lease_manager_.remove(lid);
      }

      TIMER_END();

      // slave unlink log
      if (!is_master)
      {
        TBSYS_LOG(INFO, "unlink file %s. blockid: %d, fileid: %" PRI64_PREFIX "u, "
            "leaseid: %"PRI64_PREFIX"u, action: %d, role: %s, peer ip: %s, cost: %"PRI64_PREFIX"d",
            TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, lease_id, action,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::new_block(NewBlockMessageV2* message)
    {
      int ret = TFS_SUCCESS;
      const uint64_t block_id = message->get_block_id();
      ret = DataManager::instance().new_block(block_id, false);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "new block %"PRI64_PREFIX"u fail, ret: %d", block_id, ret);
      }

      if (TFS_SUCCESS == ret);
      {
        message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    int ClientRequestServer::remove_block(RemoveBlockMessageV2* message)
    {
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      ret = DataManager::instance().remove_block(block_id, false);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "remove block %"PRI64_PREFIX"u fail, ret: %d", block_id, ret);
      }
      else
      {
        message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    int ClientRequestServer::create_file_id_(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id)
    {
      int ret = TFS_SUCCESS;
      // in new version, we won't support reset seq id
      if (0 == (file_id & 0xFFFFFFFF))  // high 32-bits is suffix hash value
      {
        ret = DataManager::instance().create_file_id(block_id, file_id);
      }

      if ((TFS_SUCCESS == ret) && (0 == lease_id))
      {
        lease_id = lease_manager_.gen_lease_id();
      }

      return ret;
    }

    int ClientRequestServer::callback(NewClient* client)
    {
      int ret = (NULL == client)? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      NewClient::RESPONSE_MSG_MAP* sresponse = NULL;
      NewClient::RESPONSE_MSG_MAP* fresponse = NULL;
      tbnet::Packet* source = NULL;
      int32_t pcode = 0;
      if (TFS_SUCCESS == ret)
      {
        sresponse = client->get_success_response();
        fresponse = client->get_fail_response();
        if ((NULL == sresponse) || (NULL == fresponse))
        {
          ret = EXIT_POINTER_NULL;
        }
        else
        {
          source = client->get_source_msg();
          ret = (NULL == source) ? EXIT_POINTER_NULL: TFS_SUCCESS;
          if( TFS_SUCCESS == ret)
          {
            pcode = source->getPCode();
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        if (WRITE_FILE_MESSAGE_V2 == pcode)
        {
          WriteFileMessageV2* msg = dynamic_cast<WriteFileMessageV2*> (source);
          uint64_t block_id = msg->get_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          LeaseId lid(lease_id, file_id, block_id);
          int64_t now = Func::get_monotonic_time();
          Lease* lease = lease_manager_.get(lid, now);
          if (NULL != lease)
          {
            NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
            for ( ; iter != sresponse->end(); iter++)
            {
              check_write_response_(iter->second.second);
            }
            write_file_callback_(msg, lease);
            lease_manager_.put(lease);
            lease_manager_.remove(lid);
          }
        }
        else if (CLOSE_FILE_MESSAGE_V2 == pcode)
        {
          CloseFileMessageV2* msg = dynamic_cast<CloseFileMessageV2*> (source);
          uint64_t block_id = msg->get_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          LeaseId lid(lease_id, file_id, block_id);
          int64_t now = Func::get_monotonic_time();
          Lease* lease = lease_manager_.get(lid, now);
          if (NULL != lease)
          {
            NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
            for ( ; iter != sresponse->end(); iter++)
            {
              check_write_response_(iter->second.second);
            }
            close_file_callback_(msg, lease);
            lease_manager_.put(lease);
            lease_manager_.remove(lid);
          }
        }
        else if (UNLINK_FILE_MESSAGE_V2 == pcode)
        {
          UnlinkFileMessageV2* msg = dynamic_cast<UnlinkFileMessageV2*> (source);
          uint64_t block_id = msg->get_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          LeaseId lid(lease_id, file_id, block_id);
          int64_t now = Func::get_monotonic_time();
          Lease* lease = lease_manager_.get(lid, now);
          if (NULL != lease)
          {
            NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
            for ( ; iter != sresponse->end(); iter++)
            {
              check_write_response_(iter->second.second);
            }
            unlink_file_callback_(msg, lease);
            lease_manager_.put(lease);
            lease_manager_.remove(lid);
          }
        }
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::check_write_response_(tbnet::Packet* msg)
    {
      int ret = (NULL == msg) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        if (SLAVE_DS_RESP_MESSAGE != msg->getPCode())
        {
          ret = EXIT_RECVMSG_ERROR;
        }
        else
        {
          SlaveDsRespMessage* smsg = dynamic_cast<SlaveDsRespMessage*>(msg);
          ret = smsg->get_status();
        }
     }

      return ret;
    }

    int ClientRequestServer::check_close_response_(tbnet::Packet* msg)
    {
      int ret = (NULL == msg) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        if (STATUS_MESSAGE != msg->getPCode())
        {
          ret = EXIT_RECVMSG_ERROR;
        }
        else
        {
          StatusMessage* smsg = dynamic_cast<StatusMessage*>(msg);
          ret = smsg->get_status();
        }
      }

      return ret;
    }

    int ClientRequestServer::check_unlink_response(tbnet::Packet* msg)
    {
      return check_write_response_(msg);
    }

    int ClientRequestServer::write_file_callback_(WriteFileMessageV2* message, Lease* lease)
    {
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint32_t length = message->get_length();
      uint32_t offset = message->get_offset();
      uint64_t peer_id = message->get_connection()->getPeerId();

      if (lease->check_all_successful())
      {
        WriteFileRespMessageV2* resp_msg = new (std::nothrow) WriteFileRespMessageV2();
        assert(NULL != resp_msg);
        resp_msg->set_file_id(file_id);
        resp_msg->set_lease_id(lease_id);
        message->reply(resp_msg);
      }
      else
      {
        // req ns resolve version conflict
        if (lease->check_has_version_conflict())
        {
          ret = resolve_block_version_conflict_(block_id, *lease);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "resolve block version conflict fail. "
                "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
                block_id, file_id, lease_id, ret);
          }
        }
        message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "write file failed");
      }

      // master write log
      TBSYS_LOG(INFO, "write data %s, block: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
          "leaseid: %"PRI64_PREFIX"u, length: %d, offset: %d, role: master, peer ip: %s",
          (TFS_SUCCESS == ret) ? "success": "fail",
          block_id, file_id, lease_id, length, offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str());

      return ret;
    }

    int ClientRequestServer::close_file_callback_(CloseFileMessageV2* message, Lease* lease)
    {
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint64_t peer_id = message->get_connection()->getPeerId();

      if (lease->check_all_successful())
      {
        ret = update_block_info_(block_id, UNLINK_FLAG_NO);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "update block info fail. "
              "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
        else
        {
          message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      else
      {
        message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "write file failed");
      }

      // master close log
      TBSYS_LOG(INFO, "close file %s. leaseid: %" PRI64_PREFIX "u, blockid: %u, "
          "fileid: %"PRI64_PREFIX"u, peerip: %s, role: master",
          TFS_SUCCESS == ret ? "success" : "fail",
          block_id, file_id, lease_id,
          tbsys::CNetUtil::addrToString(peer_id).c_str());

      return ret;
    }

    int ClientRequestServer::unlink_file_callback_(UnlinkFileMessageV2* message, Lease* lease)
    {
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();

      if (lease->check_all_successful())
      {
        ret = update_block_info_(block_id, UNLINK_FLAG_YES);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "update block info fail. "
              "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
        else
        {
          message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      else
      {
        message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "unlink file failed");
      }

      // master unlink log
      TBSYS_LOG(INFO, "unlink file %s. blockid: %d, fileid: %" PRI64_PREFIX "u, "
          "action: %d, role: master, peer ip: %s",
          TFS_SUCCESS == ret ? "success" : "fail",
          block_id, file_id, action,
          tbsys::CNetUtil::addrToString(peer_id).c_str());

      return ret;
    }


    int ClientRequestServer::resolve_block_version_conflict_(uint64_t block_id, Lease& lease)
    {
      int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        lease.dump(TBSYS_LOG_LEVEL_INFO, "resolve block version conflict, information: ");
        ResolveBlockVersionConflictMessage req_msg;
        req_msg.set_block(block_id);
        ret = lease.get_member_info(req_msg.get_members());
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
          ret = send_msg_to_server(ns_ip_port_, client, &req_msg, ret_msg);
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

    int ClientRequestServer::update_block_info_(const uint64_t block_id, const common::UnlinkFlag unlink_flag)
    {
     int ret = (INVALID_BLOCK_ID != block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
     BlockInfoV2 block_info;
     if (TFS_SUCCESS == ret)
     {
       ret = DataManager::instance().get_block_info(block_id, block_info);
     }

     if (TFS_SUCCESS == ret)
     {
       UpdateBlockInfoMessageV2 req_msg;
       req_msg.set_block_info(block_info);
       req_msg.set_unlink_flag(unlink_flag);
       req_msg.set_server_id(ds_ip_port_);

       NewClient* client = NewClientManager::get_instance().create_client();
       if (NULL == client)
       {
         ret = EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR;
       }
       else
       {
         tbnet::Packet* ret_msg = NULL;
         ret = send_msg_to_server(ns_ip_port_, client, &req_msg, ret_msg);
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

  }
}


