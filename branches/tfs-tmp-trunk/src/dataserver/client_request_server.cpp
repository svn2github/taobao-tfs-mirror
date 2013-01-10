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

    ClientRequestServer::ClientRequestServer(DataService& service):
      service_(service)
    {

    }

    ClientRequestServer::~ClientRequestServer()
    {
    }


    inline BlockManager& ClientRequestServer::block_manager()
    {
      return service_.block_manager();
    }

    inline DataManager& ClientRequestServer::data_manager()
    {
      return service_.data_manager();
    }

    int ClientRequestServer::handle(tbnet::Packet* packet)
    {
      int ret = (NULL == packet) ? EXIT_POINTER_NULL : TFS_SUCCESS;
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
            TBSYS_LOG(WARN, "process packet pcode: %d\n", pcode);
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
        file_info.id_ = file_id;
        ret = block_manager().stat(file_info, flag, block_id, block_id);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "read file info fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, flag: %d, ret: %d",
              block_id, file_id, flag, ret);
        }
        else
        {
          StatFileRespMessageV2* resp_msg = new (std::nothrow) StatFileRespMessageV2();
          assert(NULL != resp_msg);
          resp_msg->set_file_info(file_info);
          ret = message->reply(resp_msg);
        }
      }

      TIMER_END();

      // access log
      TBSYS_LOG(INFO, "STAT file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
          "peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          (TFS_SUCCESS == ret) ? "success" : "fail", block_id, file_id,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(), ret);

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
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        FileInfoV2 file_info;
        file_info.id_ = file_id;
        ret = block_manager().stat(file_info, FORCE_STAT, block_id, block_id);
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
            ret = block_manager().read(buffer, length, offset, file_id, flag, block_id, block_id);
            ret = (ret < 0) ? ret: TFS_SUCCESS;
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
              ret = message->reply(resp_msg);
            }
          }
        }
      }

      TIMER_END();

      // access log
      TBSYS_LOG(INFO, "READ file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
          "length: %d, offset: %d, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, length, offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(), ret);

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
      uint64_t master_id = message->get_master_id();
      bool is_master = (master_id == service_.get_ds_ipport());
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = ((NULL == data) || (INVALID_BLOCK_ID == block_id) ||
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      // first write, create file id & lease id
      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id, file_id, lease_id, LEASE_TYPE_WRITE, servers);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "prepare write lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, file_id, lease_id, is_master ? "master" : "slave", ret);
        }
        else
        {
          // callback & slave will use
          message->set_file_id(file_id);
          message->set_lease_id(lease_id);
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        int32_t result = service_.post_message_to_server(message, servers);
        ret = (result == EXIT_POST_MSG_RET_POST_MSG_ERROR) ? EXIT_SENDMSG_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write data to slave fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, role: master, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      // local write file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().write_file(block_id,
            file_id, lease_id, data, length, offset, version, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write data to datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, length: %d, offset: %d, version: %d, role: %s, ret: %d",
              block_id, file_id, lease_id, length, offset, version,
              is_master? "master": "slave", ret);
        }
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        write_file_callback(message);
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(service_.get_ds_ipport());
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);

        // slave write fail, remove lease
        if (TFS_SUCCESS != ret)
        {
          data_manager().remove_lease(block_id, file_id, lease_id);
        }
      }

      TIMER_END();

      // access log
      TBSYS_LOG(INFO, "write file %s, block: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
          "leaseid: %"PRI64_PREFIX"u, length: %d, offset: %d, version: %d, role: %s, "
          "peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          (TFS_SUCCESS == ret) ? "success" : "fail", block_id, file_id, lease_id, length, offset,
          version, is_master ? "master": "slave", tbsys::CNetUtil::addrToString(peer_id).c_str(),
          TIMER_DURATION(), ret);

      return TFS_SUCCESS;
    }

    int ClientRequestServer::close_file(CloseFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint64_t master_id = message->get_master_id();
      uint32_t crc = message->get_crc();
      bool is_master = (master_id == service_.get_ds_ipport());

      const VUINT64& servers = message->get_ds();
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id, file_id, lease_id, LEASE_TYPE_WRITE, servers);
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(WARN, "prepare write lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        int32_t result = service_.post_message_to_server(message, servers);
        ret = (result == EXIT_POST_MSG_RET_POST_MSG_ERROR) ? EXIT_SENDMSG_ERROR: TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close file to slave fail. block: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "leaseid: %"PRI64_PREFIX"u, role: master, ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      // close file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().close_file(block_id, file_id, lease_id, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close file fail. block: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, file_id, lease_id, is_master? "master" : "slave", ret);
        }
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        close_file_callback(message);
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(service_.get_ds_ipport());
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);
        data_manager().remove_lease(block_id, file_id, lease_id);
      }

     TIMER_END();

     // access log
     TBSYS_LOG(INFO, "close file %s. blockid: %" PRI64_PREFIX "u, fileid: %"PRI64_PREFIX"u, "
         "leaseid: %"PRI64_PREFIX"u, crc: %u, role: %s, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
         TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, lease_id, crc,
         is_master ? "master" : "slave", tbsys::CNetUtil::addrToString(peer_id).c_str(),
         TIMER_DURATION(), ret);

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
      uint64_t master_id = message->get_master_id();
      bool is_master = (master_id == service_.get_ds_ipport());

      int ret = (INVALID_BLOCK_ID == block_id || INVALID_FILE_ID == file_id) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id, file_id, lease_id, LEASE_TYPE_UNLINK, servers);
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(WARN, "prepare write lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }
        else
        {
          // callback & slave will use
          message->set_lease_id(lease_id);
        }
      }

      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        int32_t result = service_.post_message_to_server(message, servers);
        ret = (result  == EXIT_POST_MSG_RET_POST_MSG_ERROR) ?  EXIT_SENDMSG_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "unlink file to slave fail. block: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, role: master ret: %d",
              block_id, file_id, lease_id, ret);
        }
      }

      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().unlink_file(block_id, file_id, lease_id, action, version, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "unlink file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "leaseid: %"PRI64_PREFIX"u, action: %d, role: %s, ret: %d",
            block_id, file_id, lease_id, action, is_master ? "master" : "slave", ret);
        }
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        unlink_file_callback(message);
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(service_.get_ds_ipport());
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);
        data_manager().remove_lease(block_id, file_id, lease_id);
      }

      TIMER_END();

      // access log
      TBSYS_LOG(INFO, "unlink file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
          "action: %d, version: %d, role: %s, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, action, version,
          is_master ? "master" : "slave", tbsys::CNetUtil::addrToString(peer_id).c_str(),
          TIMER_DURATION(), ret);

      return TFS_SUCCESS;
    }

    int ClientRequestServer::new_block(NewBlockMessageV2* message)
    {
      int ret = TFS_SUCCESS;
      const uint64_t block_id = message->get_block_id();
      ret = block_manager().new_block(block_id, false);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "new block %"PRI64_PREFIX"u fail, ret: %d", block_id, ret);
      }
      else
      {
        ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    int ClientRequestServer::remove_block(RemoveBlockMessageV2* message)
    {
      int ret = TFS_SUCCESS;
      uint64_t block_id = message->get_block_id();
      ret = block_manager().del_block(block_id, false);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "remove block %"PRI64_PREFIX"u fail, ret: %d", block_id, ret);
      }
      else
      {
        ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
      }

      return ret;
    }

    int ClientRequestServer::callback(NewClient* client)
    {
      int ret = (NULL == client) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
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
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          write_file_callback(msg);
        }
        else if (CLOSE_FILE_MESSAGE_V2 == pcode)
        {
          CloseFileMessageV2* msg = dynamic_cast<CloseFileMessageV2*> (source);
          uint64_t block_id = msg->get_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          close_file_callback(msg);
        }
        else if (UNLINK_FILE_MESSAGE_V2 == pcode)
        {
          UnlinkFileMessageV2* msg = dynamic_cast<UnlinkFileMessageV2*> (source);
          uint64_t block_id = msg->get_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          unlink_file_callback(msg);
        }
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::write_file_callback(WriteFileMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint32_t length = message->get_length();
      uint32_t offset = message->get_offset();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = data_manager().check_lease(block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        if (TFS_SUCCESS != ret)
        {
          // req ns resolve version conflict
          if (EXIT_VERSION_CONFLICT_ERROR == ret)
          {
            if (TFS_SUCCESS != data_manager().resolve_block_version_conflict(block_id, file_id, lease_id))
            {
              TBSYS_LOG(WARN, "resolve block version conflict fail. "
                  "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
                  block_id, file_id, lease_id, ret);
            }
          }
          message->reply_error_packet(TBSYS_LOG_LEVEL(WARN), ret, err_msg.str().c_str());

          // if fail, close will never happen, remove lease
          data_manager().remove_lease(block_id, file_id, lease_id);
        }
        else
        {
          WriteFileRespMessageV2* resp_msg = new (std::nothrow) WriteFileRespMessageV2();
          assert(NULL != resp_msg);
          resp_msg->set_file_id(file_id);
          resp_msg->set_lease_id(lease_id);
          message->reply(resp_msg);
        }

        TBSYS_LOG(INFO, "WRITE file %s, block: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, length: %d, offset: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
            (TFS_SUCCESS == ret) ? "success": "fail", block_id, file_id, lease_id, length, offset,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::close_file_callback(CloseFileMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = data_manager().check_lease(block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        ret = data_manager().update_block_info(block_id, file_id, lease_id, UNLINK_FLAG_NO);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "update block info fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
          message->reply_error_packet(TBSYS_LOG_LEVEL(WARN), ret, err_msg.str().c_str());
        }
        else
        {
          message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }

        // after close, remove lease
        data_manager().remove_lease(block_id, file_id, lease_id);

        TBSYS_LOG(INFO, "CLOSE file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, peer ip: %s, cost: %"PRI64_PREFIX"d",
            TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, lease_id,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::unlink_file_callback(UnlinkFileMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = data_manager().check_lease(block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        int err = ret;
        ret = data_manager().update_block_info(block_id, file_id, lease_id, UNLINK_FLAG_YES);
        if (TFS_SUCCESS != ret)
        {
          message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, err_msg.str().c_str());
          TBSYS_LOG(WARN, "update block info fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              block_id, file_id, lease_id, ret);
        }

        if (EXIT_VERSION_CONFLICT_ERROR == err)
        {
          // ignore return value
          if (TFS_SUCCESS != data_manager().resolve_block_version_conflict(block_id, file_id, lease_id))
          {
            TBSYS_LOG(WARN, "resolve block version conflict fail. "
                "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u",
                block_id, file_id, lease_id);
          }
        }

        if (TFS_SUCCESS != ret)
        {
          message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, err_msg.str().c_str());
        }
        else
        {
          char ex_msg[64];
          snprintf(ex_msg, 64, "%"PRI64_PREFIX"d", file_size);
          message->reply(new StatusMessage(STATUS_MESSAGE_OK, ex_msg));
        }

        // after unlink, remove lease
        data_manager().remove_lease(block_id, file_id, lease_id);

        TBSYS_LOG(INFO, "UNLINK file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, action: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, lease_id, action,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }
  }
}


