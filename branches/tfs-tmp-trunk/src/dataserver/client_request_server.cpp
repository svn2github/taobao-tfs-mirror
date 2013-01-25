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


    inline DataHelper& ClientRequestServer::data_helper()
    {
      return service_.data_helper();
    }

    int ClientRequestServer::handle(tbnet::Packet* packet)
    {
      int ret = (NULL == packet) ? EXIT_POINTER_NULL : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        switch (pcode)
        {
          case REQ_CALL_DS_REPORT_BLOCK_MESSAGE:
            ret = report_block(dynamic_cast<CallDsReportBlockRequestMessage*>(packet));
            break;
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

    int ClientRequestServer::report_block(CallDsReportBlockRequestMessage* message)
    {
      int32_t ret = (NULL != message) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DsRuntimeGlobalInformation& info = DsRuntimeGlobalInformation::instance();
        CallDsReportBlockRequestMessage* msg = dynamic_cast<CallDsReportBlockRequestMessage*>(message);
        ReportBlocksToNsRequestMessage req_msg;
        req_msg.set_server(info.information_.id_);
        int32_t block_count = block_manager().get_all_logic_block_count();
        BlockInfoV2* blocks_ext = req_msg.alloc_blocks_ext(block_count);
        assert(NULL != blocks_ext);
        ArrayHelper<BlockInfoV2> blocks_helper(block_count, blocks_ext);
        block_manager().get_all_block_info(blocks_helper);
        TBSYS_LOG(INFO, "report block to ns, blocks size: %d",req_msg.get_block_count());

        NewClient* client = NewClientManager::get_instance().create_client();
        tbnet::Packet* message = NULL;
        ret = send_msg_to_server(msg->get_server(), client, &req_msg, message);
        if (TFS_SUCCESS == ret)
        {
          ret = message->getPCode() == RSP_REPORT_BLOCKS_TO_NS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == ret)
          {
            ReportBlocksToNsResponseMessage* msg = dynamic_cast<ReportBlocksToNsResponseMessage*>(message);
            TBSYS_LOG(INFO, "nameserver %s ask for expire block\n",
                tbsys::CNetUtil::addrToString(msg->get_server()).c_str());//TODO
            // data_management_.add_new_expire_block(&msg->get_blocks(), NULL, NULL);
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    int ClientRequestServer::stat_file(StatFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t flag = message->get_flag();
      uint64_t peer_id = message->get_connection()->getPeerId();
      const FamilyInfoExt& family_info = message->get_family_info();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id)) ?
        EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        FileInfoV2 file_info;
        file_info.id_ = file_id;
        if (INVALID_FAMILY_ID == family_info.family_id_)
        {
          ret = block_manager().stat(file_info, flag, block_id, attach_block_id);
        }
        else
        {
          ret = data_helper().stat_file_degrade(block_id,
              file_id, flag, family_info, file_info);
        }

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
      TBSYS_LOG(INFO, "STAT file %s. blockid: %"PRI64_PREFIX"u, "
          "fileid: %"PRI64_PREFIX"u, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          (TFS_SUCCESS == ret) ? "success" : "fail", block_id, file_id,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(), ret);

      return ret;
    }

    int ClientRequestServer::read_file(ReadFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t length = message->get_length();
      int32_t offset = message->get_offset();
      int8_t flag = message->get_flag();
      uint64_t peer_id = message->get_connection()->getPeerId();
      const FamilyInfoExt& family_info = message->get_family_info();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        FileInfoV2 file_info;
        file_info.id_ = file_id;
        if (INVALID_FAMILY_ID == family_info.family_id_)
        {
          ret = block_manager().stat(file_info,
              FORCE_STAT, block_id, message->get_attach_block_id());
        }
        else
        {
          ret = data_helper().stat_file_degrade(block_id,
              file_id, FORCE_STAT, family_info, file_info);
        }

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
            if (INVALID_FAMILY_ID == family_info.family_id_)
            {
              ret = block_manager().read(buffer,
                  length, offset, file_id, flag, block_id, attach_block_id);
              ret = (ret < 0) ? ret: TFS_SUCCESS;
            }
            else
            {
              ret = data_helper().read_file_degrade(block_id,
                  file_info, buffer, length, offset, flag, family_info);
            }
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
      VUINT64 servers = message->get_ds(); // will copy vector
      const char* data = message->get_data();
      uint64_t master_id = message->get_master_id();
      bool is_master = (master_id == service_.get_ds_ipport());
      uint64_t peer_id = message->get_connection()->getPeerId();
      FamilyInfoExt& family_info = message->get_family_info();

      int ret = ((NULL == data) || (INVALID_BLOCK_ID == block_id) ||
          (offset < 0) || (length <= 0)) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;

      // first write, create file id & lease id
      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id,
            file_id, lease_id, LEASE_TYPE_WRITE, servers);
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
        if (INVALID_FAMILY_ID != family_info.family_id_)
        {
          servers.clear();
          family_info.get_check_servers(servers);
        }
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == service_.get_ds_ipport())
          {
            continue;  // exclude self
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // not send family to slave
          NewClient* client = NewClientManager::get_instance().create_client();
          assert(NULL != client);
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(WARN, "write file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                block_id, file_id, lease_id, ret);
          }
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
          TBSYS_LOG(WARN, "write datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
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
      TBSYS_LOG(INFO, "write file %s, blockid: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
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
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint64_t master_id = message->get_master_id();
      uint32_t crc = message->get_crc();
      FamilyInfoExt& family_info = message->get_family_info();
      bool tmp = message->get_tmp_flag(); // if true, we are writing a tmp block
      bool is_master = (master_id == service_.get_ds_ipport());
      VUINT64 servers = message->get_ds(); // will copy vector
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id,
            file_id, lease_id, LEASE_TYPE_WRITE, servers);
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
        if (INVALID_FAMILY_ID != family_info.family_id_)
        {
          servers.clear();
          family_info.get_check_servers(servers);
        }
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == service_.get_ds_ipport())
          {
            continue;  // exclude self
          }
          if (INVALID_FAMILY_ID != family_info.family_id_)
          {
            message->set_attach_block_id(family_info.get_block(servers[i]));
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // won't send family to slave
          NewClient* client = NewClientManager::get_instance().create_client();
          assert(NULL != client);
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(WARN, "close file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                block_id, file_id, lease_id, ret);
          }
        }
      }

      // close file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        // it's wierd to set attach_block_id as the first parameter,
        // but it's should be, i want to keep block_id unchanged
        ret = data_manager().close_file(attach_block_id, block_id, file_id, lease_id, tmp, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close file fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
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
     TBSYS_LOG(INFO, "close file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
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
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();
      VUINT64 servers = message->get_ds(); // will copy vector
      int32_t version = message->get_version();
      uint64_t master_id = message->get_master_id();
      bool is_master = (master_id == service_.get_ds_ipport());
      FamilyInfoExt& family_info = message->get_family_info();

      int ret = (INVALID_BLOCK_ID == block_id || INVALID_FILE_ID == file_id) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = data_manager().prepare_lease(block_id,
            file_id, lease_id, LEASE_TYPE_UNLINK, servers);
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
        if (INVALID_FAMILY_ID != family_info.family_id_)
        {
          servers.clear();
          family_info.get_check_servers(servers);
        }
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == service_.get_ds_ipport())
          {
            continue;  // exclude self
          }
          if (INVALID_FAMILY_ID != family_info.family_id_)
          {
            message->set_attach_block_id(family_info.get_block(servers[i]));
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // won't send family to slave
          NewClient* client = NewClientManager::get_instance().create_client();
          assert(NULL != client);
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS == ret)
          {
            TBSYS_LOG(WARN, "unlink file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                block_id, file_id, lease_id, ret);
          }
        }
      }

      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        // it's wierd to set attach_block_id as the first parameter,
        // but it's should be, i want to keep block_id unchanged
        ret = data_manager().unlink_file(attach_block_id,
            block_id, file_id, lease_id, action, version, local);
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
      uint64_t block_id = message->get_block_id();
      int64_t family_id = message->get_family_id();
      int8_t index_num = message->get_index_num();
      bool tmp = message->get_tmp_flag();
      int ret = block_manager().new_block(block_id, tmp, family_id, index_num);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "new block %"PRI64_PREFIX"u fail, tmp: %d, family_id: %"PRI64_PREFIX"u, "
            "index num: %d, ret: %d", block_id, tmp, family_id, index_num, ret);
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
      bool tmp = message->get_tmp_flag();
      ret = block_manager().del_block(block_id, tmp);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "remove block %"PRI64_PREFIX"u fail, tmp: %d, ret: %d",
            block_id, tmp, ret);
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

        TBSYS_LOG(INFO, "WRITE file %s, blockid: %"PRI64_PREFIX"d, fileid: %"PRI64_PREFIX"u, "
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
      bool tmp = message->get_tmp_flag();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = data_manager().check_lease(block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        if (!tmp) // close tmp block no need to commit
        {
          ret = data_manager().update_block_info(block_id, file_id, lease_id, UNLINK_FLAG_NO);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "update block info fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
                block_id, file_id, lease_id, ret);
          }
        }

        if (TFS_SUCCESS != ret)
        {
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

    int ClientRequestServer::read_raw_data(message::ReadRawdataMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      int32_t length = message->get_length();
      int32_t offset = message->get_offset();

      int ret = ((INVALID_BLOCK_ID == block_id) || (length <= 0) || (offset < 0)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ReadRawdataRespMessageV2* resp_msg = new (std::nothrow) ReadRawdataRespMessageV2();
        assert(NULL != resp_msg);
        char* data = resp_msg->alloc_data(length);
        assert(NULL != data);
        ret = block_manager().pread(data, length, offset, block_id);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(resp_msg);
          TBSYS_LOG(WARN, "read raw data fail. blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d", block_id, length, offset, ret);
        }
        else
        {
          resp_msg->set_length(length);
          ret = message->reply(resp_msg);
        }
      }

      return ret;
    }

    int ClientRequestServer::write_raw_data(message::WriteRawdataMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      int32_t length = message->get_length();
      int32_t offset = message->get_offset();
      const char* data = message->get_data();

      int ret = ((INVALID_BLOCK_ID == block_id) || (length <= 0) || (offset < 0)
          || (NULL == data)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().pwrite(data, length, offset, block_id, true);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write raw data fail. blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, ret: %d", block_id, length, offset, ret);
        }
        else
        {
          ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }

      return ret;
    }

    int ClientRequestServer::read_index(message::ReadIndexMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      uint64_t attach_block_id = message->get_block_id();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_BLOCK_ID == attach_block_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ReadIndexRespMessageV2* resp_msg = new (std::nothrow) ReadIndexRespMessageV2();
        assert(NULL != resp_msg);
        ret = block_manager().traverse(resp_msg->get_index_data().header_,
            resp_msg->get_index_data().finfos_, block_id, attach_block_id);
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(resp_msg);
          TBSYS_LOG(WARN, "read index fail. blockid: %"PRI64_PREFIX"u, "
              "attach_block_id: %"PRI64_PREFIX"u, ret: %d",
              block_id, attach_block_id, ret);
        }
        else
        {
          ret = message->reply(resp_msg);
        }
      }

      return ret;
    }

    int ClientRequestServer::write_index(message::WriteIndexMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      uint64_t attach_block_id = message->get_block_id();
      IndexDataV2& index_data = message->get_index_data();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_BLOCK_ID == attach_block_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = block_manager().write_file_infos(index_data.header_,
            index_data.finfos_, block_id, attach_block_id, true);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write index fail. blockid: %"PRI64_PREFIX"u, "
              "attach_block_id: %"PRI64_PREFIX"u, ret: %d",
              block_id, attach_block_id, ret);
        }
        else
        {
          ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }

      return ret;
    }

    int ClientRequestServer::query_ec_meta(message::QueryEcMetaMessage* message)
    {
      uint64_t block_id = message->get_block_id();
      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        QueryEcMetaRespMessage* resp_msg = new (std::nothrow) QueryEcMetaRespMessage();
        assert(NULL != resp_msg);
        ECMeta& ec_meta = resp_msg->get_ec_meta();

        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_family_id(ec_meta.family_id_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_used_offset(ec_meta.used_offset_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = block_manager().get_marshalling_offset(ec_meta.mars_offset_, block_id);
        }

        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(resp_msg);
          TBSYS_LOG(WARN, "query ec meta fail. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id, ret);
        }
        else
        {
          ret = message->reply(resp_msg);
        }
      }

      return ret;
    }

    int ClientRequestServer::commit_ec_meta(message::CommitEcMetaMessage* message)
    {
      uint64_t block_id = message->get_block_id();
      ECMeta& ec_meta = message->get_ec_meta();
      int8_t switch_flag = message->get_switch_flag();

      int ret = (INVALID_BLOCK_ID == block_id) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        // commit family id
        if ((TFS_SUCCESS == ret) && (ec_meta.family_id_ >= 0))
        {
          ret = block_manager().set_family_id(ec_meta.family_id_, block_id);
        }

        // commit marshalling length
        if ((TFS_SUCCESS == ret) && (ec_meta.mars_offset_ > 0))
        {
          ret = block_manager().set_marshalling_offset(ec_meta.mars_offset_, block_id);
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "commit ec meta fail. blockid: %"PRI64_PREFIX"u, ret: %d",
              block_id, ret);
        }
        else if (switch_flag) // if need, switch block
        {
          ret = block_manager().switch_logic_block(block_id, true);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "switch logic block fail. blockid: %"PRI64_PREFIX"u, "
                "ret: %d", block_id, ret);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }

      return ret;
    }

  }
}


