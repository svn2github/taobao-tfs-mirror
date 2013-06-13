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

#include "ds_define.h"
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


    inline BlockManager& ClientRequestServer::get_block_manager()
    {
      return service_.get_block_manager();
    }

    inline DataManager& ClientRequestServer::get_data_manager()
    {
      return service_.get_data_manager();
    }

    inline TrafficControl& ClientRequestServer::get_traffic_control()
    {
      return service_.get_traffic_control();
    }

    inline DataHelper& ClientRequestServer::get_data_helper()
    {
      return service_.get_data_helper();
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
          case READ_RAWDATA_MESSAGE_V2:
            ret = read_raw_data(dynamic_cast<ReadRawdataMessageV2*>(packet));
            break;
          case WRITE_RAWDATA_MESSAGE_V2:
            ret = write_raw_data(dynamic_cast<WriteRawdataMessageV2*>(packet));
            break;
          case READ_INDEX_MESSAGE_V2:
            ret = read_index(dynamic_cast<ReadIndexMessageV2*>(packet));
            break;
          case WRITE_INDEX_MESSAGE_V2:
            ret = write_index(dynamic_cast<WriteIndexMessageV2*>(packet));
            break;
          case QUERY_EC_META_MESSAGE:
            ret = query_ec_meta(dynamic_cast<QueryEcMetaMessage*>(packet));
            break;
          case COMMIT_EC_META_MESSAGE:
            ret = commit_ec_meta(dynamic_cast<CommitEcMetaMessage*>(packet));
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
        int32_t block_count = get_block_manager().get_all_logic_block_count();
        BlockInfoV2* blocks_ext = req_msg.alloc_blocks_ext(block_count);
        assert(NULL != blocks_ext);
        ArrayHelper<BlockInfoV2> blocks_helper(block_count, blocks_ext);
        get_block_manager().get_all_block_info(blocks_helper);
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
            std::vector<uint64_t>& cleanup_family_id_array = msg->get_blocks();
            TBSYS_LOG(INFO, "nameserver %s ask for cleanup family id blocks, count: %zd",
                tbsys::CNetUtil::addrToString(msg->get_server()).c_str(), cleanup_family_id_array.size());
            std::vector<uint64_t>::const_iterator iter = cleanup_family_id_array.begin();
            for (; iter != cleanup_family_id_array.end(); ++iter)
            {
              service_.get_block_manager().set_family_id(INVALID_FAMILY_ID, (*iter));
            }
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
          ret = get_block_manager().stat(file_info, flag, block_id, attach_block_id);
        }
        else
        {
          ret = get_data_helper().stat_file_degrade(block_id,
              file_id, flag, family_info, file_info);
        }

        if (TFS_SUCCESS == ret)
        {
          StatFileRespMessageV2* resp_msg = new (std::nothrow) StatFileRespMessageV2();
          assert(NULL != resp_msg);
          resp_msg->set_file_info(file_info);
          ret = message->reply(resp_msg);
        }
      }

      TIMER_END();

      get_traffic_control().rw_stat(RW_STAT_TYPE_STAT, ret, true, 0);

      // access log
      TBSYS_LOG(INFO, "STAT file %s. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
          "fileid: %"PRI64_PREFIX"u, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          (TFS_SUCCESS == ret) ? "success" : "fail", block_id, attach_block_id, file_id,
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
          ret = get_block_manager().stat(file_info,
              FORCE_STAT, block_id, message->get_attach_block_id());
        }
        else
        {
          ret = get_data_helper().stat_file_degrade(block_id,
              file_id, FORCE_STAT, family_info, file_info);
        }

        if (TFS_SUCCESS == ret)
        {
          // truncate read length
          if (offset + length > file_info.size_)
          {
            length = file_info.size_ - offset;
          }

          ret = (length < 0) ? EXIT_PARAMETER_ERROR: TFS_SUCCESS;
          if (TFS_SUCCESS == ret)
          {
            ReadFileRespMessageV2* resp_msg = new (std::nothrow) ReadFileRespMessageV2();
            assert(NULL != message);

            // if length is truncated to 0
            // reply a packet with length 0 to tell client that it already reach to the end of file
            if (0 == length)
            {
              resp_msg->set_length(0);
            }
            else
            {
              char* buffer = resp_msg->alloc_data(length);
              assert(NULL != buffer);
              if (INVALID_FAMILY_ID == family_info.family_id_)
              {
                ret = get_block_manager().read(buffer,
                    length, offset, file_id, flag, block_id, attach_block_id);
                ret = (ret < 0) ? ret: TFS_SUCCESS;
              }
              else
              {
                ret = get_data_helper().read_file_degrade(block_id,
                    file_info, buffer, length, offset, flag, family_info);
              }
            }

            if (TFS_SUCCESS != ret)
            {
              // upper layer will reply error packet to client
              tbsys::gDelete(resp_msg);
            }
            else
            {
              // readv2 support
              if (flag & READ_DATA_OPTION_WITH_FINFO)
              {
                resp_msg->set_file_info(file_info);
              }
              ret = message->reply(resp_msg);
              if (TFS_SUCCESS == ret)
              {
                get_traffic_control().rw_traffic_stat(false, length);
              }
            }
          }
        }
      }

      TIMER_END();

      get_traffic_control().rw_stat(RW_STAT_TYPE_READ, ret, offset <= FILEINFO_EXT_SIZE, length);

      // access log
      TBSYS_LOG(INFO, "READ file %s. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
          "fileid: %"PRI64_PREFIX"u, length: %d, offset: %d, peer ip: %s, "
          "cost: %"PRI64_PREFIX"d, degrade: %s, ret: %d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, attach_block_id, file_id, length, offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(),
          INVALID_FAMILY_ID == family_info.family_id_ ? "no" : "yes", ret);

      return ret;
    }


    /**
     * (attach_block_id, file_id, lease_id) is a unique identifier
     *
     * attach block id won't be changed in whole write/unlink process
     */
    int ClientRequestServer::write_file(WriteFileMessageV2* message)
    {
      TIMER_START();
      uint64_t block_id = message->get_block_id();
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t offset = message->get_offset();
      int32_t length = message->get_length();
      int32_t version = message->get_version();
      VUINT64 servers = message->get_ds(); // will copy vector
      const char* data = message->get_data();
      uint64_t master_id = message->get_master_id();
      uint64_t peer_id = message->get_connection()->getPeerId();
      FamilyInfoExt& family_info = message->get_family_info();
      int64_t family_id = family_info.family_id_;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      bool is_master = (master_id == ds_info.information_.id_);
      bool lease_ok = false;

      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (NULL == data) || (offset < 0) || (length <= 0))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      // tbnet already receive this packet from network
      get_traffic_control().rw_traffic_stat(true, length);

      // transform servers
      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_info.family_id_))
      {
        VUINT64 check_servers;
        family_info.get_check_servers(check_servers);
        for (uint32_t i = 0; i < check_servers.size(); i++)
        {
          if (INVALID_SERVER_ID != check_servers[i])
          {
            servers.push_back(check_servers[i]);
          }
        }
      }

      // first write, create file id & lease id
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_manager().prepare_lease(attach_block_id,
            file_id, lease_id, LEASE_TYPE_WRITE, servers, true);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "prepare write lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, role: %s, ret: %d",
              attach_block_id, file_id, lease_id, is_master ? "master" : "slave", ret);
        }
        else
        {
          // lease maybe overload, or expired, in this case, master need reply errro msg
          lease_ok = true;

          // callback & slave will use
          message->set_file_id(file_id);
          message->set_lease_id(lease_id);
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        assert(NULL != client);
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == ds_info.information_.id_)
          {
            continue;  // exclude self
          }
          if (INVALID_FAMILY_ID != family_id)
          {
            message->set_block_id(family_info.get_block(servers[i]));
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // not send family to slave
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                attach_block_id, file_id, lease_id, ret);
          }
        }
      }

      // local write file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_manager().write_file(block_id, attach_block_id,
            file_id, lease_id, data, length, offset, version, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write datafile fail. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
              "lease id: %"PRI64_PREFIX"u, length: %d, offset: %d, version: %d, role: %s, ret: %d",
              attach_block_id, file_id, lease_id, length, offset, version,
              is_master? "master": "slave", ret);
        }
        get_data_manager().update_lease(attach_block_id, file_id, lease_id, ret, local);
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        if (lease_ok)
        {
          write_file_callback(message);
        }
        else
        {
          message->reply(new StatusMessage(ret, "master prepare lease fail"));
        }
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(ds_info.information_.id_);
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);

        // slave write fail, remove lease
        if (TFS_SUCCESS != ret)
        {
          get_data_manager().remove_lease(attach_block_id, file_id, lease_id);
        }
      }

      TIMER_END();

      TBSYS_LOG(INFO, "write file %s, blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
          "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, length: %d, offset: %d, "
          "version: %d, role: %s, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          (TFS_SUCCESS == ret) ? "success" : "fail", block_id, attach_block_id, file_id, lease_id,
          length, offset, version, is_master ? "master": "slave",
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(), ret);

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
      int32_t status = message->get_status();
      FamilyInfoExt& family_info = message->get_family_info();
      int64_t family_id = family_info.family_id_;
      bool tmp = message->get_tmp_flag(); // if true, we are writing a tmp block
      VUINT64 servers = message->get_ds(); // will copy vector
      uint64_t peer_id = message->get_connection()->getPeerId();
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      bool is_master = (master_id == ds_info.information_.id_);
      bool lease_ok = false;

      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_FILE_ID == file_id) ||
          (INVALID_LEASE_ID == lease_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      // transform servers
      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_info.family_id_))
      {
        VUINT64 check_servers;
        family_info.get_check_servers(check_servers);
        for (uint32_t i = 0; i < check_servers.size(); i++)
        {
          if (INVALID_SERVER_ID != check_servers[i])
          {
            servers.push_back(check_servers[i]);
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = get_data_manager().prepare_lease(attach_block_id,
            file_id, lease_id, LEASE_TYPE_WRITE, servers, false);
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(WARN, "prepare write lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              attach_block_id, file_id, lease_id, ret);
        }
        else
        {
          lease_ok = true;
        }
      }

      // post request to slaves
      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        assert(NULL != client);
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == ds_info.information_.id_)
          {
            continue;  // exclude self
          }
          if (INVALID_FAMILY_ID != family_id)
          {
            message->set_block_id(family_info.get_block(servers[i]));
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // won't send family to slave
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "close file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                attach_block_id, file_id, lease_id, ret);
          }
        }
      }

      // close file
      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        ret = get_data_manager().close_file(block_id,
            attach_block_id, file_id, lease_id, crc, status, tmp, local);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close file fail. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: %s, ret: %d",
              block_id, attach_block_id, file_id, lease_id, is_master? "master" : "slave", ret);
        }
        get_data_manager().update_lease(attach_block_id, file_id, lease_id, ret, local);
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        if (lease_ok)
        {
          close_file_callback(message);
        }
        else
        {
          message->reply(new StatusMessage(ret, "master prepare lease fail"));
        }
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(ds_info.information_.id_);
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);
        get_data_manager().remove_lease(attach_block_id, file_id, lease_id);
      }

     TIMER_END();

     // access log
     TBSYS_LOG(INFO, "close file %s. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
         "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, crc: %u, role: %s, "
         "peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
         TFS_SUCCESS == ret ? "success" : "fail", block_id, attach_block_id, file_id, lease_id, crc,
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
      bool prepare = message->get_prepare_flag();
      FamilyInfoExt& family_info = message->get_family_info();
      int64_t family_id = family_info.family_id_;
      DsRuntimeGlobalInformation& ds_info = DsRuntimeGlobalInformation::instance();
      bool is_master = (master_id == ds_info.information_.id_);
      bool lease_ok = false;

      int ret = TFS_SUCCESS;
      if ((INVALID_BLOCK_ID == block_id) ||
          (INVALID_BLOCK_ID == attach_block_id) ||
          (INVALID_FILE_ID == file_id))
      {
        ret = EXIT_PARAMETER_ERROR;
      }

      // transform servers
      if ((TFS_SUCCESS == ret) && (INVALID_FAMILY_ID != family_info.family_id_))
      {
        VUINT64 check_servers;
        family_info.get_check_servers(check_servers);
        for (uint32_t i = 0; i < check_servers.size(); i++)
        {
          if (INVALID_SERVER_ID != check_servers[i])
          {
            servers.push_back(check_servers[i]);
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = get_data_manager().prepare_lease(attach_block_id,
            file_id, lease_id, LEASE_TYPE_UNLINK, servers, prepare);
        if (TFS_SUCCESS != ret)
        {
         TBSYS_LOG(WARN, "prepare unlink lease fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              attach_block_id, file_id, lease_id, ret);
        }
        else
        {
          lease_ok = true;
          // callback & slave will use
          message->set_lease_id(lease_id);
        }
      }

      if ((TFS_SUCCESS == ret) && (servers.size() > 1U) && is_master)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        assert(NULL != client);
        for (uint32_t i = 0; (TFS_SUCCESS == ret) && (i < servers.size()); i++)
        {
          if (servers[i] == ds_info.information_.id_)
          {
            continue;  // exclude self
          }
          if (INVALID_FAMILY_ID != family_id)
          {
            message->set_block_id(family_info.get_block(servers[i]));
          }
          family_info.family_id_ = INVALID_FAMILY_ID; // won't send family to slave
          ret = post_msg_to_server(servers[i], client, message, ds_async_callback);
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "unlink file to slave fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, lease id: %"PRI64_PREFIX"u, role: master ret: %d",
                attach_block_id, file_id, lease_id, ret);
          }
        }
      }

      BlockInfoV2 local;
      if (TFS_SUCCESS == ret)
      {
        if (prepare)
        {
          ret = get_data_manager().prepare_unlink_file(block_id,
              attach_block_id, file_id, lease_id, action, version, local);
        }
        else
        {
          ret = get_data_manager().unlink_file(block_id,
              attach_block_id, file_id, lease_id, action, local);
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "unlink file fail. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, action: %d, "
              "version: %d, role: %s, prepare: %s, ret: %d",
            block_id, attach_block_id, file_id, lease_id, action,
            version, is_master ? "master" : "slave", prepare ? "true" : "false", ret);
        }
        get_data_manager().update_lease(attach_block_id, file_id, lease_id, ret, local);
      }

      // master check if all successful
      // slave response to master
      if (is_master)
      {
        if (lease_ok)
        {
          if (prepare)
          {
            prepare_unlink_file_callback(message);
          }
          else
          {
            unlink_file_callback(message);
          }
        }
        else
        {
          message->reply(new StatusMessage(ret, "master prepare lease fail"));
        }
      }
      else
      {
        SlaveDsRespMessage* resp_msg = new (std::nothrow) SlaveDsRespMessage();
        assert(NULL != resp_msg);
        resp_msg->set_server_id(ds_info.information_.id_);
        resp_msg->set_block_info(local);
        resp_msg->set_status(ret);
        message->reply(resp_msg);

        if (!prepare || (prepare && (TFS_SUCCESS != ret)))
        {
          get_data_manager().remove_lease(attach_block_id, file_id, lease_id);
        }
      }

      TIMER_END();

      // access log
      TBSYS_LOG(INFO, "unlink file %s. blockid: %"PRI64_PREFIX"u, attach_blockid: %"PRI64_PREFIX"u, "
          "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, action: %d, version: %d, role: %s, "
          "prepare: %s, peer ip: %s, cost: %"PRI64_PREFIX"d, ret: %d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, attach_block_id, file_id, lease_id, action, version,
          is_master ? "master" : "slave", prepare ? "true" : "false",
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION(), ret);

      return TFS_SUCCESS;
    }

    int ClientRequestServer::new_block(NewBlockMessageV2* message)
    {
      uint64_t block_id = message->get_block_id();
      int64_t family_id = message->get_family_id();
      int8_t index_num = message->get_index_num();
      bool tmp = message->get_tmp_flag();
      int32_t expire_time = message->get_expire_time();
      int ret = get_block_manager().new_block(block_id, tmp, family_id, index_num, expire_time);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "new block %"PRI64_PREFIX"u fail, tmp: %d, family_id: %"PRI64_PREFIX"u, "
            "index num: %d, expire time: %d, ret: %d",
            block_id, tmp, family_id, index_num, expire_time, ret);
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
      ret = get_block_manager().del_block(block_id, tmp);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "remove block %"PRI64_PREFIX"u fail, tmp: %d, ret: %d",
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
          uint64_t block_id = msg->get_attach_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            get_data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          write_file_callback(msg);
        }
        else if (CLOSE_FILE_MESSAGE_V2 == pcode)
        {
          CloseFileMessageV2* msg = dynamic_cast<CloseFileMessageV2*> (source);
          uint64_t block_id = msg->get_attach_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            get_data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          close_file_callback(msg);
        }
        else if (UNLINK_FILE_MESSAGE_V2 == pcode)
        {
          UnlinkFileMessageV2* msg = dynamic_cast<UnlinkFileMessageV2*> (source);
          uint64_t block_id = msg->get_attach_block_id();
          uint64_t file_id = msg->get_file_id();
          uint64_t lease_id = msg->get_lease_id();
          bool prepare = msg->get_prepare_flag();
          NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
          for ( ; iter != sresponse->end(); iter++)
          {
            get_data_manager().update_lease(block_id, file_id, lease_id, iter->second.second);
          }
          if (prepare)
          {
            prepare_unlink_file_callback(msg);
          }
          else
          {
            unlink_file_callback(msg);
          }
        }
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::write_file_callback(WriteFileMessageV2* message)
    {
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint32_t length = message->get_length();
      uint32_t offset = message->get_offset();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = get_data_manager().check_lease(attach_block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        if (TFS_SUCCESS != ret)
        {
          // req ns resolve version conflict
          if (EXIT_BLOCK_VERSION_CONFLICT_ERROR == ret)
          {
            if (TFS_SUCCESS != get_data_manager().resolve_block_version_conflict(attach_block_id, file_id, lease_id))
            {
              TBSYS_LOG(WARN, "resolve block version conflict fail. "
                  "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
                  attach_block_id, file_id, lease_id, ret);
            }
          }
          message->reply_error_packet(TBSYS_LOG_LEVEL(WARN), ret, err_msg.str().c_str());

          // if fail, close will never happen, remove lease
          get_data_manager().remove_lease(attach_block_id, file_id, lease_id);
        }
        else
        {
          WriteFileRespMessageV2* resp_msg = new (std::nothrow) WriteFileRespMessageV2();
          assert(NULL != resp_msg);
          resp_msg->set_file_id(file_id);
          resp_msg->set_lease_id(lease_id);
          message->reply(resp_msg);
        }

        if (TFS_SUCCESS != ret)
        {
          get_traffic_control().rw_stat(RW_STAT_TYPE_WRITE, ret, 0 == offset, length);
        }

        TBSYS_LOG(INFO, "WRITE file %s, ret: %d, attach_blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, "
            "length: %d, offset: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
            (TFS_SUCCESS == ret) ? "success": "fail", ret, attach_block_id,
            file_id, lease_id, length, offset,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::close_file_callback(CloseFileMessageV2* message)
    {
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      uint64_t peer_id = message->get_connection()->getPeerId();
      bool tmp = message->get_tmp_flag();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = get_data_manager().check_lease(attach_block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        if (!tmp) // close tmp block no need to commit & write log
        {
          /*
           * commit success, keep ret unchanged
           * commit fail, set ret to error code
           */
          int tmp_ret = get_data_manager().update_block_info(attach_block_id, file_id, lease_id, UPDATE_BLOCK_INFO_WRITE);
          if (TFS_SUCCESS != tmp_ret)
          {
            ret = tmp_ret;
            TBSYS_LOG(WARN, "update block info fail. blockid: %"PRI64_PREFIX"u, "
                "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
                attach_block_id, file_id, lease_id, ret);
          }

          if (TFS_SUCCESS == ret)
          {
            std::vector<SyncBase*>& sync_mirror = service_.get_sync_mirror();
            for (uint32_t i = 0; (TFS_SUCCESS == ret) && i < sync_mirror.size(); i++)
            {
              ret = sync_mirror[i]->write_sync_log(OPLOG_INSERT, attach_block_id, file_id);
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "write sync log fail. blockid: %"PRI64_PREFIX"u, "
                    "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, index: %d, ret: %d",
                    attach_block_id, file_id, lease_id, i, ret);
              }
            }
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

        get_traffic_control().rw_stat(RW_STAT_TYPE_WRITE, ret, true, file_size);

        // after close, remove lease
        get_data_manager().remove_lease(attach_block_id, file_id, lease_id);

        TBSYS_LOG(INFO, "CLOSE file %s. blockid: %"PRI64_PREFIX"u, "
            "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, peer ip: %s, cost: %"PRI64_PREFIX"d",
            TFS_SUCCESS == ret ? "success" : "fail", attach_block_id, file_id, lease_id,
            tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::prepare_unlink_file_callback(UnlinkFileMessageV2* message)
    {
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = get_data_manager().check_lease(attach_block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        if (TFS_SUCCESS != ret)
        {
          if (EXIT_BLOCK_VERSION_CONFLICT_ERROR == ret)
          {
            // ignore return value
            if (TFS_SUCCESS != get_data_manager().resolve_block_version_conflict(attach_block_id, file_id, lease_id))
            {
              TBSYS_LOG(WARN, "resolve block version conflict fail. "
                  "blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u",
                  attach_block_id, file_id, lease_id);
            }
          }
          message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, err_msg.str().c_str());
        }
        else
        {
          char ex_msg[64];
          snprintf(ex_msg, 64, "%"PRI64_PREFIX"d", lease_id);
          message->reply(new StatusMessage(STATUS_MESSAGE_OK, ex_msg));
        }

        if (TFS_SUCCESS != ret)
        {
          get_traffic_control().rw_stat(RW_STAT_TYPE_UNLINK, ret, true, 0);
        }

        TBSYS_LOG(INFO, "PREPARE UNLINK file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, action: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "success" : "fail", attach_block_id, file_id, lease_id, action,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), req_cost_time);
      }

      return TFS_SUCCESS;
    }

    int ClientRequestServer::unlink_file_callback(UnlinkFileMessageV2* message)
    {
      uint64_t attach_block_id = message->get_attach_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t lease_id = message->get_lease_id();
      int32_t action = message->get_action();
      uint64_t peer_id = message->get_connection()->getPeerId();
      int64_t file_size = 0;
      int64_t req_cost_time = 0;
      stringstream err_msg;

      int ret = TFS_SUCCESS;
      bool all_finish = get_data_manager().check_lease(attach_block_id,
          file_id, lease_id, ret, req_cost_time, file_size, err_msg);
      if (all_finish)
      {
        /*
         * commit success, keep ret unchanged
         * commit fail, set ret to error code
         */
        int tmp_ret = get_data_manager().update_block_info(attach_block_id, file_id, lease_id, UPDATE_BLOCK_INFO_UNLINK);
        if (TFS_SUCCESS != tmp_ret)
        {
          ret = tmp_ret;
          TBSYS_LOG(WARN, "update block info fail. blockid: %"PRI64_PREFIX"u, "
              "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, ret: %d",
              attach_block_id, file_id, lease_id, ret);
        }

        if (TFS_SUCCESS == ret)
        {
          std::vector<SyncBase*>& sync_mirror = service_.get_sync_mirror();
          for (uint32_t i = 0; (TFS_SUCCESS == ret) && i < sync_mirror.size(); i++)
          {
            ret = sync_mirror[i]->write_sync_log(OPLOG_REMOVE, attach_block_id, file_id, action);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write sync log fail. blockid: %"PRI64_PREFIX"u, "
                  "fileid: %"PRI64_PREFIX"u, leaseid: %"PRI64_PREFIX"u, index: %d, ret: %d",
                  attach_block_id, file_id, lease_id, i, ret);
            }
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

        get_traffic_control().rw_stat(RW_STAT_TYPE_UNLINK, ret, true, 0);

        // after unlink, remove lease
        get_data_manager().remove_lease(attach_block_id, file_id, lease_id);

        TBSYS_LOG(INFO, "UNLINK file %s. blockid: %"PRI64_PREFIX"u, fileid: %"PRI64_PREFIX"u, "
            "leaseid: %"PRI64_PREFIX"u, action: %d, peer ip: %s, cost: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "success" : "fail", attach_block_id, file_id, lease_id, action,
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

      if ((TFS_SUCCESS == ret) && get_traffic_control().mr_traffic_out_of_threshold(false))
      {
        ret = EXIT_NETWORK_BUSY_ERROR;
      }

      if (TFS_SUCCESS == ret)
      {
        ReadRawdataRespMessageV2* resp_msg = new (std::nothrow) ReadRawdataRespMessageV2();
        assert(NULL != resp_msg);
        char* data = resp_msg->alloc_data(length);
        assert(NULL != data);
        ret = get_block_manager().pread(data, length, offset, block_id);
        ret = (ret < 0) ? ret : TFS_SUCCESS;
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
          if (TFS_SUCCESS == ret)
          {
            get_traffic_control().mr_traffic_stat(false, length);
          }
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
      bool tmp = message->get_tmp_flag();
      bool is_new = message->get_new_flag();

      int ret = ((INVALID_BLOCK_ID == block_id) || (length <= 0) || (offset < 0)
          || (NULL == data)) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      // tbnet already receive this packet from network
      get_traffic_control().mr_traffic_stat(true, length);

      // add this to support transfer tool
      if ((TFS_SUCCESS == ret) && is_new)
      {
        ret = get_block_manager().new_block(block_id, tmp);
      }

      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().pwrite(data, length, offset, block_id, tmp);
        ret = (ret < 0) ? ret : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write raw data fail. blockid: %"PRI64_PREFIX"u, "
              "length: %d, offset: %d, tmp: %d, ret: %d", block_id, length, offset, tmp, ret);
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
      uint64_t attach_block_id = message->get_attach_block_id();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_BLOCK_ID == attach_block_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ReadIndexRespMessageV2* resp_msg = new (std::nothrow) ReadIndexRespMessageV2();
        assert(NULL != resp_msg);
        ret = get_block_manager().traverse(resp_msg->get_index_data().header_,
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
      uint64_t attach_block_id = message->get_attach_block_id();
      IndexDataV2& index_data = message->get_index_data();
      bool tmp = message->get_tmp_flag();
      bool partial = message->get_partial_flag();
      bool is_cluster = message->get_cluster_flag();

      int ret = ((INVALID_BLOCK_ID == block_id) || (INVALID_BLOCK_ID == attach_block_id)) ?
        EXIT_PARAMETER_ERROR : TFS_SUCCESS;

      if (TFS_SUCCESS == ret)
      {
        ret = get_block_manager().write_file_infos(index_data.header_,
            index_data.finfos_, block_id, attach_block_id, tmp, partial);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write index fail. blockid: %"PRI64_PREFIX"u, "
              "attach_block_id: %"PRI64_PREFIX"u, tmp: %d, ret: %d",
              block_id, attach_block_id, tmp, ret);
        }
        else
        {
          if (is_cluster) // only happened when replicate between cluster
          {
            BlockInfoV2 local_info;
            ret = get_block_manager().get_block_info(local_info, block_id);
            if (TFS_SUCCESS == ret)
            {
              ret = get_data_manager().update_block_info(local_info, UPDATE_BLOCK_INFO_REPL);
            }
          }
          if (TFS_SUCCESS == ret)
          {
            ret = message->reply(new StatusMessage(STATUS_MESSAGE_OK));
          }
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
          ret = get_block_manager().get_family_id(ec_meta.family_id_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_used_offset(ec_meta.used_offset_, block_id);
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_block_manager().get_marshalling_offset(ec_meta.mars_offset_, block_id);
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
        BaseLogicBlock* logic_block = get_block_manager().get(block_id, switch_flag);
        ret = (NULL != logic_block) ? TFS_SUCCESS  : EXIT_NO_LOGICBLOCK_ERROR;

        // commit family id
        if ((TFS_SUCCESS == ret) && (ec_meta.family_id_ >= 0))
        {
          ret = logic_block->set_family_id(ec_meta.family_id_);
        }

        // commit marshalling length
        if ((TFS_SUCCESS == ret) && (ec_meta.mars_offset_ > 0))
        {
          ret = logic_block->set_marshalling_offset(ec_meta.mars_offset_);
        }

        // update block version
        if ((TFS_SUCCESS == ret) && (ec_meta.version_step_ > 0))
        {
          ret = logic_block->update_block_version(ec_meta.version_step_);
        }

        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "commit ec meta fail. blockid: %"PRI64_PREFIX"u, switch flag: %d, ret: %d",
              block_id, switch_flag, ret);
        }
        else if (switch_flag) // if need, switch block
        {
          ret = get_block_manager().switch_logic_block(block_id, true);
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
  }/** end namespace dataserver **/
}/** end namespace tfs **/


