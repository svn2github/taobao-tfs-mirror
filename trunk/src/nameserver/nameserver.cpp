/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: nameserver.cpp 983 2011-10-31 09:59:33Z duanfei $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com>
 *      - modify 2010-04-23
 *
 */
#include "nameserver.h"

#include <Service.h>
#include <Memory.hpp>
#include <iterator>
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "global_factory.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {
    NameServer::NameServer() :
      layout_manager_(*this),
      master_slave_heart_manager_(layout_manager_),
      heart_manager_(*this)
    {

    }

    NameServer::~NameServer()
    {

    }

    int NameServer::initialize(int /*argc*/, char* /*argv*/[])
    {
      int32_t ret =  SYSPARAM_NAMESERVER.initialize();
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize nameserver parameter error, must be exit");
        ret = EXIT_GENERAL_ERROR;
      }
      const char* ip_addr = get_ip_addr();
      if (NULL == ip_addr)//get ip addr
      {
        ret =  EXIT_CONFIG_ERROR;
        TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
      }

      if (TFS_SUCCESS == ret)
      {
        const char *dev_name = get_dev();
        if (NULL == dev_name)//get dev name
        {
          ret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s","nameserver not set dev_name");
        }
        else
        {
          uint32_t ip_addr_id = tbsys::CNetUtil::getAddr(ip_addr);
          if (0 == ip_addr_id)
          {
            ret =  EXIT_CONFIG_ERROR;
            TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
          }
          else
          {
            uint32_t local_ip = Func::get_local_addr(dev_name);
            NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            ngi.owner_ip_port_ = tbsys::CNetUtil::ipToAddr(local_ip, get_port());
            ngi.heart_ip_port_ = tbsys::CNetUtil::ipToAddr(local_ip, get_port() + 1);
            bool find_ip_in_dev = Func::is_local_addr(ip_addr_id);
            if (!find_ip_in_dev)
            {
              TBSYS_LOG(WARN, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
            }
          }
        }
      }

      //start clientmanager
      if (TFS_SUCCESS == ret)
      {
        NewClientManager::get_instance().destroy();
        assert(NULL != get_packet_streamer());
        assert(NULL != get_packet_factory());
        BasePacketStreamer* packet_streamer = dynamic_cast<BasePacketStreamer*>(get_packet_streamer());
        BasePacketFactory* packet_factory   = dynamic_cast<BasePacketFactory*>(get_packet_factory());
        ret = NewClientManager::get_instance().initialize(packet_factory, packet_streamer,
                NULL, &BaseService::golbal_async_callback_func, this);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "start client manager failed, must be exit!!!");
          ret = EXIT_NETWORK_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = initialize_ns_global_info();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "%s", "initialize nameserver global information error, must be exit");
          ret = EXIT_GENERAL_ERROR;
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret =  layout_manager_.initialize();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize layoutmanager failed, must be exit, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t port = get_listen_port() + 1;
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_HEART_THREAD_COUNT, 1);
        int32_t report_thread_count = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_REPORT_BLOCK_THREAD_COUNT, 2);
        ret = heart_manager_.initialize(heart_thread_count, report_thread_count, port);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize heart manager failed, must be exit, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        ret = master_slave_heart_manager_.initialize();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize master and slave heart manager failed, must be exit, ret: %d", ret);
        }
        else
        {
          if (GFactory::get_runtime_info().is_master())
          {
            ret = master_slave_heart_manager_.establish_peer_role_(GFactory::get_runtime_info());
            if (EXIT_ROLE_ERROR == ret)
            {
              TBSYS_LOG(INFO, "nameserve role error, must be exit, ret: %d", ret);
            }
            else
            {
              ret = TFS_SUCCESS;
            }
          }
        }
      }

      //start heartbeat loop
      if (TFS_SUCCESS == ret)
      {
        //if we're the master ns or slave ns ,we can start service now.change status to INITIALIZED.
        GFactory::get_runtime_info().owner_status_ = NS_STATUS_INITIALIZED;
        TBSYS_LOG(INFO, "nameserver running, listen port: %d", get_port());
      }
      return ret;
    }

    int NameServer::destroy_service()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (!ngi.is_destroyed())
      {
        GFactory::destroy();
        heart_manager_.destroy();
        master_slave_heart_manager_.destroy();
        layout_manager_.destroy();

        heart_manager_.wait_for_shut_down();
        master_slave_heart_manager_.wait_for_shut_down();
        layout_manager_.wait_for_shut_down();
        GFactory::wait_for_shut_down();
      }
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode NameServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode hret = tbnet::IPacketHandler::FREE_CHANNEL;
      bool bret = (NULL != connection) && (NULL != packet);
      if (bret)
      {
        TBSYS_LOG(DEBUG, "receive pcode : %d", packet->getPCode());
        if (!packet->isRegularPacket())
        {
          bret = false;
          TBSYS_LOG(WARN, "control packet, pcode: %d, peer ip: %s", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand(),
            tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str());
        }
        if (bret)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(packet);
          bpacket->set_connection(connection);
          bpacket->setExpireTime(MAX_RESPONSE_TIME);
          bpacket->set_direction(static_cast<DirectionStatus>(bpacket->get_direction()|DIRECTION_RECEIVE));

          if (bpacket->is_enable_dump())
          {
            bpacket->dump();
          }
          int32_t pcode = bpacket->getPCode();
          int32_t ret = common::TFS_ERROR;
          if (GFactory::get_runtime_info().is_master())
            ret = do_master_msg_helper(bpacket);
          else
            ret = do_slave_msg_helper(bpacket);
          if (common::TFS_SUCCESS == ret)
          {
            hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            switch (pcode)
            {
            case MASTER_AND_SLAVE_HEART_MESSAGE:
            case HEARTBEAT_AND_NS_HEART_MESSAGE:
              master_slave_heart_manager_.push(bpacket, 0, false);
              break;
            case OPLOG_SYNC_MESSAGE:
              layout_manager_.get_oplog_sync_mgr().push(bpacket, 0, false);
              break;
            default:
              if (!main_workers_.push(bpacket, work_queue_size_, false))
              {
                hret = tbnet::IPacketHandler::FREE_CHANNEL;
                bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard, peer ip: %s", get_ip_addr(),
                  tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str());
                bpacket->free();
              }
              break;
            }
          }
          else
          {
            bpacket->free();
            GFactory::get_runtime_info().dump(TBSYS_LOG_LEVEL(WARN), "the msg : %d will be ignored", ret);
            TBSYS_LOG(WARN, "the msg: %d will be ignored, peer ip: %s", pcode,
               tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str());
          }
        }
      }
      return hret;
    }

    /** handle packet*/
    bool NameServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t ret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          //TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case GET_BLOCK_INFO_MESSAGE_V2:
              ret = open(msg);
              break;
            case BATCH_GET_BLOCK_INFO_MESSAGE_V2:
              ret = batch_open(msg);
              break;
            case REPLICATE_BLOCK_MESSAGE:
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
            case REQ_EC_MARSHALLING_COMMIT_MESSAGE:
            case REQ_EC_REINSTATE_COMMIT_MESSAGE:
            case REQ_EC_DISSOLVE_COMMIT_MESSAGE:
              ret = layout_manager_.get_client_request_server().handle(msg);
              break;
            case SHOW_SERVER_INFORMATION_MESSAGE:
              ret = show_server_information(msg);
              break;
            case STATUS_MESSAGE:
              ret = ping(msg);
              break;
            case DUMP_PLAN_MESSAGE:
              ret = dump_plan(msg);
              break;
            case CLIENT_CMD_MESSAGE:
              ret = client_control_cmd(msg);
              break;
            case REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE:
              {
              BaseTaskMessage* packet = dynamic_cast<BaseTaskMessage*>(msg);
              if (0 == packet->get_seqno())
                ret = resolve_block_version_conflict(msg);
              else
                ret = layout_manager_.get_client_request_server().handle(msg);
              }
              break;
            case REQ_GET_FAMILY_INFO_MESSAGE:
              ret = get_family_info(msg);
              break;
            case REPAIR_BLOCK_MESSAGE_V2:
              ret = repair(msg);
              break;
            case DS_APPLY_BLOCK_MESSAGE:
              ret = apply_block(msg);
              break;
            case DS_APPLY_BLOCK_FOR_UPDATE_MESSAGE:
              ret = apply_block_for_update(msg);
              break;
            case DS_GIVEUP_BLOCK_MESSAGE:
              ret = giveup_block(msg);
              break;
            default:
              ret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(WARN, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != ret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed, pcode: %d", pcode);
          }
        }
      }
      return bret;
    }

    int NameServer::callback(common::NewClient* client)
    {
      int32_t ret = (NULL != client) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
          ret = ((NULL != sresponse) && (fresponse != NULL)) ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
        if (TFS_SUCCESS == ret)
        {
          tbnet::Packet* packet = client->get_source_msg();
          assert(NULL != packet);
          int32_t pcode = packet->getPCode();
          if (REMOVE_BLOCK_MESSAGE_V2 == pcode)
          {
            if (!sresponse->empty())
            {
              NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
              for (; iter != sresponse->end(); ++iter)
              {
                if (iter->second.second->getPCode() == STATUS_MESSAGE)
                {
                  RemoveBlockMessageV2* msg = dynamic_cast<RemoveBlockMessageV2*>(packet);
                  StatusMessage* sm = dynamic_cast<StatusMessage*>(iter->second.second);
                  TBSYS_LOG(INFO, "remove block: %"PRI64_PREFIX"u %s", msg->get_block_id(),
                    STATUS_MESSAGE_OK == sm->get_status() ? "successful" : "failure");
                }
              }
           }
          }
        }
      }
      return ret;
    }

    int NameServer::open(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == GET_BLOCK_INFO_MESSAGE_V2)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        GetBlockInfoMessageV2* message = dynamic_cast<GetBlockInfoMessageV2*> (msg);
        GetBlockInfoRespMessageV2* result_msg = new (std::nothrow)GetBlockInfoRespMessageV2();
        const uint64_t block_id = message->get_block_id();
        uint64_t lease_id = common::INVALID_LEASE_ID;
        const int32_t  mode     = message->get_mode();
        const int32_t  flag     = message->get_flag();
        const time_t now = Func::get_monotonic_time();
        const uint64_t ipport = msg->get_connection()->getServerId();
        BlockMeta& meta = result_msg->get_block_meta();
        common::ArrayHelper<uint64_t> servers(MAX_REPLICATION_NUM, meta.ds_);

        ret = layout_manager_.get_client_request_server().open(block_id, mode, flag, now, lease_id, servers,
                meta.family_info_);
        if (TFS_SUCCESS == ret)
        {
          meta.lease_id_ = lease_id;
          meta.block_id_ = block_id;
          meta.size_ = servers.get_array_index();
          ret = message->reply(result_msg);
        }
        else
        {
          result_msg->free();
          if (EXIT_ACCESS_PERMISSION_ERROR == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NAMESERVER_ONLY_READ,
                  "current nameserver only read, %s", tbsys::CNetUtil::addrToString(ipport).c_str());
          }
          else
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
                  "got error, when get block: %"PRI64_PREFIX"u mode: %d, result: %d information, %s",
                  block_id, mode, ret,tbsys::CNetUtil::addrToString(ipport).c_str());
          }
        }
      }
      return ret;
    }

    int NameServer::batch_open(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == BATCH_GET_BLOCK_INFO_MESSAGE_V2)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BatchGetBlockInfoMessageV2* message = dynamic_cast<BatchGetBlockInfoMessageV2*>(msg);
        BatchGetBlockInfoRespMessageV2* reply = new (std::nothrow)BatchGetBlockInfoRespMessageV2();
        ArrayHelper<uint64_t> blocks(message->get_size(), message->get_block_ids(), message->get_size());
        ArrayHelper<BlockMeta> meta(MAX_BATCH_SIZE, reply->get_block_metas());
        const int32_t  mode     = message->get_mode();
        const int32_t  flag     = message->get_flag();
        for (int64_t index = 0; index < blocks.get_array_index(); ++index)
        {
          meta.push_back(BlockMeta());
          BlockMeta* bm = meta.at(index);
          bm->block_id_ = *blocks.at(index);
        }
        ret = layout_manager_.get_client_request_server().batch_open(mode, flag, meta);
        if (TFS_SUCCESS == ret)
        {
          reply->set_size(meta.get_array_index());
          TBSYS_LOG(DEBUG, "batch open return %"PRI64_PREFIX"d blocks meta.", meta.get_array_index());
          ret = message->reply(reply);
        }
        else
        {
          reply->free();
          if(EXIT_NO_DATASERVER == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NO_DATASERVER,
                "not found dataserver, dataserver size equal 0");
          }
          else if (EXIT_ACCESS_PERMISSION_ERROR == ret)
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), EXIT_NAMESERVER_ONLY_READ,
                "current nameserver only read");
          }
          else
          {
            ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
                "batch get get block information error, mode: %d, ret: %d", mode, ret);
          }
        }
      }
      return ret;
    }

    int NameServer::show_server_information(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == SHOW_SERVER_INFORMATION_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(msg);
        ShowServerInformationMessage* resp = new (std::nothrow)ShowServerInformationMessage();
        assert(NULL != resp);
        SSMScanParameter& param = resp->get_param();
        param.addition_param1_ = message->get_param().addition_param1_;
        param.addition_param2_ = message->get_param().addition_param2_;
        param.start_next_position_ = message->get_param().start_next_position_;
        param.should_actual_count_ = message->get_param().should_actual_count_;
        param.child_type_ = message->get_param().child_type_;
        param.type_ = message->get_param().type_;
        param.end_flag_ = message->get_param().end_flag_;
        ret = layout_manager_.scan(param);
        if (TFS_SUCCESS == ret)
          ret = message->reply(resp);
        else
          resp->free();
      }
      return ret;
    }

    int NameServer::resolve_block_version_conflict(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == REQ_RESOLVE_BLOCK_VERSION_CONFLICT_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ResolveBlockVersionConflictMessage* message = dynamic_cast<ResolveBlockVersionConflictMessage*>(msg);
        ArrayHelper<std::pair<uint64_t, BlockInfoV2> > members(message->get_size(), message->get_members(),message->get_size());
        ret = layout_manager_.get_client_request_server().resolve_block_version_conflict(message->get_block(), members);
        ResolveBlockVersionConflictResponseMessage* reply_msg = new ResolveBlockVersionConflictResponseMessage();
        reply_msg->set_status(ret);
        ret = message->reply(reply_msg);
      }
      return ret;
    }

    int NameServer::ping(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == STATUS_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        StatusMessage* stmsg = dynamic_cast<StatusMessage*>(msg);
        if (STATUS_MESSAGE_PING == stmsg->get_status())
        {
          StatusMessage* reply_msg = dynamic_cast<StatusMessage*>(get_packet_factory()->createPacket(STATUS_MESSAGE));
          reply_msg->set_message(STATUS_MESSAGE_PING);
          ret = msg->reply(reply_msg);
        }
        else
        {
          ret = EXIT_GENERAL_ERROR;
        }
      }
      return ret;
   }

    int NameServer::dump_plan(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == DUMP_PLAN_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DumpPlanResponseMessage* rmsg = new DumpPlanResponseMessage();
        layout_manager_.get_client_request_server().dump_plan(rmsg->get_data());
        ret = msg->reply(rmsg);
      }
      return ret;
    }

    int NameServer::client_control_cmd(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == CLIENT_CMD_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char buf[256] = {'\0'};
        ClientCmdMessage* message = dynamic_cast<ClientCmdMessage*>(msg);
        ret = layout_manager_.get_client_request_server().handle_control_cmd(message->get_cmd_info(), msg, 256, buf);
        if (TFS_SUCCESS == ret)
          ret = msg->reply(new StatusMessage(STATUS_MESSAGE_OK, buf));
        else
          ret = msg->reply_error_packet(TBSYS_LOG_LEVEL(INFO), STATUS_MESSAGE_ERROR, buf);
      }
      return ret;
    }

    int NameServer::initialize_ns_global_info()
    {
      int32_t ret = GFactory::initialize(get_timer());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "%s", "GFactory initialize error, must be exit");
      }
      else
      {
        const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_IP_ADDR_LIST);
        ret = NULL == ns_ip ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "%s", "initialize ns ip is null or ns port <= 0, must be exit");
        }

        if (TFS_SUCCESS == ret)
        {
          std::vector < uint32_t > ns_ip_list;
          char buffer[256];
          strncpy(buffer, ns_ip, 256);
          char *t = NULL;
          char *s = buffer;
          while ((t = strsep(&s, "|")) != NULL)
          {
            ns_ip_list.push_back(tbsys::CNetUtil::getAddr(t));
          }
          ret = 2U != ns_ip_list.size() ? EXIT_GENERAL_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(INFO, "%s", "must have two ns,check your ns' list");
          }
          else
          {
            bool flag = false;
            uint32_t local_ip = 0;
            std::vector<uint32_t>::iterator iter = ns_ip_list.begin();
            for (; iter != ns_ip_list.end() && !flag; ++iter)
            {
              if ((flag = Func::is_local_addr((*iter))))
                local_ip = (*iter);
            }
            ret = flag ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "ip list: %s not in %s, must be exit", ns_ip, get_dev());
            }
            else
            {
              NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
              ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
              ngi.peer_status_ = NS_STATUS_UNINITIALIZE;
              ngi.vip_ = Func::get_addr(get_ip_addr());
              for (iter = ns_ip_list.begin();iter != ns_ip_list.end(); ++iter)
              {
                if (local_ip == (*iter))
                  ngi.owner_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
                else
                  ngi.peer_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
              }
              ngi.switch_role(true);
            }
          }
        }
      }
      return ret;
    }

    int NameServer::do_master_msg_helper(common::BasePacket* packet)
    {
      int32_t ret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          //receive all owner check message , master and slave heart message, dataserver heart message
          if (pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != SET_DATASERVER_MESSAGE
            && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            ret = ngi.owner_status_ < NS_STATUS_INITIALIZED? common::TFS_ERROR : common::TFS_SUCCESS;
          }
        }
      }
      return ret;
    }

    int NameServer::do_slave_msg_helper(common::BasePacket* packet)
    {
      int32_t ret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == ret)
        {
          if (pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != SET_DATASERVER_MESSAGE
            && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            if (ngi.owner_status_ < NS_STATUS_INITIALIZED)
            {
              ret = common::TFS_ERROR;
            }
            else
            {
              if (pcode != REPLICATE_BLOCK_MESSAGE
                && pcode != BLOCK_COMPACT_COMPLETE_MESSAGE
                && pcode != OPLOG_SYNC_MESSAGE
                && pcode != GET_BLOCK_INFO_MESSAGE
                && pcode != GET_BLOCK_INFO_MESSAGE_V2
                && pcode != SET_DATASERVER_MESSAGE
                && pcode != REQ_REPORT_BLOCKS_TO_NS_MESSAGE
                && pcode != BATCH_GET_BLOCK_INFO_MESSAGE
                && pcode != SHOW_SERVER_INFORMATION_MESSAGE
                && pcode != REQ_GET_FAMILY_INFO_MESSAGE)
              {
                ret = common::TFS_ERROR;
              }
            }
          }
        }
      }
      return ret;
    }

    int NameServer::get_family_info(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == REQ_GET_FAMILY_INFO_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (common::TFS_SUCCESS == ret)
      {
        GetFamilyInfoMessage* message = dynamic_cast<GetFamilyInfoMessage*>(msg);
        int32_t mode = message->get_mode();
        int64_t family_id = message->get_family_id();
        int32_t family_aid_info = 0;
        uint64_t ipport = msg->get_connection()->getServerId();
        GetFamilyInfoResponseMessage* reply_msg = new GetFamilyInfoResponseMessage();
        ArrayHelper<std::pair<uint64_t, uint64_t> > members(MAX_MARSHALLING_NUM, reply_msg->get_members());
        ret = layout_manager_.get_client_request_server().open(family_id, mode, family_aid_info, members);
        if (TFS_SUCCESS == ret)
        {
          reply_msg->set_family_id(family_id);
          reply_msg->set_family_aid_info(family_aid_info);
          ret = message->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
          ret = message->reply_error_packet(TBSYS_LOG_LEVEL(INFO), ret,
              "got error, when get family: %"PRI64_PREFIX"d mode: %d, result: %d information, %s",
              family_id, mode, ret, tbsys::CNetUtil::addrToString(ipport).c_str());
        }
      }
      return ret;
    }

    int NameServer::repair(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == REPAIR_BLOCK_MESSAGE_V2)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char error_msg[512] = {'\0'};
        RepairBlockMessageV2* message = dynamic_cast<RepairBlockMessageV2*>(msg);
        const uint64_t block_id = message->get_block_id();
        const uint64_t server   = message->get_server_id();
        const int64_t  family_id = message->get_family_id();
        const int32_t  type     = message->get_repair_type();
        ret = layout_manager_.repair(error_msg, 512, block_id, server, family_id, type, Func::get_monotonic_time());
        ret = message->reply(new StatusMessage(ret, error_msg));
      }
      return ret;
    }

    int NameServer::apply_block(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == DS_APPLY_BLOCK_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DsApplyBlockMessage* ab_msg = dynamic_cast<DsApplyBlockMessage*>(msg);
        uint64_t server   = ab_msg->get_server_id();
        int32_t MAX_COUNT = ab_msg->get_count();
        MAX_COUNT = std::min(MAX_COUNT, MAX_WRITABLE_BLOCK_COUNT);
        DsApplyBlockResponseMessage* reply_msg = new (std::nothrow)DsApplyBlockResponseMessage();
        assert(NULL != reply_msg);
        ArrayHelper<BlockLease> output(MAX_COUNT, reply_msg->get_block_lease());
        ret = layout_manager_.get_client_request_server().apply_block(server, output);
        if (TFS_SUCCESS == ret)
        {
          reply_msg->set_size(output.get_array_index());
          ret = ab_msg->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
        }
      }
      return ret;
    }

    int NameServer::apply_block_for_update(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == DS_APPLY_BLOCK_FOR_UPDATE_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DsApplyBlockForUpdateMessage* ab_msg = dynamic_cast<DsApplyBlockForUpdateMessage*>(msg);
        uint64_t server   = ab_msg->get_server_id();
        DsApplyBlockForUpdateResponseMessage* reply_msg = new (std::nothrow)DsApplyBlockForUpdateResponseMessage();
        assert(NULL != reply_msg);
        const int32_t MAX_COUNT = 1;
        BlockLease lease[MAX_COUNT];
        lease[0].block_id_ = ab_msg->get_block_id();
        ArrayHelper<BlockLease> output(MAX_COUNT, lease);
        ret = layout_manager_.get_client_request_server().apply_block_for_update(server,output);
        if (TFS_SUCCESS == ret)
        {
          reply_msg->set_block_lease(lease[0]);
          ret = ab_msg->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
        }
      }
      return ret;
    }

    int NameServer::giveup_block(common::BasePacket* msg)
    {
      int32_t ret = ((NULL != msg) && (msg->getPCode() == DS_GIVEUP_BLOCK_MESSAGE)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        DsGiveupBlockMessage* ab_msg = dynamic_cast<DsGiveupBlockMessage*>(msg);
        uint64_t server   = ab_msg->get_server_id();
        int32_t MAX_COUNT = ab_msg->get_size();
        assert(MAX_COUNT <= MAX_WRITABLE_BLOCK_COUNT);
        DsGiveupBlockResponseMessage* reply_msg = new (std::nothrow)DsGiveupBlockResponseMessage();
        assert(NULL != reply_msg);
        ArrayHelper<BlockInfoV2> input(MAX_COUNT, ab_msg->get_block_infos(), MAX_COUNT);
        ArrayHelper<BlockLease> output(MAX_COUNT, reply_msg->get_block_lease());
        ret = layout_manager_.get_client_request_server().giveup_block(server, input, output);
        if (TFS_SUCCESS == ret)
        {
          reply_msg->set_size(output.get_array_index());
          ret = ab_msg->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
        }
      }
      return ret;
    }

    int ns_async_callback(common::NewClient* client)
    {
      NameServer* service = dynamic_cast<NameServer*>(BaseMain::instance());
      int32_t ret = (NULL != service) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = service->callback(client);
      }
      return ret;
    }
  } /** nameserver **/
}/** tfs **/

