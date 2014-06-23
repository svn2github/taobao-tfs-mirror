/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: kvrootserver.cpp  $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 */

#include <Service.h>
#include <Memory.hpp>
#include <iterator>
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/local_packet.h"
#include "common/directory_op.h"
#include "common/status_message.h"
#include "common/client_manager.h"
#include "message/kv_rts_message.h"
#include "kv_root_server.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace kvrootserver
  {
    KvRootServer::KvRootServer():
      rt_ms_heartbeat_handler_(*this)
    {

    }

    KvRootServer::~KvRootServer()
    {

    }

    int KvRootServer::initialize(int /*argc*/, char* /*argv*/[])
    {//TODO KV
      int32_t iret =  SYSPARAM_KVRTSERVER.initialize();
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize kvrootserver parameter error, must be exit");
        iret = EXIT_GENERAL_ERROR;
      }

      //initialize kvmetaserver manager
      if (TFS_SUCCESS == iret)
      {
				iret = manager_.initialize();
				if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "kv meta server manager initialize error, iret: %d, must be exit", iret);
        }
				/*
        const char* work_dir = get_work_dir();
        iret = NULL == work_dir ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          std::string table_dir(work_dir + std::string("/kvrootserver"));
          iret = DirectoryOp::create_full_path(table_dir.c_str()) ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            std::string table_file_path(table_dir + std::string("/table"));
            iret = manager_.initialize(table_file_path);
            if (TFS_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "kv meta server manager initialize error, iret: %d, must be exit", iret);
            }
          }
        }
        */
      }

      //initialize kvmetaserver ==> kvrootserver heartbeat
      if (TFS_SUCCESS == iret)
      {//TODO
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_KVROOTSERVER, CONF_HEART_THREAD_COUNT, 1);
        ms_rs_heartbeat_workers_.setThreadParameter(heart_thread_count, &rt_ms_heartbeat_handler_, this);
        ms_rs_heartbeat_workers_.start();
      }
      return iret;
    }

    int KvRootServer::destroy_service()
    {
      ms_rs_heartbeat_workers_.stop();
      ms_rs_heartbeat_workers_.wait();
      manager_.destroy();
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode KvRootServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode hret = tbnet::IPacketHandler::FREE_CHANNEL;
      bool bret = NULL != connection && NULL != packet;
      if (bret)
      {
        TBSYS_LOG(DEBUG, "receive pcode : %d", packet->getPCode());
        if (!packet->isRegularPacket())
        {
          bret = false;
          TBSYS_LOG(WARN, "control packet, pcode: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
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
          int32_t iret = common::TFS_SUCCESS;

          if (common::TFS_SUCCESS == iret)
          {
            hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            switch (pcode)
            {
            case REQ_KV_RT_MS_KEEPALIVE_MESSAGE:
              ms_rs_heartbeat_workers_.push(bpacket, 0/* no limit */, false/* no block */);
              break;
            default:
              if (!main_workers_.push(bpacket, work_queue_size_))
              {
                bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
                bpacket->free();
              }
              break;
            }
          }
          else
          {
            bpacket->free();
            TBSYS_LOG(WARN, "the msg: %d will be ignored", pcode);
          }
        }
      }
      return hret;
    }

    /** handle packet*/
    bool KvRootServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t iret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case REQ_KV_RT_GET_TABLE_MESSAGE:
              iret = get_tables(msg);
              break;
            default:
              iret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != iret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "execute message failed, pcode: %d", pcode);
          }
        }
      }
      return bret;
    }

		bool KvRootServer::KeepAliveIPacketQueueHandlerHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool bret = packet != NULL;
      if (bret)
      {
        //if return TFS_SUCCESS, packet had been delete in this func
        //if handlePacketQueue return true, tbnet will delete this packet
        assert(packet->getPCode() == REQ_KV_RT_MS_KEEPALIVE_MESSAGE);
        manager_.kv_ms_keepalive(dynamic_cast<BasePacket*>(packet));
      }
      return bret;
    }

    int KvRootServer::kv_ms_keepalive(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        KvRtsMsHeartMessage* msg = dynamic_cast<KvRtsMsHeartMessage*>(packet);
        common::KvMetaServerBaseInformation& base_info = msg->get_ms();
        iret = manager_.keepalive(base_info);

				KvRtsMsHeartResponseMessage* reply_msg = new(std::nothrow) KvRtsMsHeartResponseMessage();
				assert(NULL != reply_msg);
        int32_t tmp = SYSPARAM_KVRTSERVER.kv_mts_rts_heart_interval_;
        //reply_msg->set_time(SYSPARAM_KVRTSERVER.kv_mts_rts_heart_interval_);
        TBSYS_LOG(DEBUG, "kv_mts_rts_heart_interval_: %d is ", SYSPARAM_KVRTSERVER.kv_mts_rts_heart_interval_);
        reply_msg->set_time(tmp);
        iret = packet->reply(reply_msg);
      }
      return iret;
    }

    int KvRootServer::get_tables(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        GetTableFromKvRtsResponseMessage* reply_msg = new GetTableFromKvRtsResponseMessage();
        iret = manager_.get_table(reply_msg->get_mutable_table());
        if (TFS_SUCCESS == iret)
        {
          iret = packet->reply(reply_msg);
        }
        else
        {
          tbsys::gDelete(reply_msg);
        }
      }
      return iret;
    }
  } /** kvrootserver **/
}/** tfs **/

