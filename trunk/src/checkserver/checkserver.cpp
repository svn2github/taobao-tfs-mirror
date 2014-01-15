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

#include <unistd.h>
#include <iostream>
#include <string>

#include "common/directory_op.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/base_packet_factory.h"
#include "common/base_packet_streamer.h"
#include "message/checkserver_message.h"
#include "common/client_manager.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tbutil;
using namespace tbsys;

#include "default_server_helper.h"
#include "checkserver.h"

namespace tfs
{
  namespace checkserver
  {
    CheckServer::CheckServer():
      check_manager_(*this, new DefaultServerHelper())
    {

    }

    CheckServer::~CheckServer()
    {

    }

    int CheckServer::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);

      // load config file
      int32_t ret = SYSPARAM_CHECKSERVER.initialize(config_file_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load dataserver parameter failed: %d", ret);
        ret = EXIT_GENERAL_ERROR;
      }
      else
      {
        // start check thread
        check_thread_ = new (std::nothrow)RunCheckThreadHelper(*this);
        assert(0 != check_thread_);
      }

      return ret;
    }

    int CheckServer::destroy_service()
    {
      check_manager_.stop_check();
      if (0 != check_thread_)
      {
        check_thread_->join();
      }
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode CheckServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
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
            default:
              if (!main_workers_.push(bpacket, work_queue_size_))
              {
                bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR,
                    "%s, task message beyond max queue size, discard", get_ip_addr());
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
    bool CheckServer::handlePacketQueue(tbnet::Packet *packet, void *args)
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
            case REPORT_CHECK_BLOCK_RESPONSE_MESSAGE:
              iret = check_manager_.handle(packet);
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

    void CheckServer::RunCheckThreadHelper::run()
    {
      service_.run_check();
    }
  }
}

