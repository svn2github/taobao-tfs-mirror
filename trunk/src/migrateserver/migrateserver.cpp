/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrateservice.cpp 1000 2013-09-02 09:40:09Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <Memory.hpp>
#include "common/client_manager.h"
#include "common/func.h"
#include "common/config_item.h"
#include "common/directory_op.h"

#include "ms_define.h"
#include "migrateserver.h"

namespace tfs
{
  namespace migrateserver
  {
    using namespace std;
    using namespace tfs::common;
    using namespace tfs::message;
    MigrateService::MigrateService():
      timeout_thread_(0),
      manager_(NULL)
    {

    }

    MigrateService::~MigrateService()
    {
      tbsys::gDelete(manager_);
    }

    int MigrateService::initialize(int argc, char* argv[])
    {
      UNUSED(argc);
      UNUSED(argv);
      srandom(time(NULL));
      const char* ipaddr = TBSYS_CONFIG.getString(CONF_SN_MIGRATESERVER, CONF_IP_ADDR, "");
      const int32_t port = TBSYS_CONFIG.getInt(CONF_SN_MIGRATESERVER, CONF_PORT, 0);
      int32_t ret = (NULL != ipaddr && port > 1024 && port < 65535) ? TFS_SUCCESS : EXIT_SYSTEM_PARAMETER_ERROR;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "%s not set (nameserver vip) ipaddr: %s or port: %d, must be exit", argv[0], NULL == ipaddr ? "null" : ipaddr, port);
      }
      if (TFS_SUCCESS == ret)
      {
        const uint64_t ns_vip_port = tbsys::CNetUtil::strToAddr(ipaddr, port);
        const char* percent = TBSYS_CONFIG.getString(CONF_SN_MIGRATESERVER, CONF_BALANCE_PERCENT, "0.05");
        const double balance_percent = strtod(percent, NULL);
        const int32_t TWO_MONTH = 2 * 31 * 86400;
        const int64_t hot_time_range = TBSYS_CONFIG.getInt(CONF_SN_MIGRATESERVER, CONF_HOT_TIME_RANGE, TWO_MONTH);
        const char* str_system_disk_access_ratio = TBSYS_CONFIG.getString(CONF_SN_MIGRATESERVER, CONF_SYSTEM_DISK_ACCESS_RATIO, "");
        const char* str_full_disk_access_ratio = TBSYS_CONFIG.getString(CONF_SN_MIGRATESERVER, CONF_FULL_DISK_ACCESS_RATIO, "");
        std::vector<string> ratios[2];
        Func::split_string(str_full_disk_access_ratio, ':', ratios[0]);
        Func::split_string(str_system_disk_access_ratio, ':', ratios[1]);
        int32_t ret = (5U == ratios[0].size() && 5U == ratios[1].size()) ? TFS_SUCCESS : EXIT_SYSTEM_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          AccessRatio ar[2];
          for (int32_t i = 0; i < 2; ++i)
          {
            ar[i].last_access_time_ratio = atoi(ratios[i][0].c_str());
            ar[i].read_ratio = atoi(ratios[i][1].c_str());
            ar[i].write_ratio = atoi(ratios[i][2].c_str());
            ar[i].update_ratio = atoi(ratios[i][3].c_str());
            ar[i].unlink_ratio = atoi(ratios[i][4].c_str());
          }
          manager_ = new (std::nothrow)MigrateManager(ns_vip_port, balance_percent, hot_time_range, ar[0], ar[1]);
          assert(NULL != manager_);
          manager_->initialize();
        }
      }
      if (TFS_SUCCESS == ret)
      {
        timeout_thread_  = new (std::nothrow)TimeoutThreadHelper(*this);
        assert(0 != timeout_thread_);
      }
      TBSYS_LOG(INFO, "initialize migrate server : %s, ret: %d", TFS_SUCCESS == ret ? "successful" : "failed", ret);
      return ret;
    }

    int MigrateService::destroy_service()
    {
      migrateserver::MsRuntimeGlobalInformation& srgi= migrateserver::MsRuntimeGlobalInformation::instance();
      srgi.is_destroy_ = true;
      if (0 != timeout_thread_)
      {
        timeout_thread_->join();
        timeout_thread_ = 0;
      }
      if (NULL != manager_)
      {
        manager_->destroy();
      }
      return TFS_SUCCESS;
    }

    void MigrateService::rotate_(time_t& last_rotate_log_time, time_t now, time_t zonesec)
    {
      if ((now % 86400 >= zonesec)
          && (now % 86400 < zonesec + 300)
          && (last_rotate_log_time < now - 600))
      {
        last_rotate_log_time = now;
        TBSYS_LOGGER.rotateLog(NULL);
      }
    }

    void MigrateService::timeout_()
    {
      tzset();
      const int32_t MAX_SLEEP_TIME_US = 1 * 1000 * 1000;//1s
      time_t zonesec = 86400 + timezone, now = 0, last_rotate_log_time = 0;
      migrateserver::MsRuntimeGlobalInformation& srgi= migrateserver::MsRuntimeGlobalInformation::instance();
      while (!srgi.is_destroyed())
      {
        now = time(NULL);

        //rotate log
        rotate_(last_rotate_log_time, now, zonesec);

        manager_->timeout(Func::get_monotonic_time());

        usleep(MAX_SLEEP_TIME_US);
      }
    }

    int MigrateService::keepalive_(common::BasePacket* packet)
    {
      int32_t ret = (NULL != packet && packet->getPCode() == REQ_MIGRATE_DS_HEARTBEAT_MESSAGE) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        assert(NULL != manager_);
        MigrateDsHeartMessage* msg = dynamic_cast<MigrateDsHeartMessage*>(packet);
        assert(NULL != msg);
        const common::DataServerStatInfo& info = msg->get_dataserver_information();
        ret = manager_->keepalive(info);
        MigrateDsHeartResponseMessage* reply_msg = new (std::nothrow)MigrateDsHeartResponseMessage();
        assert(NULL != reply_msg);
        reply_msg->set_ret_value(ret);
        TBSYS_LOG(INFO, "%s keepalive %s, ret: %d, total_capacity: %"PRI64_PREFIX"d, use_capacity: %"PRI64_PREFIX"d, block_count: %d, disk type: %d",
            tbsys::CNetUtil::addrToString(info.id_).c_str(), TFS_SUCCESS == ret ? "successful" : "failed", ret,
            info.total_capacity_, info.use_capacity_, info.block_count_, info.type_);
        ret = msg->reply(reply_msg);
      }
      return ret;
    }

    bool MigrateService::check_response(common::NewClient* client)
    {
      bool all_success = (NULL != client);
      NewClient::RESPONSE_MSG_MAP* sresponse = NULL;
      NewClient::RESPONSE_MSG_MAP* fresponse = NULL;
      if (all_success)
      {
        sresponse = client->get_success_response();
        fresponse = client->get_fail_response();
        all_success = ((NULL != sresponse) && (NULL != fresponse));
      }

      if (all_success)
      {
        all_success = (sresponse->size() == client->get_send_id_sign().size());
        NewClient::RESPONSE_MSG_MAP::iterator iter = sresponse->begin();
        for ( ; all_success && (iter != sresponse->end()); iter++)
        {
          tbnet::Packet* rmsg = iter->second.second;
          all_success = (NULL != rmsg);
          if (all_success)
          {
            all_success = (STATUS_MESSAGE == rmsg->getPCode());
          }
          if (all_success)
          {
            StatusMessage* smsg = dynamic_cast<StatusMessage*>(rmsg);
            all_success = STATUS_MESSAGE_OK == smsg->get_status();
          }
        }
      }
      return all_success;
    }

    int MigrateService::callback(common::NewClient* client)
    {
      int32_t ret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbnet::Packet* packet = client->get_source_msg();
        ret = (NULL != packet)? TFS_SUCCESS: TFS_ERROR;
        if (TFS_SUCCESS == ret)
        {
          //bool all_success = check_response(client);
          //int32_t pcode = packet->getPCode();
          //common::BasePacket* bpacket= dynamic_cast<BasePacket*>(packet);
        }
      }
      return ret;
    }

    tbnet::IPacketHandler::HPRetCode MigrateService::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
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
          migrateserver::MsRuntimeGlobalInformation& srgi= migrateserver::MsRuntimeGlobalInformation::instance();
          if (!srgi.is_destroyed())
          {
            bret = push(bpacket, false);
            if (bret)
              hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            else
            {
              bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, task message beyond max queue size, discard", get_ip_addr());
              bpacket->free();
            }
          }
          else
          {
            bpacket->reply_error_packet(TBSYS_LOG_LEVEL(WARN), STATUS_MESSAGE_ACCESS_DENIED,
                "you client %s access been denied. msgtype: %d", tbsys::CNetUtil::addrToString(
                  connection->getPeerId()).c_str(), packet->getPCode());
            // packet denied, must free
            bpacket->free();
          }
        }
      }
      return hret;
    }

    bool MigrateService::handlePacketQueue(tbnet::Packet* packet, void* args)
    {
      bool bret = BaseService::handlePacketQueue(packet, args);
      if (bret)
      {
        int32_t pcode = packet->getPCode();
        int32_t ret = LOCAL_PACKET == pcode ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          switch (pcode)
          {
            case REQ_MIGRATE_DS_HEARTBEAT_MESSAGE:
              ret = keepalive_(dynamic_cast<common::BasePacket*>(packet));
              break;
            default:
              ret = EXIT_UNKNOWN_MSGTYPE;
              break;
          }
          if (common::TFS_SUCCESS != ret)
          {
            common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed, pcode: %d", pcode);
          }
        }
      }
      return bret;
    }

    void MigrateService::TimeoutThreadHelper::run()
    {
      service_.timeout_();
    }

    int ms_async_callback(common::NewClient* client)
    {
      MigrateService* service = dynamic_cast<MigrateService*>(BaseMain::instance());
      int32_t ret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = service->callback(client);
      }
      return ret;
    }
  }/** end namespace migrateserver **/
}/** end namespace tfs **/
