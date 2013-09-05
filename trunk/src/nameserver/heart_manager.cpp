/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_manager.cpp 983 2011-10-31 09:59:33Z duanfei $
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
#include <tbsys.h>
#include <Memory.hpp>
#include <Mutex.h>
#include "ns_define.h"
#include "nameserver.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "common/client_manager.h"
#include "message/ds_lease_message.h"
#include "heart_manager.h"
#include "global_factory.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {
    HeartManagement::HeartManagement(NameServer& m) :
      manager_(m),
      packet_factory_(NULL),
      streamer_(NULL),
      transport_(NULL),
      keepalive_queue_header_(*this),
      report_block_queue_header_(*this)
    {

    }

    HeartManagement::~HeartManagement()
    {
      tbsys::gDelete(packet_factory_);
      tbsys::gDelete(streamer_);
      tbsys::gDelete(transport_);
    }

    int HeartManagement::initialize(const int32_t keepalive_thread_count,const int32_t report_block_thread_count, const int32_t port)
    {
      keepalive_threads_.setThreadParameter(keepalive_thread_count, &keepalive_queue_header_, this);
      report_block_threads_.setThreadParameter(report_block_thread_count, &report_block_queue_header_, this);
      keepalive_threads_.start();
      report_block_threads_.start();
      streamer_ = new (std::nothrow)common::BasePacketStreamer();
      assert(NULL != streamer_);
      packet_factory_ = new (std::nothrow)message::MessageFactory();
      assert(NULL != packet_factory_);
      transport_ = new (std::nothrow)tbnet::Transport();
      streamer_->set_packet_factory(packet_factory_);
      assert(NULL != transport_);
      char spec[32];
      snprintf(spec, 32, "tcp::%d", port);
      tbnet::IOComponent* com = transport_->listen(spec, streamer_, this);
      int32_t ret = (NULL == com) ? EXIT_NETWORK_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "listen port: %d fail", port);
      }
      else
      {
        transport_->start();
      }
      return ret;
    }

    void HeartManagement::wait_for_shut_down()
    {
      if (NULL != transport_)
        transport_->wait();
      keepalive_threads_.wait();
      report_block_threads_.wait();
    }

    void HeartManagement::destroy()
    {
      if (NULL != transport_)
        transport_->stop();
      keepalive_threads_.stop(true);
      report_block_threads_.stop(true);
    }

    tbnet::IPacketHandler::HPRetCode HeartManagement::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
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
          int32_t ret   = TFS_SUCCESS;
          int32_t pcode = bpacket->getPCode();
          hret = tbnet::IPacketHandler::KEEP_CHANNEL;
          switch (pcode)
          {
          case DS_APPLY_LEASE_MESSAGE:
          case DS_RENEW_LEASE_MESSAGE:
          case DS_GIVEUP_LEASE_MESSAGE:
            ret = keepalive_threads_.push(bpacket, SYSPARAM_NAMESERVER.keepalive_queue_size_, false) ? TFS_SUCCESS : EXIT_QUEUE_FULL_ERROR;
          break;
          case REQ_REPORT_BLOCKS_TO_NS_MESSAGE:
            ret = report_block_threads_.push(bpacket, SYSPARAM_NAMESERVER.report_block_queue_size_, false) ? TFS_SUCCESS : EXIT_QUEUE_FULL_ERROR;
          break;
          default:
            ret  = EXIT_UNKNOWN_MSGTYPE;
            hret = tbnet::IPacketHandler::FREE_CHANNEL;
          break;
          }
          if (TFS_SUCCESS != ret)
          {
            bpacket->reply_error_packet(TBSYS_LOG_LEVEL(ERROR),STATUS_MESSAGE_ERROR, "%s, unknown msg type: %d, discard, peer ip: %s", manager_.get_ip_addr(), pcode,
                tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str());
            bpacket->free();
          }
        }
      }
      return hret;
    }

    const char* HeartManagement::KeepAliveIPacketQueueHeaderHelper::transform_type_to_str_(const int32_t type)
    {
      return DS_APPLY_LEASE_MESSAGE == type ? "apply" : DS_RENEW_LEASE_MESSAGE == type ? "renew" : DS_GIVEUP_LEASE_MESSAGE == type ? "giveup" : "unknown";
    }

    // event handler
    bool HeartManagement::KeepAliveIPacketQueueHeaderHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      //if return TFS_SUCCESS, packet had been delete in this func
      //if handlePacketQueue return true, tbnet will delete this packet
      bool bret = (packet != NULL);
      if (bret)
      {
        TIMER_START();
        int32_t pcode = packet->getPCode();
        uint64_t server = INVALID_SERVER_ID;
        int32_t ret = (DS_APPLY_LEASE_MESSAGE == pcode
            || DS_RENEW_LEASE_MESSAGE == pcode
            || DS_GIVEUP_LEASE_MESSAGE == pcode) ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS == ret)
        {
          DsApplyLeaseMessage* msg = dynamic_cast<DsApplyLeaseMessage*>(packet);
          server = msg->get_ds_stat().id_;
          switch (pcode)
          {
            case DS_APPLY_LEASE_MESSAGE:
              ret = manager_.apply_(packet);
              break;
            case DS_RENEW_LEASE_MESSAGE:
              ret = manager_.renew_(packet);
              break;
            case DS_GIVEUP_LEASE_MESSAGE:
              ret = manager_.giveup_(packet);
              break;
            default :
              ret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(WARN, "unknown msg type: %d", pcode);
              break;
          }
        }
        if (TFS_SUCCESS != ret)
        {
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed, pcode: %d", pcode);
        }
        TIMER_END();
        TBSYS_LOG(INFO, "dataserver: %s %s %s consume times: %"PRI64_PREFIX"d(us), ret: %d", CNetUtil::addrToString(server).c_str(),
            transform_type_to_str_(pcode) ,TFS_SUCCESS == ret ? "successful" : "failed", TIMER_DURATION(), ret);
      }
      return bret;
    }

    bool HeartManagement::ReportBlockIPacketQueueHeaderHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      //if return TFS_SUCCESS, packet had been delete in this func
      //if handlePacketQueue return true, tbnet will delete this packet
      bool bret = (packet != NULL);
      if (bret)
      {
        TIMER_START();
        int32_t ret = TFS_SUCCESS;
        uint64_t server = INVALID_SERVER_ID;
        int32_t block_count = 0;
        int32_t pcode = packet->getPCode();
        switch (pcode)
        {
        case REQ_REPORT_BLOCKS_TO_NS_MESSAGE:
        {
          ReportBlocksToNsRequestMessage* msg = dynamic_cast<ReportBlocksToNsRequestMessage*>(packet);
          server = msg->get_server();
          block_count =msg->get_block_count();
          ret = manager_.report_block_(packet);
        }
        break;
        default :
         ret = EXIT_UNKNOWN_MSGTYPE;
         TBSYS_LOG(WARN, "unknown msg type: %d", pcode);
        break;
        }
        if (TFS_SUCCESS != ret)
        {
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), ret, "execute message failed, pcode: %d", pcode);
        }
        TIMER_END();
      }
      return bret;
    }

    int HeartManagement::apply_(tbnet::Packet* packet)
    {
      int32_t ret = (NULL != packet && DS_APPLY_LEASE_MESSAGE == packet->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        LayoutManager& layout_manager = manager_.get_layout_manager();
        ClientRequestServer& rs       = layout_manager.get_client_request_server();
        ServerManager& server_manager = layout_manager.get_server_manager();
        DsApplyLeaseMessage* msg = dynamic_cast<DsApplyLeaseMessage*>(packet);
        DataServerStatInfo& info = msg->get_ds_stat();
        DsApplyLeaseResponseMessage * reply_msg = new (std::nothrow)DsApplyLeaseResponseMessage();
        LeaseMeta& meta = reply_msg->get_lease_meta();
        meta.lease_id_ = info.id_;
        meta.ns_role_ = GFactory::get_runtime_info().get_role();
        meta.max_block_size_ = SYSPARAM_NAMESERVER.max_block_size_;
        meta.max_write_file_count_ = SYSPARAM_NAMESERVER.max_write_file_count_;
        server_manager.calc_single_process_max_network_bandwidth(
              meta.max_mr_network_bandwith_, meta.max_rw_network_bandwith_, info);
        ret = rs.apply(info, meta.lease_expire_time_,meta.lease_renew_time_, meta.renew_retry_times_, meta.renew_retry_timeout_);
        if (TFS_SUCCESS == ret)
        {
          ret = msg->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
        }
      }
      return ret;
    }

    int HeartManagement::renew_(tbnet::Packet* packet)
    {
      int32_t ret = (NULL != packet && DS_RENEW_LEASE_MESSAGE == packet->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        LayoutManager& layout_manager = manager_.get_layout_manager();
        ClientRequestServer& rs       = layout_manager.get_client_request_server();
        ServerManager& server_manager = layout_manager.get_server_manager();
        DsRenewLeaseMessage* msg = dynamic_cast<DsRenewLeaseMessage*>(packet);
        DataServerStatInfo& info = msg->get_ds_stat();
        DsRenewLeaseResponseMessage* reply_msg = new (std::nothrow)DsRenewLeaseResponseMessage();
        LeaseMeta& meta = reply_msg->get_lease_meta();
        meta.lease_id_ = info.id_;
        meta.ns_role_ = GFactory::get_runtime_info().get_role();
        ArrayHelper<BlockInfoV2> input(MAX_WRITABLE_BLOCK_COUNT, msg->get_block_infos(), msg->get_size());
        ArrayHelper<BlockLease>  output(MAX_WRITABLE_BLOCK_COUNT, reply_msg->get_block_lease());
        meta.max_block_size_ = SYSPARAM_NAMESERVER.max_block_size_;
        meta.max_write_file_count_ = SYSPARAM_NAMESERVER.max_write_file_count_;
        server_manager.calc_single_process_max_network_bandwidth(
              meta.max_mr_network_bandwith_, meta.max_rw_network_bandwith_, info);
        ret = rs.renew(input, info, output, meta.lease_expire_time_,meta.lease_renew_time_, meta.renew_retry_times_, meta.renew_retry_timeout_);
        if (TFS_SUCCESS == ret)
        {
          reply_msg->set_size(output.get_array_index());
          ret = msg->reply(reply_msg);
        }
        else
        {
          reply_msg->free();
        }
      }
      return ret;
    }

    int HeartManagement::giveup_(tbnet::Packet* packet)
    {
      int32_t ret = (NULL != packet && DS_GIVEUP_LEASE_MESSAGE == packet->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        LayoutManager& layout_manager = manager_.get_layout_manager();
        ClientRequestServer& rs       = layout_manager.get_client_request_server();
        DsGiveupLeaseMessage* msg = dynamic_cast<DsGiveupLeaseMessage*>(packet);
        DataServerStatInfo& info = msg->get_ds_stat();
        ArrayHelper<BlockInfoV2> input(MAX_WRITABLE_BLOCK_COUNT, msg->get_block_infos(), msg->get_size());
        ret = rs.giveup(input, info);
        if (TFS_SUCCESS == ret)
        {
          ret = msg->reply(new (std::nothrow)StatusMessage(STATUS_MESSAGE_OK));
        }
      }
      return ret;
    }

    int HeartManagement::report_block_(tbnet::Packet* packet)
    {
      TIMER_START();
      uint64_t server = 0;
      int32_t block_nums = 0, expire_nums = 0, result = 0;
      int32_t ret = (NULL != packet && REQ_REPORT_BLOCKS_TO_NS_MESSAGE == packet->getPCode()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        tbutil::Time begin = tbutil::Time::now();
        ReportBlocksToNsRequestMessage* message = dynamic_cast<ReportBlocksToNsRequestMessage*> (packet);
        assert(REQ_REPORT_BLOCKS_TO_NS_MESSAGE == packet->getPCode());
        server = message->get_server();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        ReportBlocksToNsResponseMessage* result_msg = new ReportBlocksToNsResponseMessage();
        result_msg->set_server(ngi.owner_ip_port_);
        time_t now = Func::get_monotonic_time();
        ArrayHelper<BlockInfoV2> blocks(message->get_block_count(), message->get_blocks_ext(), message->get_block_count());
			  result = ret = manager_.get_layout_manager().get_client_request_server().report_block(
          result_msg->get_blocks(), server, now, blocks);
        result_msg->set_status(HEART_MESSAGE_OK);
        block_nums = message->get_block_count();
        expire_nums= result_msg->get_blocks().size();
			  ret = message->reply(result_msg);
      }
      TIMER_END();
      TBSYS_LOG(INFO, "dataserver: %s report block %s, ret: %d, blocks: %d, cleanup family id blocks: %d,consume time: %"PRI64_PREFIX"u(us)",
         CNetUtil::addrToString(server).c_str(), TFS_SUCCESS == ret ? "successful" : "failed",
         result , block_nums, expire_nums, TIMER_DURATION());
      return ret;
    }

    NameServerHeartManager::NameServerHeartManager(LayoutManager& manager):
      manager_(manager),
      check_thread_(0)
    {

    }

    NameServerHeartManager::~NameServerHeartManager()
    {

    }

    int NameServerHeartManager::initialize()
    {
      work_thread_.setThreadParameter(1, this, this);
      work_thread_.start();
      check_thread_ = new CheckThreadHelper(*this);
      return TFS_SUCCESS;
   }

    int NameServerHeartManager::wait_for_shut_down()
    {
      work_thread_.wait();
      if (0 != check_thread_)
        check_thread_->join();
      return TFS_SUCCESS;
    }

    int NameServerHeartManager::destroy()
    {
      work_thread_.stop(true);
      return TFS_SUCCESS;
    }

    int NameServerHeartManager::push(common::BasePacket* message, const int32_t max_queue_size, const bool block)
    {
      return work_thread_.push(message, max_queue_size, block);
    }

    bool NameServerHeartManager::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool bret = (packet != NULL);
      if (bret)
      {
        common::BasePacket* message = dynamic_cast<common::BasePacket*>(packet);
        int32_t ret = TFS_SUCCESS;
        if (message->getPCode() == HEARTBEAT_AND_NS_HEART_MESSAGE)
          ret = keepalive_in_heartbeat_(message);
        else
          ret = keepalive_(message);
      }
      return bret;
    }

    int NameServerHeartManager::keepalive_(common::BasePacket* message)
    {
      int32_t ret = NULL != message ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = message->getPCode() == MASTER_AND_SLAVE_HEART_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS == ret)
        {
          MasterAndSlaveHeartMessage* msg = dynamic_cast<MasterAndSlaveHeartMessage*>(message);
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          bool login = msg->get_type() == NS_KEEPALIVE_TYPE_LOGIN;
          bool get_peer_role = msg->get_flags() == HEART_GET_PEER_ROLE_FLAG_YES;
          int64_t lease_id = msg->get_lease_id();
          MasterAndSlaveHeartResponseMessage* reply_msg = new MasterAndSlaveHeartResponseMessage();
          if (get_peer_role)
          {
            reply_msg->set_ip_port(ngi.owner_ip_port_);
            reply_msg->set_role(ngi.owner_role_);
            reply_msg->set_status(ngi.owner_status_);
          }
          else
          {
            time_t now = Func::get_monotonic_time();
            ret = ngi.keepalive(lease_id, msg->get_ip_port(), msg->get_role(),
                      msg->get_status(), msg->get_type(), now);
            reply_msg->set_ip_port(ngi.owner_ip_port_);
            reply_msg->set_role(ngi.owner_role_);
            reply_msg->set_status(ngi.owner_status_);
            if (TFS_SUCCESS == ret)
            {
              reply_msg->set_lease_id(lease_id);
              int32_t renew_lease_interval = SYSPARAM_NAMESERVER.heart_interval_ / 2;
              if (renew_lease_interval <= 0)
                renew_lease_interval = 1;
              reply_msg->set_lease_expired_time(SYSPARAM_NAMESERVER.heart_interval_);
              reply_msg->set_renew_lease_interval_time(renew_lease_interval);
              if (login)
              {
                manager_.get_oplog_sync_mgr().get_file_queue_thread()->update_queue_information_header();
              }
            }
          }
          if (TFS_SUCCESS != ret)
          {
            ngi.dump(TBSYS_LOG_LEVEL(INFO), "%s %s lease failed, ret: %d, lease_id: %"PRI64_PREFIX"d, status: %d",
              tbsys::CNetUtil::addrToString(msg->get_ip_port()).c_str(),
              NS_KEEPALIVE_TYPE_LOGIN == msg->get_type() ? "login" : "renew", ret, msg->get_lease_id(), msg->get_status());
          }
          ret = msg->reply(reply_msg);
        }
      }
      return ret;
    }

    void NameServerHeartManager::CheckThreadHelper::run()
    {
      try
      {
        manager_.check_();
      }
      catch(std::exception& e)
      {
        TBSYS_LOG(ERROR, "catch exception: %s", e.what());
      }
      catch(...)
      {
        TBSYS_LOG(ERROR, "%s", "catch exception, unknow message");
      }
    }

    void NameServerHeartManager::check_()
    {
      time_t now = 0;
      NsKeepAliveType keepalive_type_ = NS_KEEPALIVE_TYPE_LOGIN;
      int32_t sleep_time = SYSPARAM_NAMESERVER.heart_interval_ / 2;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        now = Func::get_monotonic_time();
        ns_role_establish_(ngi, now);

        if (ngi.is_master())
        {
          ns_check_lease_expired_(ngi, now);
        }
        else
        {
          keepalive_(sleep_time, keepalive_type_, ngi, now);
          if (!ngi.has_valid_lease(now))
            keepalive_type_ = NS_KEEPALIVE_TYPE_LOGIN;
          else
            keepalive_type_ = NS_KEEPALIVE_TYPE_RENEW;
        }

        if (sleep_time <= 0)
        {
          sleep_time =  SYSPARAM_NAMESERVER.heart_interval_ / 2;
          sleep_time = std::max(sleep_time, 1);
        }
        Func::sleep(sleep_time, ngi.destroy_flag_);
      }
      keepalive_type_ = NS_KEEPALIVE_TYPE_LOGOUT;
      keepalive_(sleep_time, keepalive_type_, ngi, now);
    }

    bool NameServerHeartManager::check_vip_(const NsRuntimeGlobalInformation& ngi) const
    {
      return Func::is_local_addr(ngi.vip_);
    }

    int NameServerHeartManager::ns_role_establish_(NsRuntimeGlobalInformation& ngi, const time_t now)
    {
      if (check_vip_(ngi))//vip is local ip
      {
        if (!ngi.is_master())//slave, switch
          switch_role_salve_to_master_(ngi, now);
      }
      else
      {
        if (ngi.is_master())
          switch_role_master_to_slave_(ngi, now);
      }
      return TFS_SUCCESS;
    }

    void NameServerHeartManager::switch_role_master_to_slave_(NsRuntimeGlobalInformation& ngi, const time_t now)
    {
      if (ngi.is_master())
      {
        manager_.switch_role(now);
        TBSYS_LOG(INFO, "nameserver switch, old role: master, current role: salve");
      }
    }

    void NameServerHeartManager::switch_role_salve_to_master_(NsRuntimeGlobalInformation& ngi, const time_t now)
    {
      if (!ngi.is_master())//slave, switch
      {
        int32_t ret = establish_peer_role_(ngi);
        if (EXIT_ROLE_ERROR != ret)
        {
          manager_.switch_role(now);
          TBSYS_LOG(INFO, "nameserver switch, old role: slave, current role: master");
        }
      }
    }

    int NameServerHeartManager::ns_check_lease_expired_(NsRuntimeGlobalInformation& ngi, const time_t now)
    {
      if (!ngi.has_valid_lease(now))
      {
        ngi.logout();
      }
      return TFS_SUCCESS;
    }

    int NameServerHeartManager::establish_peer_role_(NsRuntimeGlobalInformation& ngi)
    {
      MasterAndSlaveHeartMessage msg;
      msg.set_ip_port(ngi.owner_ip_port_);
      msg.set_role(ngi.owner_role_);
      msg.set_status(ngi.owner_status_);
      msg.set_lease_id(ngi.lease_id_);
      msg.set_flags(HEART_GET_PEER_ROLE_FLAG_YES);
      int32_t ret = TFS_ERROR;
      const int32_t TIMEOUT_MS = 500;
      const int32_t MAX_RETRY_COUNT = 2;
      NewClient* client = NULL;
      tbnet::Packet* response = NULL;
      for (int32_t i = 0; i < MAX_RETRY_COUNT && TFS_SUCCESS != ret; ++i)
      {
        client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ngi.peer_ip_port_, client, &msg, response, TIMEOUT_MS);
        if (TFS_SUCCESS == ret)
        {
          ret = response->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
          if (TFS_SUCCESS == ret)
          {
            MasterAndSlaveHeartResponseMessage* result = dynamic_cast<MasterAndSlaveHeartResponseMessage*>(response);
            bool conflict = result->get_role() == NS_ROLE_MASTER;//我将要变成master, 所以对方如果是master，那就有问题
            if (conflict)
            {
              ret = EXIT_ROLE_ERROR;
              ngi.dump(TBSYS_LOG_LEVEL(ERROR), "nameserver role coflict, own role: master, other role: master, must be exit");
              NameServer* service = dynamic_cast<NameServer*>(BaseMain::instance());
              if (NULL != service)
              {
                service->stop();
              }
            }
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;

    }
    int NameServerHeartManager::keepalive_(int32_t& sleep_time, NsKeepAliveType& type, NsRuntimeGlobalInformation& ngi, const time_t now)
    {
      MasterAndSlaveHeartMessage msg;
      msg.set_ip_port(ngi.owner_ip_port_);
      msg.set_role(ngi.owner_role_);
      msg.set_status(ngi.owner_status_);
      msg.set_status(ngi.owner_status_);
      msg.set_lease_id(ngi.lease_id_);
      msg.set_type(type);
      int32_t interval = SYSPARAM_NAMESERVER.heart_interval_ / 2;
      interval = std::max(interval, 1);

      int32_t ret = TFS_ERROR;
      int32_t MAX_RETRY_COUNT = 2;
      int32_t MAX_TIMEOUT_TIME_MS = 500;
      NewClient* client = NULL;
      tbnet::Packet* response = NULL;
      for (int32_t i = 0; i < MAX_RETRY_COUNT && TFS_SUCCESS != ret; ++i)
      {
        client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ngi.peer_ip_port_, client, &msg, response, MAX_TIMEOUT_TIME_MS);
        if (TFS_SUCCESS == ret)
        {
          ret = response->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
          if (TFS_SUCCESS == ret)
          {
            MasterAndSlaveHeartResponseMessage* result = dynamic_cast<MasterAndSlaveHeartResponseMessage*>(response);
            if (common::INVALID_LEASE_ID == result->get_lease_id())
            {
              ngi.logout();
            }
            else
            {
              ngi.renew(result->get_lease_id(), result->get_lease_expired_time(), now);
              sleep_time = result->get_renew_lease_interval_time();
              ngi.update_peer_info(result->get_ip_port(), result->get_role(), result->get_status());
            }
          }
        }
        NewClientManager::get_instance().destroy_client(client);
        if (TFS_SUCCESS != ret)
        {
          ngi.dump(TBSYS_LOG_LEVEL(INFO), "%s %s lease failed, ret: %d, lease_id: %"PRI64_PREFIX"d, lease_expired_time: %"PRI64_PREFIX"d,status: %d",
            tbsys::CNetUtil::addrToString(ngi.owner_ip_port_).c_str(), NS_KEEPALIVE_TYPE_LOGIN == type ? "login" : "renew",
            ret, ngi.lease_id_, ngi.lease_expired_time_, ngi.owner_status_);
        }
      }
      return ret;
    }

    int NameServerHeartManager::keepalive_in_heartbeat_(common::BasePacket* message)
    {
      int32_t ret = NULL != message ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = message->getPCode() == HEARTBEAT_AND_NS_HEART_MESSAGE ? TFS_SUCCESS : EXIT_UNKNOWN_MSGTYPE;
        if (TFS_SUCCESS == ret)
        {
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          HeartBeatAndNSHeartMessage* hbam = dynamic_cast<HeartBeatAndNSHeartMessage*> (message);
          int32_t ns_switch_flag = hbam->get_ns_switch_flag();
          TBSYS_LOG(DEBUG, "ns_switch_flag: %s, status: %d",
              hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no" : "yes", hbam->get_ns_status());
          HeartBeatAndNSHeartMessage* mashrm = new HeartBeatAndNSHeartMessage();
          mashrm->set_ns_switch_flag_and_status(0 /*no use*/ , ngi.owner_status_);
          message->reply(mashrm);
          if (ns_switch_flag == NS_SWITCH_FLAG_YES)
          {
            TBSYS_LOG(WARN, "ns_switch_flag: %s, status: %d", hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no"
                : "yes", hbam->get_ns_status());
            if (check_vip_(ngi))
            {
              if (!ngi.is_master())
                switch_role_salve_to_master_(ngi, Func::get_monotonic_time());
            }
          }
        }
      }
      return ret;
    }
  }/** nameserver **/
} /** tfs **/
