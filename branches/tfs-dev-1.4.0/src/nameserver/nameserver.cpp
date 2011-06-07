/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
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
    OwnerCheckTimerTask::OwnerCheckTimerTask(NameServer* server) :
      server_(server),
      MAX_LOOP_TIME(SYSPARAM_NAMESERVER.heart_interval_ * 1000 * 1000 / 2)
    {
      int32_t percent_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
      owner_check_time_ = (server_->get_work_queue_size() * percent_size) * 1000;//us
      max_owner_check_time_ = owner_check_time_ * 4;//us
      TBSYS_LOG(INFO, "owner_check_time(%"PRI64_PREFIX"d)(us), max_owner_check_time(%"PRI64_PREFIX"u)(us)",
          owner_check_time_, max_owner_check_time_);
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.last_push_owner_check_packet_time_ = ngi.last_owner_check_time_ = tbutil::Time::now().toMicroSeconds();//ms
    }

    OwnerCheckTimerTask::~OwnerCheckTimerTask()
    {

    }

    void OwnerCheckTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.owner_status_ >= NS_STATUS_INITIALIZED)
      {
        bool bret = false;
        tbutil::Time now = tbutil::Time::now();
        tbutil::Time end = now + MAX_LOOP_TIME;
        if (ngi.last_owner_check_time_ >= ngi.last_push_owner_check_packet_time_)
        {
          int64_t current = 0;
          OwnerCheckMessage* message = new OwnerCheckMessage();
          while(!bret
              && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
              && current < end.toMicroSeconds())
          {
            bret = server_->push(message);
            if (!bret)
            {
              current = tbutil::Time::now().toMicroSeconds();
            }
            else
            {
              ngi.last_push_owner_check_packet_time_ = now.toMicroSeconds();
            }
          }
          if (!bret)
          {
            message->free();
          }
        }
        else
        {
          int64_t diff = now.toMicroSeconds() - ngi.last_owner_check_time_;
          if (diff >= max_owner_check_time_)
          {
            TBSYS_LOG(INFO,"last push owner check packet time(%"PRI64_PREFIX"d)(us) > max owner check time(%"PRI64_PREFIX"d)(us), nameserver dead, modify owner status(uninitialize)",
                ngi.last_push_owner_check_packet_time_, now.toMicroSeconds());
            ngi.owner_status_ = NS_STATUS_UNINITIALIZE;//modif owner status
          }
        }
      }
      return;
    }

    NameServer::NameServer() :
      meta_mgr_(),
      master_heart_task_(0),
      slave_heart_task_(0),
      owner_check_task_(0),
      check_owner_is_master_task_(0),
      master_slave_heart_mgr_(&meta_mgr_, get_timer()),
      heart_mgr_(meta_mgr_)
    {

    }

    NameServer::~NameServer()
    {

    }

    int NameServer::initialize(int argc, char* argv[])
    {
      int32_t iret =  SYSPARAM_NAMESERVER.initialize();
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize nameserver parameter error, must be exit");
        iret = EXIT_GENERAL_ERROR;
      }
      const char* ip_addr = get_ip_addr();
      if (NULL == ip_addr)//get ip addr
      {
        iret =  EXIT_CONFIG_ERROR;
        TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
      }

      if (TFS_SUCCESS == iret)
      {
        const char *dev_name = get_dev();                                                          
        if (NULL == dev_name)//get dev name
        {
          iret =  EXIT_CONFIG_ERROR;
          TBSYS_LOG(ERROR, "%s","nameserver not set dev_name");
        }
        else
        {
          uint32_t ip_addr_id = tbsys::CNetUtil::getAddr(ip_addr);
          if (0 == ip_addr_id)
          {
            iret =  EXIT_CONFIG_ERROR;
            TBSYS_LOG(ERROR, "%s", "nameserver not set ip_addr");
          }
          else
          {
            uint32_t local_ip = Func::get_local_addr(dev_name);
            NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
            ngi.owner_ip_port_  = tbsys::CNetUtil::ipToAddr(local_ip, get_port());
            bool find_ip_in_dev = Func::is_local_addr(ip_addr_id);
            if (!find_ip_in_dev)
            {
              //iret = EXIT_GENERAL_ERROR;
              TBSYS_LOG(WARN, "ip '%s' is not local ip, local ip: %s",ip_addr, tbsys::CNetUtil::addrToString(local_ip).c_str());
            }
          }
        }
      }

      if (TFS_SUCCESS == iret)
      {
        iret = initialize_ns_global_info();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "%s", "initialize nameserver global information error, must be exit");
          iret = EXIT_GENERAL_ERROR;
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int32_t block_chunk_num = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_BLOCK_CHUNK_NUM, 32);
        iret =  meta_mgr_.initialize(block_chunk_num);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "initialize layoutmanager failed, must be exit, ret(%d)", iret);
        }
      }

      if (TFS_SUCCESS == iret)
      {
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_HEART_THREAD_COUNT, 2);
        int32_t heart_max_queue_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_HEART_MAX_QUEUE_SIZE, 10);
        iret = heart_mgr_.initialize(heart_thread_count, heart_max_queue_size);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "initialize heart manager failed, must be exit, ret: %d", iret);
        }
      }

      if (TFS_SUCCESS == iret)
      {
        iret = master_slave_heart_mgr_.initialize();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "initialize master and slave heart manager failed, must be exit, ret: %d", iret);
        }
      }

      if (TFS_SUCCESS == iret)
      {
        //send msg to peer
        //if we're the slave ns & send msg to master failed,we must wait.
        //if we're the master ns, just ignore this failure.
        iret = get_peer_role();
        //ok,now we can check the role of peer
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if ((TFS_SUCCESS != iret)
          || (ngi.owner_role_ == ngi.other_side_role_))
        {
          TBSYS_LOG(ERROR, "iret != TFS_SUCCESS or owner role(%s) == other side role(%s), must be exit...",
              ngi.owner_role_ == NS_ROLE_MASTER ? "master" : "slave", ngi.other_side_role_
              == NS_ROLE_MASTER ? "master" : "slave");
          iret = EXIT_GENERAL_ERROR;
        }
      }
     
      if (TFS_SUCCESS == iret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();

        //if we're the master ns,we can start service now.change status to INITIALIZED.
        if (ngi.owner_role_ == NS_ROLE_MASTER)
        {
          ngi.owner_status_ = NS_STATUS_INITIALIZED;
        }
        else
        {
          //if we're the slave ns, we must sync data from the master ns.
          ngi.owner_status_ = NS_STATUS_ACCEPT_DS_INFO;
          iret = wait_for_ds_report();//in wait_for_ds_report,someone killed me, 
                                      //the signal handler have already called stop()
          if (TFS_SUCCESS == iret)
            ngi.owner_status_ = NS_STATUS_INITIALIZED;
          else
            TBSYS_LOG(ERROR, "wait for dataserver report failed, must be exit, ret: %d", iret);
        }
      }

      //start heartbeat loop
      if (TFS_SUCCESS == iret)
      {
        master_heart_task_ = new MasterHeartTimerTask(&meta_mgr_);
        iret = get_timer()->scheduleRepeated(master_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
        if (iret < 0)
        {
          TBSYS_LOG(ERROR, "%s", "add timer task(MasterHeartTimerTask) error, must be exit");
          iret = EXIT_GENERAL_ERROR;
        }
        if (TFS_SUCCESS == iret)
        {
          slave_heart_task_ = new SlaveHeartTimerTask(&meta_mgr_, get_timer());
          iret = get_timer()->scheduleRepeated(slave_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
          if (iret < 0)
          {
            TBSYS_LOG(ERROR, "%s", "add timer task(SlaveHeartTimerTask) error, must be exit");
            iret = EXIT_GENERAL_ERROR;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          int32_t percent_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
          int64_t owner_check_interval = get_work_queue_size() * percent_size * 1000;
          owner_check_task_ = new OwnerCheckTimerTask(this);
          iret = GFactory::get_timer()->scheduleRepeated(owner_check_task_, tbutil::Time::microSeconds(owner_check_interval));
          if (iret < 0)
          {
            TBSYS_LOG(ERROR, "%s", "add timer task(OwnerCheckTimerTask) error, must be exit");
            iret = EXIT_GENERAL_ERROR;
          }
        }
        if (TFS_SUCCESS == iret)
        {
          check_owner_is_master_task_ = new CheckOwnerIsMasterTimerTask(&meta_mgr_);
          iret = get_timer()->scheduleRepeated(check_owner_is_master_task_, tbutil::Time::seconds(
                SYSPARAM_NAMESERVER.heart_interval_));
          if (iret < 0)
          {
            TBSYS_LOG(ERROR, "%s", "add timer task(CheckOwnerIsMasterTimerTask) error, must be exit");
            iret = EXIT_GENERAL_ERROR;
          }
        }

        if (TFS_SUCCESS == iret)
        {
          GFactory::get_gc_manager().set_layout_manager(&meta_mgr_);
        }

        TBSYS_LOG(INFO, "nameserver running, listen port: %d", get_port());
      }
      return iret;
    }

    int NameServer::destroy_service()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
      {
        {
          tbutil::Mutex::Lock lock(ngi);
          ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
          ngi.destroy_flag_ = NS_DESTROY_FLAGS_YES;
        }

        GFactory::destroy();
        heart_mgr_.destroy();
        master_slave_heart_mgr_.destroy();
        meta_mgr_.destroy();

        heart_mgr_.wait_for_shut_down();
        master_slave_heart_mgr_.wait_for_shut_down();
        meta_mgr_.wait_for_shut_down();
        GFactory::wait_for_shut_down();
      }
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode NameServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
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
          int32_t iret = common::TFS_ERROR;
          NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
          if (ngi.owner_role_ == NS_ROLE_MASTER)
            iret = do_master_msg_helper(bpacket);
          else
            iret = do_slave_msg_helper(bpacket);
          if (common::TFS_SUCCESS == iret)
          {
            hret = tbnet::IPacketHandler::KEEP_CHANNEL;
            switch (pcode)
            {
            case SET_DATASERVER_MESSAGE:
              heart_mgr_.push(bpacket);
              break;
            case MASTER_AND_SLAVE_HEART_MESSAGE:
            case HEARTBEAT_AND_NS_HEART_MESSAGE:
              master_slave_heart_mgr_.push(bpacket);
              break;
            case OPLOG_SYNC_MESSAGE:
              meta_mgr_.get_oplog_sync_mgr().push(bpacket, 0, false);
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
            ngi.dump(TBSYS_LOG_LEVEL(INFO));
            TBSYS_LOG(WARN, "the msg(%d) will be ignored", pcode);
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
        int32_t iret = LOCAL_PACKET == pcode ? TFS_ERROR : common::TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "PCODE: %d", pcode);
          common::BasePacket* msg = dynamic_cast<common::BasePacket*>(packet);
          switch (pcode)
          {
            case GET_BLOCK_INFO_MESSAGE:
              iret = open(msg);
              break;
            case BATCH_GET_BLOCK_INFO_MESSAGE:
              iret = batch_open(msg);
              break;
            case BLOCK_WRITE_COMPLETE_MESSAGE:
              iret = close(msg);
              break;
            case REPLICATE_BLOCK_MESSAGE:
            case BLOCK_COMPACT_COMPLETE_MESSAGE:
            case REMOVE_BLOCK_RESPONSE_MESSAGE:
              iret = meta_mgr_.get_client_request_server().handle(msg);
              break;
            case UPDATE_BLOCK_INFO_MESSAGE:
              iret = update_block_info(msg);
              break;
            case SHOW_SERVER_INFORMATION_MESSAGE:
              iret = show_server_information(msg);
              break;
            case OWNER_CHECK_MESSAGE:
              iret = owner_check(msg);
              break;
            case STATUS_MESSAGE:
              iret = ping(msg);
              break;
            case DUMP_PLAN_MESSAGE:
              iret = dump_plan(msg);
              break;
            case CLIENT_CMD_MESSAGE:
              iret = client_control_cmd(msg);
              break;
            default:
              iret = EXIT_UNKNOWN_MSGTYPE;
              TBSYS_LOG(ERROR, "unknown msg type: %d", pcode);
              break;
          }
          if (common::TFS_SUCCESS != iret)
          {
            msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, "execute message failed");
          }
        }
      }
      return bret;
    }

    int NameServer::callback(common::NewClient* client)
    {
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
        iret = NULL != sresponse && fresponse != NULL ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          tbnet::Packet* packet = client->get_source_msg();
          assert(NULL != packet);
          int32_t pcode = packet->getPCode();
          if (REMOVE_BLOCK_MESSAGE == pcode)
          {
            RemoveBlockMessage* msg = dynamic_cast<RemoveBlockMessage*>(packet);
            if (!sresponse->empty())
            {
              std::vector<uint32_t>::const_iterator iter =  msg->get_remove_blocks().begin();
              for (; iter !=  msg->get_remove_blocks().end(); ++iter)
              {
                TBSYS_LOG(ERROR, "remove block: %u successful", (*iter));
              }
            }
            else
            {
              std::vector<uint32_t>::const_iterator iter =  msg->get_remove_blocks().begin();
              for (; iter !=  msg->get_remove_blocks().end(); ++iter)
              {
                TBSYS_LOG(ERROR, "remove block: %u failed", (*iter));
              }
            }
          }
        }
      }
      return iret;
    }

    int NameServer::open(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        GetBlockInfoMessage* message = dynamic_cast<GetBlockInfoMessage*> (msg);
        uint32_t block_id = message->get_block_id();
        uint32_t lease_id = 0;
        int32_t  mode     = message->get_mode();
        int32_t  version  = 0;
        VUINT64  hold;

        iret = meta_mgr_.get_client_request_server().open(block_id, mode, lease_id, version, hold);
        if (iret == TFS_SUCCESS)
        {
          SetBlockInfoMessage *result_msg = new SetBlockInfoMessage();
          if (mode & T_READ)// read mode
          {
            TBSYS_LOG(DEBUG, "read block info, block(%u), mode(%d)", block_id, mode);
            result_msg->set_read_block_ds(block_id, &hold);
          }
          else //write mode
          {
            result_msg->set_write_block_ds(block_id, &hold, version, lease_id);
            #ifdef TFS_NS_DEBUG
            std::string str;
            std::vector<uint64_t>::iterator iter = hold.begin();
            for (; iter != hold.end(); ++iter)
            {
              str +="/";
              str += tbsys::CNetUtil::addrToString((*iter));
            }
            TBSYS_LOG(DEBUG, "get block info: block(%u) mode(%d) lease(%u), version(%d), dataserver(%s), result(%d)",
                block_id, mode, lease_id, version, str.c_str(), iret);
            #endif
          }
          iret = message->reply(result_msg);
        }
        else
        {
          if(iret == EXIT_NO_DATASERVER)
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
                            "not found dataserver, dataserver size equal 0");
          }
          else if (iret == EXIT_ACCESS_PERMISSION_ERROR)
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_NAMESERVER_ONLY_READ,
                            "current nameserver only read");
          }
          else
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret, 
                            "got error, when get block(%u) mode(%d), result(%d) information", block_id, mode, iret);
          }
        }
      }
      return iret;
    }

    /**
     * a write operation completed, commit to nameserver and update block's verion
     */
    int NameServer::close(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        BlockWriteCompleteMessage* message = dynamic_cast<BlockWriteCompleteMessage*> (msg);
        CloseParameter param;
        memset(&param, 0, sizeof(param));
        param.need_new_ = false;
        param.block_info_ = (*message->get_block());
        param.id_ = message->get_server_id();
        param.lease_id_ = message->get_lease_id();
        param.status_ = message->get_success();
        param.unlink_flag_ = message->get_unlink_flag();
        iret = meta_mgr_.get_client_request_server().close(param);
        if (TFS_SUCCESS == iret)
        {
          iret = message->reply(new StatusMessage(iret, param.error_msg_));
        }
        if (param.need_new_ && iret == TFS_SUCCESS)// add new block when block filled complete
        {
          bool promote = true;
          time_t now = time(NULL);
          meta_mgr_.touch(param.id_, now, promote);
        }
      }
      return iret;
    }

    int NameServer::batch_open(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        BatchGetBlockInfoMessage* message = dynamic_cast<BatchGetBlockInfoMessage*>(msg);
        int32_t block_count = message->get_block_count();
        int32_t  mode     = message->get_mode();
        common::VUINT32& blocks = message->get_block_id();

        BatchSetBlockInfoMessage* reply = new BatchSetBlockInfoMessage();

        int32_t iret = meta_mgr_.get_client_request_server().batch_open(blocks, mode, block_count, reply->get_infos());
        if (iret == TFS_SUCCESS)
        {
          iret = message->reply(reply);
        }
        else
        {
          reply->free();
          if(iret == EXIT_NO_DATASERVER)
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
                            "not found dataserver, dataserver size equal 0");
          }
          else if (iret == EXIT_ACCESS_PERMISSION_ERROR)
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_NAMESERVER_ONLY_READ,
                            "current nameserver only read");
          }
          else
          {
            iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), iret,
                            "batch get get block information error, mode: %d, iret: %d", mode, iret);
          }
        }
     }
      return iret;
    }

    int NameServer::update_block_info(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        UpdateBlockInfoMessage* message = dynamic_cast<UpdateBlockInfoMessage*>(msg);
        uint32_t block_id = message->get_block_id();
        if (block_id == 0)
        {
          iret = msg->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
              "repair block(%u), block object not found", block_id);
        }
        else
        {
          time_t now = time(NULL);
          int32_t repair_flag = message->get_repair();
          uint64_t dest_server = message->get_server_id();
          if (repair_flag == UPDATE_BLOCK_NORMAL)//normal
          {
            if (NULL == message->get_block())
            {
              iret = message->reply_error_packet(TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
                  "repair block(%u) blockinfo object is null", block_id);
            }
            else
            {
              BlockInfo info = (*message->get_block());
              TBSYS_LOG(DEBUG, "block(%u) repair, new information: version(%d)", block_id, info.version_);
              bool addnew = true;
              int iret = meta_mgr_.update_block_info(info, dest_server, now, addnew);
              if (iret == TFS_SUCCESS)
              {
                iret = message->reply(new StatusMessage(iret, "update block info successful"));
              }
              else
              {
                TBSYS_LOG(ERROR, "new block info version lower than meta,cannot update, iret(%d)", iret);
                iret = message->reply(new StatusMessage(iret, "new block info version lower than meta, cannot update"));
              }
            }
          }
          else
          {
            std::string error_msg;
            iret = meta_mgr_.repair(block_id, dest_server, repair_flag, now, error_msg);
            iret = message->reply(new StatusMessage(iret, error_msg.c_str()));
          }
        }
      }
      return iret;
    }

    int NameServer::show_server_information(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        ShowServerInformationMessage* message = dynamic_cast<ShowServerInformationMessage*>(msg);
        ShowServerInformationMessage* resp = new ShowServerInformationMessage();
        SSMScanParameter& param = resp->get_param();
        param.addition_param1_ = message->get_param().addition_param1_;
        param.addition_param2_ = message->get_param().addition_param2_;
        param.start_next_position_ = message->get_param().start_next_position_;
        param.should_actual_count_ = message->get_param().should_actual_count_;
        param.child_type_ = message->get_param().child_type_;
        param.type_ = message->get_param().type_;
        param.end_flag_ = message->get_param().end_flag_;
        iret = meta_mgr_.scan(param);
        if (TFS_SUCCESS == iret)
        {
          iret = message->reply(resp);
        }
      }
      return iret;
    }

    int NameServer::owner_check(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        tbutil::Mutex::Lock lock(ngi);
        ngi.last_owner_check_time_ = tbutil::Time::now().toMicroSeconds();//us
      }
      return iret;
    }

    int NameServer::ping(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        StatusMessage* stmsg = dynamic_cast<StatusMessage*>(msg);
        if (STATUS_MESSAGE_PING == stmsg->get_status())
        {
          StatusMessage* reply_msg = dynamic_cast<StatusMessage*>(get_packet_factory()->createPacket(STATUS_MESSAGE));
          reply_msg->set_message(STATUS_MESSAGE_PING);
          iret = msg->reply(reply_msg);
        }
        else
        {
          iret = EXIT_GENERAL_ERROR;
        }
      }
      return iret;
   }

    int NameServer::dump_plan(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        DumpPlanResponseMessage* rmsg = new DumpPlanResponseMessage();
        iret = meta_mgr_.get_client_request_server().dump_plan(rmsg->get_data());
        if (TFS_SUCCESS == iret)
        {
          iret = msg->reply(rmsg);
        }
      }
      return iret;
    }

    int NameServer::client_control_cmd(common::BasePacket* msg)
    {
      int32_t iret = NULL != msg ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        char buf[256] = {'\0'};
        ClientCmdMessage* message = dynamic_cast<ClientCmdMessage*>(msg);
        StatusMessage* rmsg = NULL;
        iret = meta_mgr_.get_client_request_server().handle_control_cmd(message->get_cmd_info(), msg, 256, buf);
        if (TFS_SUCCESS == iret)
          rmsg = new StatusMessage(STATUS_MESSAGE_OK, buf);
        else
          rmsg = new StatusMessage(STATUS_MESSAGE_ERROR, buf);
        TBSYS_LOG(DEBUG, "IRET: %d", iret);
        iret = msg->reply(rmsg);
      }
      return iret;
    }

    int NameServer::initialize_ns_global_info()
    {
      int32_t iret = GFactory::initialize();
      if (TFS_SUCCESS == iret)
      {
        const char* ns_ip = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_IP_ADDR_LIST);
        if (NULL == ns_ip)
        {
          iret = EXIT_GENERAL_ERROR;
          TBSYS_LOG(ERROR, "%s", "initialize ns ip is null or ns port <= 0, must be exit");
        }
        if (TFS_SUCCESS == iret)
        {
          std::vector < uint32_t > ns_ip_list;
          char buffer[256];
          strncpy(buffer, ns_ip, 256);
          char *t = NULL;
          char *s = buffer;
          while ((t = strsep(&s, "|")) != NULL)
          {
            ns_ip_list.push_back(tbsys::CNetUtil::getAddr(t));
            //ns_ip_list.push_back(Func::str_to_addr(t, get_port()));
          }

          if (2U != ns_ip_list.size())
          {
            TBSYS_LOG(DEBUG, "%s", "must have two ns,check your ns' list");
            iret = EXIT_GENERAL_ERROR;
          }
          else
          {
            uint32_t local_ip = 0;
            bool bfind_flag = false;
            std::vector<uint32_t>::iterator iter = ns_ip_list.begin(); 
            for (; iter != ns_ip_list.end(); ++iter)
            {
              bfind_flag = Func::is_local_addr((*iter));
              if (bfind_flag)
              {
                local_ip = (*iter);
                break;
              }
            }
            if (!bfind_flag)
            {
              TBSYS_LOG(ERROR, "ip list: %s not in %s, must be exit", ns_ip, get_dev());
              iret = EXIT_GENERAL_ERROR;
            }
            else
            {
              NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
              iter = ns_ip_list.begin();
              for (;iter != ns_ip_list.end(); ++iter)
              {
                if (local_ip == (*iter))
                  ngi.owner_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
                else
                  ngi.other_side_ip_port_ = tbsys::CNetUtil::ipToAddr((*iter), get_port());
              }

              ngi.switch_time_ = time(NULL);
              ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
              ngi.vip_ = Func::get_addr(get_ip_addr());
              ngi.owner_role_ = Func::is_local_addr(ngi.vip_) == true ? NS_ROLE_MASTER : NS_ROLE_SLAVE;
              ngi.other_side_role_ = NS_ROLE_NONE;
              TBSYS_LOG(DEBUG, "i %s the master server", ngi.owner_role_ == NS_ROLE_MASTER ? "am" : "am not");
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "%s", "GFactory initialize error, must be exit");
      }
      return iret;
    }

    int NameServer::get_peer_role()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      MasterAndSlaveHeartMessage master_slave_msg;
      master_slave_msg.set_ip_port(ngi.owner_ip_port_);
      master_slave_msg.set_role(ngi.owner_role_);
      master_slave_msg.set_status(ngi.owner_status_);
      master_slave_msg.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      int32_t iret = TFS_SUCCESS;
      bool complete = false;
      tbnet::Packet* ret_msg = NULL;
      while (!stop_)
      {
        TBSYS_LOG(DEBUG, "get peers(%s) role, owner role(%s), other side role(%s)...", tbsys::CNetUtil::addrToString(
              ngi.other_side_ip_port_).c_str(), ngi.owner_role_ == NS_ROLE_MASTER ? "master"
            : "slave", ngi.other_side_role_ == NS_ROLE_MASTER ? "master" : "slave");
        ret_msg = NULL;
        NewClient* client = NewClientManager::get_instance().create_client();
        iret = send_msg_to_server(ngi.other_side_ip_port_, client, &master_slave_msg, ret_msg);
        if (TFS_SUCCESS == iret)
        {
          iret = ret_msg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE ? TFS_SUCCESS : EXIT_GENERAL_ERROR;
          if (TFS_SUCCESS == iret)
          {
            MasterAndSlaveHeartResponseMessage* response = dynamic_cast<MasterAndSlaveHeartResponseMessage *> (ret_msg);
            ngi.other_side_role_ = static_cast<NsRole> (response->get_role());
            if (ngi.other_side_ip_port_ != response->get_ip_port())
            {
              TBSYS_LOG(WARN, "%s", "peer's id in config file is incorrect?");
              ngi.other_side_ip_port_ = response->get_ip_port();
            }
            ngi.other_side_status_ = static_cast<NsStatus> (response->get_status());
            if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED)
                && (ngi.owner_role_== NS_ROLE_MASTER))
            {
              ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
            }
          }
        }
        NewClientManager::get_instance().destroy_client(client);

        if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_YES)
        {
          complete = true;
          TBSYS_LOG(DEBUG, "%s", "maybe someone killed me...");
        }
        else
        {
          complete = ngi.owner_role_ == NS_ROLE_MASTER;
          if (!complete)
          {
            if (ngi.owner_role_ == NS_ROLE_SLAVE)
            {
              if (Func::is_local_addr(ngi.vip_))
              {
                iret = TFS_SUCCESS;
                ngi.owner_role_ = NS_ROLE_MASTER;
              }
              complete =  ngi.owner_role_ == NS_ROLE_MASTER;
              if (!complete)
              {
                //send failed or recv msg error
                //if we're the slave ns,retry
                complete = TFS_SUCCESS == iret;
                if (!complete)
                {
                  TBSYS_LOG(ERROR, "%s", "wait master done, retry...");
                  usleep(500000); //500ms
                  continue;
                }
              }
            }
          }
        }
        if (complete)
        {
          iret = TFS_SUCCESS;
          break;
        }
      }
      return iret;
    }

    int NameServer::wait_for_ds_report()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      MasterAndSlaveHeartMessage master_slave_msg;
      master_slave_msg.set_ip_port(ngi.owner_ip_port_);
      master_slave_msg.set_role(ngi.owner_role_);
      master_slave_msg.set_status(ngi.owner_status_);
      master_slave_msg.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_YES);
      bool complete = false;
      int32_t iret = TFS_SUCCESS;
      int32_t percent = 0;
      tbnet::Packet* ret_msg = NULL;
      time_t end_report_time = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
      while (!stop_)
      {
        if (ngi.destroy_flag_ != NS_DESTROY_FLAGS_YES)
        {
          ret_msg = NULL;
          NewClient* client = NewClientManager::get_instance().create_client();
          iret = send_msg_to_server(ngi.other_side_ip_port_, client, &master_slave_msg, ret_msg);
          if (TFS_SUCCESS == iret)
          {
            iret = ret_msg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
            if (TFS_SUCCESS == iret)
            {
              MasterAndSlaveHeartResponseMessage *response = NULL;
              response = dynamic_cast<MasterAndSlaveHeartResponseMessage *> (ret_msg);
              VUINT64 peer_list(response->get_ds_list()->begin(), response->get_ds_list()->end());
              VUINT64 local_list;
              meta_mgr_.get_alive_server(local_list);
              TBSYS_LOG(DEBUG, "local size:%u,peer size:%u", local_list.size(), peer_list.size());
              complete = peer_list.size() == 0U;
              if (!complete)
              {
                percent = (int) (((double) local_list.size() / (double) peer_list.size()) * 100);
                complete = percent > 90;
              }
            }
          }
          NewClientManager::get_instance().destroy_client(client);
        }
        else
        {
          complete = true;
        }

        if (!complete)
        {
          complete = time(NULL) > end_report_time;
          if (!complete)
          {
            usleep(500000); //500ms
          }
          complete = time(NULL) > end_report_time;
        }
        if (complete)
        {
          break;
        }
      }
      return TFS_SUCCESS;
    }

    int NameServer::do_master_msg_helper(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        iret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          //receive all owner check message , master and slave heart message, dataserver heart message
          if (pcode != OWNER_CHECK_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            iret = ngi.owner_status_ <= NS_STATUS_ACCEPT_DS_INFO ? common::TFS_ERROR : common::TFS_SUCCESS;
          }
        }
      }
      return iret;
    }

    int NameServer::do_slave_msg_helper(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet ? common::TFS_SUCCESS : common::TFS_ERROR;
      if (common::TFS_SUCCESS == iret)
      {
        int32_t pcode = packet->getPCode();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        iret = ngi.owner_status_ >= NS_STATUS_UNINITIALIZE //service status is valid, we'll receive message
               && ngi.owner_status_ <= NS_STATUS_INITIALIZED ? common::TFS_SUCCESS : common::TFS_ERROR;
        if (common::TFS_SUCCESS == iret)
        {
          if (pcode != OWNER_CHECK_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_MESSAGE
            && pcode != HEARTBEAT_AND_NS_HEART_MESSAGE
            && pcode != MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE
            && pcode != CLIENT_CMD_MESSAGE)
          {
            if (ngi.owner_status_ <= NS_STATUS_ACCEPT_DS_INFO)
            {
              iret = common::TFS_ERROR;
            }
            else
            {
              if (pcode != REPLICATE_BLOCK_MESSAGE
                && pcode != BLOCK_COMPACT_COMPLETE_MESSAGE
                && pcode != OPLOG_SYNC_MESSAGE
                && pcode != GET_BLOCK_INFO_MESSAGE
                && pcode != GET_BLOCK_INFO_MESSAGE
                && pcode != SET_DATASERVER_MESSAGE
                && pcode != BATCH_GET_BLOCK_INFO_MESSAGE 
                && pcode != SHOW_SERVER_INFORMATION_MESSAGE)
              {
                iret = common::TFS_ERROR;
              }
            }
          }
        }
      }
      return iret;
    }

    int ns_async_callback(common::NewClient* client)
    {
      NameServer* service = dynamic_cast<NameServer*>(BaseMain::instance());
      int32_t iret = NULL != service ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = service->callback(client);
      }
      return iret;
    }
  } /** nameserver **/
}/** tfs **/

