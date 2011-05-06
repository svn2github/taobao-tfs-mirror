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
#include <Memory.hpp>
#include "nameserver.h"
#include "common/config.h"
#include "common/error_msg.h"
#include "common/config_item.h"
#include "global_factory.h"
#include "message/client_manager.h"
#include <iterator>

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
      main_task_queue_size_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_MAX_QUEUE_SIZE, 100);
      int32_t percent_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
      owner_check_time_ = main_task_queue_size_ * percent_size * 1000;//us
      max_owner_check_time_ = owner_check_time_ * 4;//us
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.last_push_owner_check_packet_time_ = ngi.last_owner_check_time_ = tbutil::Time::now().toMicroSeconds();//us
      TBSYS_LOG(INFO, "owner_check_time(%"PRI64_PREFIX"d)(us), max_owner_check_time(%"PRI64_PREFIX"u)(us)",
          owner_check_time_, max_owner_check_time_);
    }

    OwnerCheckTimerTask::~OwnerCheckTimerTask()
    {

    }

    void OwnerCheckTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.owner_status_ < NS_STATUS_INITIALIZED)
         return;

      bool bret = false;
      tbutil::Time now = tbutil::Time::now().toMicroSeconds();
      tbutil::Time end = now + MAX_LOOP_TIME;
      if (ngi.last_owner_check_time_ >= ngi.last_push_owner_check_packet_time_)
      {
        tbutil::Time current(0);
        OwnerCheckMessage* message = new OwnerCheckMessage();
        while(!bret
            && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
            && current < end)
        {
          bret = server_->get_packet_queue_thread()->push(message, main_task_queue_size_, false);
          if (!bret)
          {
            current = tbutil::Time::now().toMicroSeconds();
          }
          else
          {
            ngi.last_push_owner_check_packet_time_ = now;
          }
        }
        if (!bret)
        {
          tbsys::gDelete(message);
        }
      }
      else
      {
        tbutil::Time diff = now - ngi.last_owner_check_time_;
        if (diff >= max_owner_check_time_)
        {
          TBSYS_LOG(INFO,"last push owner check packet time(%"PRI64_PREFIX"d)(us) > max owner check time(%"PRI64_PREFIX"d)(us), nameserver dead, modify owner status(uninitialize)",
              ngi.last_push_owner_check_packet_time_.toMicroSeconds(),now.toMicroSeconds());
          ngi.owner_status_ = NS_STATUS_UNINITIALIZE;//modif owner status
        } 
      }
    }

    NameServer::NameServer() :
      meta_mgr_(),
      timer_(new tbutil::Timer()),
      owner_check_timer_(new tbutil::Timer()),
      master_heart_task_(0),
      slave_heart_task_(0),
      owner_check_task_(0),
      check_owner_is_master_task_(0),
      master_slave_heart_mgr_(&meta_mgr_, timer_),
      heart_mgr_(meta_mgr_),
      main_task_queue_size_(4096)
    {

    }

    NameServer::~NameServer()
    {

    }

    int NameServer::start()
    {
      // initialize ngi
      if (initialize_ns_global_info() != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "initialize nameserver global information error, must be exit");
        return EXIT_GENERAL_ERROR;
      }

      //start ns' heartbeat
      streamer_.set_packet_factory(&msg_factory_);
      NewClientManager::get_instance().initialize(&transport_);

      int32_t server_port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
      char spec[32];
      snprintf(spec, 32, "tcp::%d", server_port);
      if (transport_.listen(spec, &streamer_, this) == NULL)
      {
        TBSYS_LOG(ERROR, "listen port failed(%d), must be exit", server_port);
        return EXIT_NETWORK_ERROR;
      }
      master_slave_heart_mgr_.initialize();

      transport_.start();

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      //send msg to peer
      //if we're the slave ns & send msg to master failed,we must wait.
      //if we're the master ns, just ignore this failure.
      int32_t ret = get_peer_role();
      //ok,now we can check the role of peer

      if ((ret != TFS_SUCCESS)
          ||(ngi.owner_role_ == ngi.other_side_role_))
      {
        TBSYS_LOG(ERROR, "iret != TFS_SUCCESS or owner role(%s) == other side role(%s), must be exit...",
            ngi.owner_role_ == NS_ROLE_MASTER ? "master" : "slave", ngi.other_side_role_
            == NS_ROLE_MASTER ? "master" : "slave");
        return EXIT_GENERAL_ERROR;
      }

      int32_t block_chunk_num = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_BLOCK_CHUNK_NUM, 32);

      ret = meta_mgr_.initialize(block_chunk_num);
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize layoutmanager failed, must be exit, ret(%d)", ret);
        return ret;
      }

      initialize_handle_task_and_heart_threads();

      //if we're the master ns,we can start service now.change status to INITIALIZED.
      //TODO lock
      if (ngi.owner_role_ == NS_ROLE_MASTER)
      {
        ngi.owner_status_ = NS_STATUS_INITIALIZED;
      }
      else
      { //if we're the slave ns, we must sync data from the master ns.
        ngi.owner_status_ = NS_STATUS_ACCEPT_DS_INFO;
        if (wait_for_ds_report() != TFS_SUCCESS)//in wait_for_ds_report,someone killed me
          return EXIT_GENERAL_ERROR; //the signal handler have already called stop()
        ngi.owner_status_ = NS_STATUS_INITIALIZED;
      }

      //start heartbeat loop
      master_heart_task_ = new MasterHeartTimerTask(&meta_mgr_);
      ret = timer_->scheduleRepeated(master_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
      if (ret < 0)
      {
        TBSYS_LOG(ERROR, "%s", "add timer task(MasterHeartTimerTask) error, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      slave_heart_task_ = new SlaveHeartTimerTask(&meta_mgr_, timer_);
      ret = timer_->scheduleRepeated(slave_heart_task_, tbutil::Time::seconds(SYSPARAM_NAMESERVER.heart_interval_));
      if (ret < 0)
      {
        TBSYS_LOG(ERROR, "%s", "add timer task(SlaveHeartTimerTask) error, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      int32_t percent_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_PRECENT_SEC_SIZE, 1);
      int32_t owner_check_interval = main_task_queue_size_ * percent_size;
      owner_check_task_ = new OwnerCheckTimerTask(this);
      ret = owner_check_timer_->scheduleRepeated(owner_check_task_, tbutil::Time::microSeconds(owner_check_interval));
      if (ret < 0)
      {
        TBSYS_LOG(ERROR, "%s", "add timer task(OwnerCheckTimerTask) error, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      check_owner_is_master_task_ = new CheckOwnerIsMasterTimerTask(&meta_mgr_);
      ret = timer_->scheduleRepeated(check_owner_is_master_task_, tbutil::Time::seconds(
            SYSPARAM_NAMESERVER.heart_interval_));
      if (ret < 0)
      {
        TBSYS_LOG(ERROR, "%s", "add timer task(CheckOwnerIsMasterTimerTask) error, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      GFactory::get_gc_manager().set_layout_manager(&meta_mgr_);
      return TFS_SUCCESS;
    }

    int NameServer::stop()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if (ngi.destroy_flag_ == NS_DESTROY_FLAGS_YES)
      {
        return TFS_SUCCESS;
      }

      transport_.stop();
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
      }
      NewClientManager::get_instance().destroy();
      ngi.destroy_flag_ = NS_DESTROY_FLAGS_YES;

      timer_->cancel(master_heart_task_);
      timer_->cancel(slave_heart_task_);
      timer_->cancel(check_owner_is_master_task_);
      owner_check_timer_->cancel(owner_check_task_);
      timer_->destroy();
      owner_check_timer_->destroy();
      GFactory::destroy();
      heart_mgr_.destroy();
      master_slave_heart_mgr_.destroy();
      main_task_queue_thread_.stop();
      meta_mgr_.destroy();
      return TFS_SUCCESS;
    }

    void NameServer::wait()
    {
      heart_mgr_.wait_for_shut_down();
      master_slave_heart_mgr_.wait_for_shut_down();
      main_task_queue_thread_.wait();
      meta_mgr_.wait_for_shut_down();
      GFactory::wait_for_shut_down();
      transport_.wait();
    }

    tbnet::IPacketHandler::HPRetCode NameServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      bool bret = connection == NULL || packet == NULL;
      if (bret)
      {
        TBSYS_LOG(ERROR, "%s", "connection is invalid or packet is invalid");
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(ERROR, "controlpacket, cmd(%d)", ((tbnet::ControlPacket*) packet)->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      Message *bp = dynamic_cast<Message*> (packet);
      bp->set_connection(connection);
      bp->setExpireTime(MAX_RESPONSE_TIME);
      bp->set_direction(bp->get_direction() | DIRECTION_RECEIVE);

      int32_t pcode = bp->getPCode();
      TBSYS_LOG(DEBUG, "PCODE: %d", pcode);

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      //need a lock ?
      // is master & status == INITIALIZED ,all msg is allowed.
      // is master & status != INITIALIZED ,ns' heart msg is allowed.
      // is slave & status == INITIALIZED || ACCEPT_DS_INFO, ns & ds' heart msg are allowed.
      // is slave & status != INITIALIZED && ACCEPT_DS_INFO, ns' heart msg is allowed.
      if (pcode == OWNER_CHECK_MESSAGE)
        goto PASS;

      if ((ngi.owner_status_ < NS_STATUS_UNINITIALIZE)
          || (ngi.owner_status_> NS_STATUS_INITIALIZED))
      {
        TBSYS_LOG(WARN,"%s", "status is incorrect");
        ngi.dump(TBSYS_LOG_LEVEL(INFO));
        goto NOT_ALLOWED;
      }

      if (ngi.owner_status_ == NS_STATUS_UNINITIALIZE)
      {
        if (pcode == MASTER_AND_SLAVE_HEART_MESSAGE)
          goto PASS;
      }
      else if ((ngi.owner_status_ == NS_STATUS_INITIALIZED) || (ngi.owner_status_
            == NS_STATUS_ACCEPT_DS_INFO))
      {
        if (pcode == GET_SERVER_STATUS_MESSAGE || pcode == GET_BLOCK_INFO_MESSAGE) //for convenience
          goto PASS;
        if (ngi.owner_role_ == NS_ROLE_MASTER)
        {
          assert(ngi.owner_status_ != NS_STATUS_ACCEPT_DS_INFO);
          goto PASS;
        }
        else if (ngi.owner_role_ == NS_ROLE_SLAVE)
        {
          if (pcode == MASTER_AND_SLAVE_HEART_MESSAGE || pcode == SET_DATASERVER_MESSAGE || pcode == OPLOG_SYNC_MESSAGE
              || pcode == REPLICATE_BLOCK_MESSAGE || pcode == BLOCK_COMPACT_COMPLETE_MESSAGE || pcode
              == GET_BLOCK_LIST_MESSAGE || pcode == HEARTBEAT_AND_NS_HEART_MESSAGE)
          {
            goto PASS;
          }
        }
      }

NOT_ALLOWED:
      TBSYS_LOG(DEBUG, "the msg(%d) will be ignored", pcode);
      bp->free();
      return tbnet::IPacketHandler::FREE_CHANNEL;
PASS:
      switch (pcode)
      {
        case SET_DATASERVER_MESSAGE:
          heart_mgr_.push(bp);
          break;
        case MASTER_AND_SLAVE_HEART_MESSAGE:
        case HEARTBEAT_AND_NS_HEART_MESSAGE:
          master_slave_heart_mgr_.push(bp);
          break;
        case OPLOG_SYNC_MESSAGE:
          meta_mgr_.get_oplog_sync_mgr()->push(bp);
          break;
        default:
          if (!main_task_queue_thread_.push(bp, main_task_queue_size_, false))
          {
            MessageFactory::send_error_message(bp, TBSYS_LOG_LEVEL(ERROR), STATUS_MESSAGE_ERROR,
                ngi.owner_ip_port_, "task message beyond max queue size, discard.");
            bp->free();
          }
          break;
      }
      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }

    int NameServer::callback(message::NewClient* client)
    {
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
        NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
        iret = NULL != sresponse && fresponse != NULL ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_SUCCESS == iret)
        {
          //if (client->get_send_id_sign().size() == 1U)
          {
            if (!sresponse->empty())
            {
              NewClient::RESPONSE_MSG_MAP_ITER iter = sresponse->begin();
              Message* msg = NewClientManager::get_instance().clone_message(iter->second.second, 2, false);
              if (!main_task_queue_thread_.push(msg, main_task_queue_size_, false))
              {
                TBSYS_LOG(ERROR, "task message beyond max queue size(%d), discard.", main_task_queue_size_);
                msg->free();
              }
            }
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return iret;
    }

    bool NameServer::handlePacketQueue(tbnet::Packet *packet, void *)
    {
      if (packet == NULL)
      {
        TBSYS_LOG(ERROR, "%s", "packet is invalid");
        return false;
      }
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      Message *message = dynamic_cast<Message*> (packet);
      int ret = TFS_SUCCESS;
      switch (message->get_message_type())
      {
        case GET_BLOCK_INFO_MESSAGE:
          ret = open(message);
          break;
        case BLOCK_WRITE_COMPLETE_MESSAGE:
          ret = close(message);
          break;
        case REPLICATE_BLOCK_MESSAGE:
        case BLOCK_COMPACT_COMPLETE_MESSAGE:
        case REMOVE_BLOCK_RESPONSE_MESSAGE:
        case DUMP_PLAN_MESSAGE:
        case CLIENT_CMD_MESSAGE:
          meta_mgr_.handle(message);
          break;
        case UPDATE_BLOCK_INFO_MESSAGE:
          ret = update_block_info(message);
          break;
        case SHOW_SERVER_INFORMATION_MESSAGE:
          ret = get_ds_status(message);
          break;
        case OWNER_CHECK_MESSAGE:
          ret = owner_check(message);
          break;
        case STATUS_MESSAGE:
          if (dynamic_cast<StatusMessage*> (message)->get_status() == STATUS_MESSAGE_PING)
            message->reply_message(new StatusMessage(STATUS_MESSAGE_PING));
          else
            ret = EXIT_GENERAL_ERROR;
          break;
        default:
          ret = EXIT_UNKNOWN_MSGTYPE;
          break;
      }
      if (ret != TFS_SUCCESS)
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, ngi.owner_ip_port_,
            "execute message failed");
      }
      return true;
    }

    int NameServer::open(Message *msg)
    {
      bool bret = msg != NULL;
      if (bret)
      {
        GetBlockInfoMessage* message = dynamic_cast<GetBlockInfoMessage*> (msg);
        uint32_t block_id = message->get_block_id();
        uint32_t lease_id = 0;
        int32_t  mode     = message->get_mode();
        int32_t  version  = 0;
        VUINT64  hold;

        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        int32_t iret = meta_mgr_.open(block_id, mode, lease_id, version, hold);
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
          message->reply_message(result_msg);
          return TFS_SUCCESS;
        }
        else if(iret == EXIT_NO_DATASERVER)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
              ngi.owner_ip_port_, "not found dataserver, dataserver size equal 0");
        }
        else if (iret == EXIT_ACCESS_PERMISSION_ERROR)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NAMESERVER_ONLY_READ,
            ngi.owner_ip_port_, "current nameserver only read");
        }
        else
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), iret,
              ngi.owner_ip_port_,"got error, when get block(%u) mode(%d), result(%d) information", block_id, mode, iret);
        }
      }
      return TFS_ERROR;
    }

    /**
     * a write operation completed, commit to nameserver and update block's verion
     */
    int NameServer::close(Message* msg)
    {
      bool bret = msg != NULL;
      if (bret)
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
        int32_t iret = meta_mgr_.close(param);
        message->reply_message(new StatusMessage(iret, param.error_msg_));
        if (param.need_new_ && iret == TFS_SUCCESS)// add new block when block filled complete
        {
          bool promote = true;
          time_t now = time(NULL);
          meta_mgr_.touch(param.id_, now, promote);
        }
        return iret;
      }
      return TFS_ERROR;
    }

    int NameServer::batch_open(message::Message* msg)
    {
      bool bret = msg != NULL;
      if (bret)
      {
        BatchGetBlockInfoMessage* message = dynamic_cast<BatchGetBlockInfoMessage*>(msg);
        int32_t block_count = message->get_block_count();
        int32_t  mode     = message->get_mode();
        common::VUINT32& blocks = message->get_block_id();

        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();

        BatchSetBlockInfoMessage* reply = new BatchSetBlockInfoMessage();

        int32_t iret = meta_mgr_.batch_open(blocks, mode, block_count, reply->get_infos());
        if (iret == TFS_SUCCESS)
        {
          message->reply_message(reply);
          return TFS_SUCCESS;
        }
        else if(iret == EXIT_NO_DATASERVER)
        {
          tbsys::gDelete(reply);
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NO_DATASERVER,
              ngi.owner_ip_port_, "not found dataserver, dataserver size equal 0");
        }
        else if (iret == EXIT_ACCESS_PERMISSION_ERROR)
        {
          tbsys::gDelete(reply);
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_NAMESERVER_ONLY_READ,
            ngi.owner_ip_port_, "current nameserver only read");
        }
        else
        {
          tbsys::gDelete(reply);
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), iret,
              ngi.owner_ip_port_,"batch get get block information error, mode(%d), iret(%d)", mode, iret);
        }
      }
      return TFS_ERROR;
    }

    int NameServer::update_block_info(Message* msg)
    {
      bool bret = msg != NULL;
      if (bret)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        UpdateBlockInfoMessage* message = dynamic_cast<UpdateBlockInfoMessage*>(msg);
        uint32_t block_id = message->get_block_id();
        if (block_id == 0)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
              ngi.owner_ip_port_, "repair block(%u), block object not found", block_id);
        }
        time_t now = time(NULL);
        int32_t repair_flag = message->get_repair();
        uint64_t dest_server = message->get_server_id();
        if (repair_flag == UPDATE_BLOCK_NORMAL)//normal
        {
          if (message->get_block() == NULL)
          {
            return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), EXIT_BLOCK_NOT_FOUND,
                ngi.owner_ip_port_, "repair block(%u) blockinfo object is null", block_id);
          }
          BlockInfo info = (*message->get_block());
          TBSYS_LOG(DEBUG, "block(%u) repair, new information: version(%d)", block_id, info.version_);
          bool addnew = true;
          int iret = meta_mgr_.update_block_info(info, dest_server, now, addnew);
          if (iret == TFS_SUCCESS)
          {
            message->reply_message(new StatusMessage(iret, "update block info successful"));
          }
          else
          {
            TBSYS_LOG(ERROR, "new block info version lower than meta,cannot update, iret(%d)", iret);
            message->reply_message(new StatusMessage(iret, "new block info version lower than meta, cannot update"));
          }
          return iret;
        } 
        else
        {
          std::string error_msg;
          int iret = meta_mgr_.repair(block_id, dest_server, repair_flag, now, error_msg);
          message->reply_message(new StatusMessage(iret, error_msg.c_str()));
        }
      }
      return TFS_ERROR;
    }

    int NameServer::get_ds_status(Message *msg)
    {
      bool bret = msg != NULL;
      if (bret)
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
        meta_mgr_.scan(param);
        message->reply_message(resp);
      }
      return TFS_ERROR;
    }

    int NameServer::owner_check(Message*)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.last_owner_check_time_ = tbsys::CTimeUtil::getTime();
      return TFS_SUCCESS;
    }

    int NameServer::initialize_ns_global_info()
    {
      if (GFactory::initialize() != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "%s", "GFactory initialize error, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      const char* ns_ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR_LIST);
      int32_t ns_port = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_PORT);
      if (ns_ip == NULL || ns_port <= 0)
      {
        TBSYS_LOG(ERROR, "%s", "initialize ns ip is null or ns port <= 0, must be exit");
        return EXIT_GENERAL_ERROR;
      }
      std::vector < uint64_t > ns_ip_list;
      char buffer[256];
      strncpy(buffer, ns_ip, 256);
      char *t = NULL;
      char *s = buffer;
      while ((t = strsep(&s, "|")) != NULL)
      {
        ns_ip_list.push_back(Func::str_to_addr(t, ns_port));
      }
      if (ns_ip_list.size() != 2)
      {
        TBSYS_LOG(DEBUG, "%s", "must have two ns,check your ns' list");
        return EXIT_GENERAL_ERROR;
      }

      const char *dev_name = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_DEV_NAME);
      uint32_t local_ip = Func::get_local_addr(dev_name);
      if (dev_name == NULL || local_ip == 0)
      {
        TBSYS_LOG(ERROR, "%s", "get dev name is null or local ip == 0 , must be exit");
        return EXIT_GENERAL_ERROR;
      }

      uint64_t local_ns_id = 0;
      IpAddr* adr = reinterpret_cast<IpAddr*>(&local_ns_id);
      adr->ip_ = local_ip;
      adr->port_ = ns_port;

      if (std::find(ns_ip_list.begin(), ns_ip_list.end(), local_ns_id) == ns_ip_list.end())
      {
        TBSYS_LOG(ERROR, "local ip(%s) not in ip_list(%s) , must be exit",
            tbsys::CNetUtil::addrToString(local_ns_id).c_str(), ns_ip);
        return EXIT_GENERAL_ERROR;
      }

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      for (std::vector<uint64_t>::iterator it = ns_ip_list.begin(); it != ns_ip_list.end(); ++it)
      {
        local_ns_id == *it ? ngi.owner_ip_port_ = *it : ngi.other_side_ip_port_ = *it;
      }

      ngi.switch_time_ = time(NULL);
      ngi.owner_status_ = NS_STATUS_UNINITIALIZE;
      const char *ip = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_IP_ADDR);
      ngi.vip_ = Func::get_addr(ip);

      ngi.owner_role_ = Func::is_local_addr(ngi.vip_) == true ? NS_ROLE_MASTER : NS_ROLE_SLAVE;
      ngi.other_side_role_ = NS_ROLE_NONE;

      TBSYS_LOG(DEBUG, "i %s the master server", ngi.owner_role_ == NS_ROLE_MASTER ? "am" : "am not");
      return TFS_SUCCESS;
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
      Message *ret_msg = NULL;
      while (true)
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
                if (iret != TFS_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "%s", "wait master done, retry...");
                  usleep(500000); //500ms
                }
              }
            }
          }
        }
        if (complete)
        {
          break;
        }
      }
      return iret;
    }

    void NameServer::initialize_handle_task_and_heart_threads()
    {
      int32_t thead_count = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_THREAD_COUNT, 1);
      main_task_queue_thread_.setThreadParameter(thead_count, this, NULL);
      main_task_queue_thread_.start();

      int32_t heart_thread_count = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_HEART_THREAD_COUNT, 2);
      int32_t heart_max_queue_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_HEART_MAX_QUEUE_SIZE, 10);
      heart_mgr_.initialize(heart_thread_count, heart_max_queue_size);

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      main_task_queue_size_ = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_TASK_MAX_QUEUE_SIZE, 100);
      TBSYS_LOG(INFO, "fsnamesystem::start: %s", tbsys::CNetUtil::addrToString(ngi.owner_ip_port_).c_str());
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
      Message *ret_msg = NULL;
      time_t end_report_time = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
      while (true)
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

    int send_msg_to_server(uint64_t server, Message* message)
    {
      NewClient* client = NewClientManager::get_instance().create_client();
      int32_t iret = NULL != client ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        uint8_t send_id = 0;
        iret = client->post_request(server, message, send_id);  
        if (TFS_SUCCESS == iret)
        {
          client->wait();
          NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
          NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
          iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            iret = sresponse->empty() ? EXIT_TIMEOUT_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              NewClient::RESPONSE_MSG_MAP_ITER  iter = sresponse->begin();
              Message* rmsg = iter->second.second;
              iret = rmsg->getPCode() == STATUS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                StatusMessage* smsg = dynamic_cast<StatusMessage*>(rmsg);
                iret = smsg->get_status();
              }
            }
          }
        }
      }
      NewClientManager::get_instance().destroy_client(client);
      return iret;
    }

    int send_msg_to_server(uint64_t server, message::NewClient* client, message::Message* msg, message::Message*& output/*not free*/)
    {
      int32_t iret = NULL != client && server > 0 && NULL != msg && NULL == output ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        uint8_t send_id = 0;
        iret = client->post_request(server, msg, send_id);  
        if (TFS_SUCCESS == iret)
        {
          client->wait();
          NewClient::RESPONSE_MSG_MAP* sresponse = client->get_success_response();
          NewClient::RESPONSE_MSG_MAP* fresponse = client->get_fail_response();
          iret = NULL != sresponse && NULL != fresponse ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            iret = sresponse->empty() ? EXIT_TIMEOUT_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == iret)
            {
              NewClient::RESPONSE_MSG_MAP_ITER  iter = sresponse->begin();
              iret = NULL != iter->second.second? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                output = iter->second.second;
              }
            }
          }
        }
      }
      return iret;
    }
  }
}

