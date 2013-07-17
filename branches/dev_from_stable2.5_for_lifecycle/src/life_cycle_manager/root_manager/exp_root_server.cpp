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
#include "common/tairengine_helper.h"
#include "exp_root_server.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace exprootserver
  {
    static const int8_t MAX_RETRY_COUNT = 2;
    static const int16_t MAX_TIMEOUT_MS = 500;
    static const int32_t TASK_PERIOD_SECONDS = 60 * 60;

    ExpRootServer::ExpRootServer():
      assign_task_thread_(0),
      rt_es_heartbeat_handler_(*this),
      manager_(*this)
    {

    }

    ExpRootServer::~ExpRootServer()
    {

    }

    int ExpRootServer::initialize(int /*argc*/, char* /*argv*/[])
    {
      int32_t iret =  SYSPARAM_KVRTSERVER.initialize();
      if (TFS_SUCCESS != iret)
      {
        TBSYS_LOG(ERROR, "%s", "initialize exprootserver parameter error, must be exit");
        iret = EXIT_GENERAL_ERROR;
      }

      //initialize expserver manager
      if (TFS_SUCCESS == iret)
      {
				iret = manager_.initialize();
				if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "exp server manager initialize error, iret: %d, must be exit", iret);
        }
      }

      //initialize expserver ==> exprootserver heartbeat
      if (TFS_SUCCESS == iret)
      {
        assign_task_thread_ = new AssignTaskThreadHelper(*this);
        int32_t heart_thread_count = TBSYS_CONFIG.getInt(CONF_SN_EXPIRESERVER, CONF_HEART_THREAD_COUNT, 1);
        rt_es_heartbeat_workers_.setThreadParameter(heart_thread_count, &rt_es_heartbeat_handler_, this);
        rt_es_heartbeat_workers_.start();
      }

      if (TFS_SUCCESS == iret)
      {
        kv_engine_helper_ = new TairEngineHelper();
        iret = kv_engine_helper_->init();
      }

      if (TFS_SUCCESS == iret)
      {
        iret = query_task_helper_.init(kv_engine_helper_);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "query task helper initial fail: %d", iret);
        }
      }

      return iret;
    }

    int ExpRootServer::destroy_service()
    {
      rt_es_heartbeat_workers_.stop();
      rt_es_heartbeat_workers_.wait();
      if (0 != assign_task_thread_)
      {
        assign_task_thread_->join();
        assign_task_thread_ = 0;
      }
      manager_.destroy();
      return TFS_SUCCESS;
    }

    /** handle single packet */
    tbnet::IPacketHandler::HPRetCode ExpRootServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
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
            case REQ_RT_ES_KEEPALIVE_MESSAGE:
              rt_es_heartbeat_workers_.push(bpacket, 0/* no limit */, false/* no block */);
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
    bool ExpRootServer::handlePacketQueue(tbnet::Packet *packet, void *args)
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
            case REQ_RT_FINISH_TASK_MESSAGE:
              iret = handle_finish_task(msg);
              break;
            case REQ_QUERY_PROGRESS_MESSAGE:
              iret = query_progress(msg);
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

		bool ExpRootServer::KeepAliveIPacketQueueHandlerHelper::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool bret = packet != NULL;
      if (bret)
      {
        //if return TFS_SUCCESS, packet had been delete in this func
        //if handlePacketQueue return true, tbnet will delete this packet
        assert(packet->getPCode() == REQ_RT_ES_KEEPALIVE_MESSAGE);
        manager_.rt_es_keepalive(dynamic_cast<BasePacket*>(packet));
      }
      return bret;
    }

    int ExpRootServer::rt_es_keepalive(common::BasePacket* packet)
    {
      int32_t iret = NULL != packet? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        ReqRtsEsHeartMessage* msg = dynamic_cast<ReqRtsEsHeartMessage*>(packet);
        common::ExpServerBaseInformation& base_info = msg->get_mutable_es();
        iret = manager_.keepalive(base_info);

				RspRtsEsHeartMessage* reply_msg = new(std::nothrow) RspRtsEsHeartMessage();
				assert(NULL != reply_msg);
        int32_t tmp = SYSPARAM_EXPIRESERVER.es_rts_heart_interval_;
        //reply_msg->set_time(SYSPARAM_KVRTSERVER.es_rts_heart_interval_);
        TBSYS_LOG(DEBUG, "es_rts_heart_interval_: %d is ", SYSPARAM_EXPIRESERVER.es_rts_heart_interval_);
        reply_msg->set_time(tmp);
        iret = packet->reply(reply_msg);
      }
      return iret;
    }

    int ExpRootServer::assign(const uint64_t es_id, const ExpireDeleteTask &del_task)
    {
      NewClient* client = NULL;
      int32_t retry_count = 0;
      int32_t iret = TFS_SUCCESS;
      tbnet::Packet* rsp = NULL;

      ReqCleanTaskFromRtsMessage msg;
      msg.set_total_es(del_task.alive_total_);
      msg.set_num_es(del_task.assign_no_);
      msg.set_task_time(del_task.spec_time_);
      msg.set_task_type(del_task.type_);
      msg.set_note_interval(del_task.note_interval_);

      do
      {
        ++retry_count;
        client = NewClientManager::get_instance().create_client();
        tbutil::Time start = tbutil::Time::now();
        iret = send_msg_to_server(es_id, client, &msg, rsp, MAX_TIMEOUT_MS);
        tbutil::Time end = tbutil::Time::now();
        if (TFS_SUCCESS == iret)
        {
          TBSYS_LOG(DEBUG, "task_sucess");
          assert(NULL != rsp);
          iret = rsp->getPCode() == STATUS_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == iret)
          {
            StatusMessage* rmsg = dynamic_cast<StatusMessage*>(rsp);
            if (rmsg->get_status() == STATUS_MESSAGE_OK)
            {
              NewClientManager::get_instance().destroy_client(client);
              break;
            }
            else
            {
              iret = TFS_ERROR;
            }
          }
        }
        //TBSYS_LOG(DEBUG, "MAX_TIMEOUT_MS: %d, cost: %"PRI64_PREFIX"d, inter: %"PRI64_PREFIX"d", MAX_TIMEOUT_MS, (int64_t)(end - start).toMilliSeconds(), heart_inter_);
        if (TFS_SUCCESS != iret)
        {
          usleep(500000);
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      while ((retry_count < MAX_RETRY_COUNT)
          && (TFS_SUCCESS != iret));

      return iret;
    }

    int ExpRootServer::assign_task()
    {
      int ret = TFS_SUCCESS;

      while (true)
      {
        ExpTable exp_table;
        ret = manager_.get_table(exp_table);

        tbutil::Time now = tbutil::Time::now(tbutil::Time::Realtime);
        //check arrive clock
        if (now.toSeconds() % TASK_PERIOD_SECONDS == 0)
        {
          mutex_task_wait_.lock();
          if (task_wait_.empty())
          {
            mutex_task_wait_.unlock();
            uint32_t num = exp_table.v_exp_table_.size();

            for (uint32_t i = 0; i < num; i++)
            {
              // if es has handled in note_interval second, should record in tair
              ExpireTaskType type = RAW;
              int32_t note_interval = 200;
              ExpireDeleteTask del_task(num, i, now.toSeconds(), 0, note_interval, type);
              mutex_task_wait_.lock();
              task_wait_.push_back(del_task);
              mutex_task_wait_.unlock();
            }
          }
          else
          {
            TBSYS_LOG(INFO, "still have %lu task wait to assign", task_wait_.size());
          }
        }

        mutex_task_wait_.lock();
        if (!exp_table.v_idle_table_.empty() && !task_wait_.empty())
        {
          mutex_task_wait_.unlock();
          for (uint32_t i = 0; i < exp_table.v_idle_table_.size(); i++)
          {
            mutex_task_wait_.lock();
            ExpireDeleteTask del_task = task_wait_.front();
            task_wait_.pop_front();
            mutex_task_wait_.unlock();

            ret = assign(exp_table.v_idle_table_[i], del_task);

            if (TFS_SUCCESS != ret)
            {
              mutex_task_wait_.lock();
              task_wait_.push_back(del_task);
              mutex_task_wait_.unlock();
            }
            else
            {
              mutex_task_.lock();
              TASK_INFO_ITER iter = m_task_info_.find(exp_table.v_idle_table_[i]);
              if (iter == m_task_info_.end())
              {
                m_task_info_[exp_table.v_idle_table_[i]] = del_task;
              }
              else
              {
                TBSYS_LOG(ERROR, "%s has task, should not assign task",
                    tbsys::CNetUtil::addrToString(iter->first).c_str());
              }
              mutex_task_.unlock();
            }
          }
        }

        sleep(1);
      }

      return ret;
    }

    int32_t ExpRootServer::handle_finish_task(common::BasePacket *packet)
    {
      int32_t iret = NULL != packet ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        StatusMessage* rsp = new StatusMessage();

        ReqFinishTaskFromEsMessage *msg = dynamic_cast<ReqFinishTaskFromEsMessage*>(packet);
        uint64_t es_id = msg->get_es_id();

        mutex_task_.lock();
        TASK_INFO_ITER iter = m_task_info_.find(es_id);
        if (iter != m_task_info_.end())
        {
          m_task_info_.erase(iter);
        }
        else
        {
          TBSYS_LOG(ERROR, "fatal error, rts has no %"PRI64_PREFIX"u assign task" , es_id);
        }
        mutex_task_.unlock();

        if (TFS_SUCCESS == iret)
        {
          iret = packet->reply(rsp);
        }
        else
        {
          tbsys::gDelete(rsp);
        }
      }

      return iret;
    }

    int ExpRootServer::handle_fail_servers(common::VUINT64 &down_servers)
    {
      mutex_task_.lock();
      for (uint32_t i = 0; i < down_servers.size(); i++)
      {
        TASK_INFO_ITER iter;
        if ((iter = m_task_info_.find(down_servers[i])) != m_task_info_.end())
        {
          ExpireDeleteTask del_task = iter->second;
          m_task_info_.erase(iter);
          mutex_task_wait_.lock();
          task_wait_.push_back(del_task);
          mutex_task_wait_.unlock();
        }
      }
      mutex_task_.unlock();
      return TFS_SUCCESS;
    }

    int32_t ExpRootServer::query_progress(common::BasePacket *packet)
    {
      int32_t iret = NULL != packet ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        RspQueryProgressMessage* rsp = new RspQueryProgressMessage();

        ReqQueryProgressMessage *msg = dynamic_cast<ReqQueryProgressMessage*>(packet);
        uint64_t es_id = msg->get_es_id();
        int32_t es_num = msg->get_es_num();
        int32_t task_time = msg->get_task_time();
        int32_t hash_bucket_id = msg->get_hash_bucket_id();
        ExpireTaskType type = msg->get_expire_task_type();

        int64_t sum_file_num;
        int32_t current_percent;
        iret = query_task_helper_.query_progress(es_id, es_num, task_time, hash_bucket_id, type,
            &sum_file_num, &current_percent);

        if (TFS_SUCCESS == iret)
        {
          rsp->set_sum_file_num(sum_file_num);
          rsp->set_current_percent(current_percent);
          iret = packet->reply(rsp);
        }
        else
        {
          tbsys::gDelete(rsp);
        }
      }

      return iret;
    }

    void ExpRootServer::AssignTaskThreadHelper::run()
    {
      manager_.assign_task();
    }
  } /** exprootserver **/
}/** tfs **/

