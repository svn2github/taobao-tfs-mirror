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
#include <Mutex.h>
#include "ns_define.h"
#include "common/error_msg.h"
#include "heart_manager.h"
#include "global_factory.h"
#include "message/client_manager.h"
#include <tbsys.h>

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;

namespace tfs
{
  namespace nameserver
  {

    HeartManagement::HeartManagement(LayoutManager& m) :
      meta_mgr_(m)
    {
    }

    HeartManagement::~HeartManagement()
    {
    }

    int HeartManagement::initialize(int32_t thread_count, int32_t max_queue_size)
    {
      max_queue_size_ = max_queue_size;
      work_thread_.setThreadParameter(thread_count, this, this);
      work_thread_.start();
      return TFS_SUCCESS;
    }

    void HeartManagement::wait_for_shut_down()
    {
      work_thread_.wait();
    }

    void HeartManagement::destroy()
    {
      work_thread_.stop(true);
    }

    /**
     * push do lot of things.
     * first call base_type::push check if current processing queue size > max_queue_size_
     * if true cannot processing this heart message, directly response to client with busy repsonse.
     * pay special attention to free the message...
     */
    int HeartManagement::push(Message* msg)
    {
      bool bret = msg != NULL;
      if (bret)
      {
        SetDataserverMessage* message = dynamic_cast<SetDataserverMessage*> (msg);
        int32_t max_queue_size = max_queue_size_;

        // normal heartbeat or dead heartbeat, just push
        if ((message->get_ds().status_ == DATASERVER_STATUS_DEAD)
            || (message->get_has_block() == HAS_BLOCK_FLAG_NO))
        {
          max_queue_size = 0;
        }

        // cannot blocking!
        bool handled = work_thread_.push(message, max_queue_size, false);
        if (handled)
        {
          return TFS_SUCCESS;
        }
        //threadpool busy..cannot handle it
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(WARN), STATUS_MESSAGE_ERROR, 0,
            "nameserver heartbeat busy! cannot accept this request from (%s)", tbsys::CNetUtil::addrToString(
                message->get_connection()->getPeerId()).c_str());
        // already repsonse, now can free this message object.
        message->free();
        return EXIT_GENERAL_ERROR;
      }
      return EXIT_GENERAL_ERROR;
   }

    // event handler
    bool HeartManagement::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        //if return TFS_SUCCESS, packet had been delete in this func
        // if handlePacketQueue return true, tbnet will delete this packet
        return keepalive(packet) != TFS_SUCCESS; 
      }
      return false;
    }

    int HeartManagement::keepalive(tbnet::Packet* packet)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        #ifdef TFS_NS_DEBUG
        tbutil::Time begin = tbutil::Time::now();
        #endif
        SetDataserverMessage* message = dynamic_cast<SetDataserverMessage*> (packet);
        RespHeartMessage *result_msg = new RespHeartMessage();
        DataServerStatInfo& ds_info = message->get_ds();
			  VUINT32 expires;
			  bool need_sent_block = false;
			  const HasBlockFlag flag = message->get_has_block();

			  int iret = meta_mgr_.keepalive(ds_info, flag, message->get_blocks(), expires, need_sent_block); 
			  if (iret != TFS_SUCCESS)
			  {
			  	TBSYS_LOG(ERROR, "dataserver(%s) keepalive failed", CNetUtil::addrToString(ds_info.id_).c_str());
			  	result_msg->set_status(STATUS_MESSAGE_ERROR);
			  	goto Response;
			  }

			  //dataserver dead
			  if (ds_info.status_ == DATASERVER_STATUS_DEAD)
			  {
			  	TBSYS_LOG(INFO, "dataserver(%s) exit", CNetUtil::addrToString(ds_info.id_).c_str());
          result_msg->set_status(HEART_MESSAGE_OK);
			  	goto Response;
			  }

			  if (flag == HAS_BLOCK_FLAG_YES)
			  {
			  	if (!expires.empty())
			  	{
			  		result_msg->set_expire_blocks(expires);
        	  result_msg->set_status(HEART_EXP_BLOCK_ID);
			  	}
          else
          {
			      result_msg->set_status(HEART_MESSAGE_OK);
          }
			  	goto Response;
			  }
			  else
			  {
			  	//dataserver need report if dataserver is new dataserver or switching occurrd
			  	if (need_sent_block)
			  	{
        		result_msg->set_status(HEART_NEED_SEND_BLOCK_INFO);
			  		goto Response;
			  	}
			  }
			  result_msg->set_status(HEART_MESSAGE_OK);
			  Response:
        #ifdef TFS_NS_DEBUG
        tbutil::Time end = tbutil::Time::now() - begin;
        TBSYS_LOG(INFO, "dataserver(%s) keepalive times: %"PRI64_PREFIX"d(us), need_sent_block(%d)", CNetUtil::addrToString(ds_info.id_).c_str(), end.toMicroSeconds(), need_sent_block);
        #endif
        TBSYS_LOG(DEBUG,"server(%s) result_msg statu: %d, need_sent_block: %d", tbsys::CNetUtil::addrToString(ds_info.id_).c_str(), result_msg->get_status(), need_sent_block);
			  message->reply_message(result_msg);
			  message->free();
			  return TFS_SUCCESS;
      }
      return EXIT_GENERAL_ERROR;
    }

    CheckOwnerIsMasterTimerTask::CheckOwnerIsMasterTimerTask(LayoutManager* mm) :
      meta_mgr_(mm)
    {

    }
    void CheckOwnerIsMasterTimerTask::master_lost_vip(NsRuntimeGlobalInformation& ngi)
    {
      if (!tbsys::CNetUtil::isLocalAddr(ngi.vip_))
      {
        TBSYS_LOG(WARN, "%s", "the master ns role modify,i'm going to be the slave ns");
        tbutil::Mutex::Lock lock(ngi);
        ngi.owner_role_ = NS_ROLE_SLAVE;
        ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
        ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
        meta_mgr_->destroy_plan();
      }
      return;
    }

    void CheckOwnerIsMasterTimerTask::check_when_slave_hold_vip(NsRuntimeGlobalInformation& ngi)
    {
      int iret = -1;
      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      NsStatus other_side_status = NS_STATUS_OTHERSIDEDEAD;
      NsSyncDataFlag ns_sync_flag = NS_SYNC_DATA_FLAG_NO;

      mashm.set_ip_port(ngi.owner_ip_port_);
      mashm.set_role(ngi.owner_role_);
      mashm.set_status(ngi.owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));

      iret = message::NewClientManager::get_instance().call(ngi.other_side_ip_port_, &mashm, DEFAULT_NETWORK_CALL_TIMEOUT, rmsg);
      if ((iret != TFS_SUCCESS) || (rmsg == NULL)) // otherSide dead
      {
        ns_switch(NS_STATUS_OTHERSIDEDEAD, NS_SYNC_DATA_FLAG_NO);
      }
      else
      {
        if (rmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
        {
          MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
          if (tmsg->get_role() == NS_ROLE_SLAVE)
          {
            if (ngi.other_side_status_ != tmsg->get_status()) // update otherside status
            {
              other_side_status = static_cast<NsStatus> (tmsg->get_status());
              if ((other_side_status == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
              {
                ns_sync_flag = NS_SYNC_DATA_FLAG_YES;
              }
            }
            ns_switch(other_side_status, ns_sync_flag);
          }
          else // otherside role == master, but vip in local
          {
            if (tbsys::CNetUtil::isLocalAddr(ngi.vip_))// make sure vip in local
            {
              TBSYS_LOG(WARN, "%s", "the master ns role modify,i'm going to be the master ns");
              tbutil::Mutex::Lock lock(ngi);
              ngi.owner_role_ = NS_ROLE_MASTER;
              ngi.other_side_role_ = NS_ROLE_SLAVE;
              ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
              meta_mgr_->calc_max_block_id();
              ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
              meta_mgr_->destroy_plan();
              ns_force_modify_other_side();
            }
          }
        }
      }
      tbsys::gDelete(rmsg);
      return;
    }

    void CheckOwnerIsMasterTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));
      if (!tbsys::CNetUtil::isLocalAddr(ngi.vip_)) //vip != local ip
      {
        if (ngi.owner_role_ == NS_ROLE_MASTER)
        {
          master_lost_vip(ngi);
        }
      }
      else //vip == local ip
      {
        if (ngi.owner_role_ == NS_ROLE_MASTER)//master
        {
          if (ngi.other_side_role_ == NS_ROLE_MASTER)
          {
            if (tbsys::CNetUtil::isLocalAddr(ngi.vip_)) // make sure owner role master,set otherside role == NS_ROLE_SLAVE
            {
              ns_force_modify_other_side();
            }
          }
          // owner role == master and otherside role == master
        }
        else //i am hold vip but i am slave now
        {
          check_when_slave_hold_vip(ngi);
        }
      }
      return;
    }

    void CheckOwnerIsMasterTimerTask::ns_switch(const NsStatus& other_side_status, const NsSyncDataFlag& ns_sync_flag)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      TBSYS_LOG(WARN, "the master ns(%s) is dead, i'm going to be the master ns(%s)",
          tbsys::CNetUtil::addrToString(ngi.other_side_ip_port_).c_str(), tbsys::CNetUtil::addrToString(
            ngi.owner_ip_port_).c_str());
      tbutil::Mutex::Lock lock(ngi);
      ngi.owner_role_ = NS_ROLE_MASTER;
      ngi.other_side_role_ = NS_ROLE_SLAVE;
      ngi.other_side_status_ = other_side_status;
      ngi.sync_oplog_flag_ = ns_sync_flag;
      meta_mgr_->calc_max_block_id();
      meta_mgr_->destroy_plan();
      ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
      meta_mgr_->get_oplog_sync_mgr()->notify_all();
      TBSYS_LOG(INFO, "%s", "notify all oplog thread");
      return;
    }

    void CheckOwnerIsMasterTimerTask::ns_force_modify_other_side()
    {
      MasterAndSlaveHeartMessage mashm;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      int iret = -1;
      Message* rmsg = NULL;

      mashm.set_ip_port(ngi.owner_ip_port_);
      mashm.set_role(NS_ROLE_SLAVE);
      mashm.set_status(ngi.other_side_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      mashm.set_force_flags(HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES);
      int count = 0;
      do
      {
        count++;

        iret = message::NewClientManager::get_instance().call(ngi.other_side_ip_port_, &mashm, DEFAULT_NETWORK_CALL_TIMEOUT, rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
          if (tmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            if (tmsg->get_role() == NS_ROLE_SLAVE)
            {
              if (ngi.other_side_status_ != tmsg->get_status()) // update otherside status
              {
                NsStatus other_side_status = static_cast<NsStatus> (tmsg->get_status());
                if ((other_side_status == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
                {
                  ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                  meta_mgr_->get_oplog_sync_mgr()->notify_all();
                  TBSYS_LOG(INFO, "%s", "notify all oplog thread");
                }
              }
              break;
            }
          }
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 3);
      tbsys::gDelete(rmsg);
      return;
    }

    MasterHeartTimerTask::MasterHeartTimerTask(LayoutManager* mm) :
      meta_mgr_(mm)
    {

    }

    void MasterHeartTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if ((ngi.owner_role_ != NS_ROLE_MASTER) || (ngi.owner_status_ != NS_STATUS_INITIALIZED))
      {
        return;
      }

      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      mashm.set_ip_port(ngi.owner_ip_port_);
      mashm.set_role(ngi.owner_role_);
      mashm.set_status(ngi.owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));
      int count(0);
      do
      {
        ++count;
        int32_t iret = message::NewClientManager::get_instance().call(ngi.other_side_ip_port_, &mashm, DEFAULT_NETWORK_CALL_TIMEOUT, rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          if (rmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
            if (tmsg->get_role() == NS_ROLE_SLAVE)
            {
              if (ngi.other_side_status_ != tmsg->get_status()) // update otherside status
              {
                tbutil::Mutex::Lock lock(ngi);
                if (ngi.other_side_status_ != tmsg->get_status())
                {
                  ngi.other_side_status_ = static_cast<NsStatus> (tmsg->get_status());
                  if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_
                        < NS_SYNC_DATA_FLAG_YES))
                  {
                    ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                    meta_mgr_->get_oplog_sync_mgr()->notify_all();
                    TBSYS_LOG(INFO, "%s", "notify all oplog thread");
                  }
                }
              }
              else
              {
                if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_
                    < NS_SYNC_DATA_FLAG_YES))
                {
                  tbutil::Mutex::Lock lock(ngi);
                  if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_
                        < NS_SYNC_DATA_FLAG_YES))
                  {
                    ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                    meta_mgr_->get_oplog_sync_mgr()->notify_all();
                    TBSYS_LOG(INFO, "%s", "notify all oplog thread");
                  }
                }
              }
              tbsys::gDelete(rmsg);
              return;
            }
            TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi.owner_role_
                == NS_ROLE_MASTER ? "master" : "slave", tmsg->get_role() == NS_ROLE_MASTER ? "master" : "slave");
          }
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);

      //slave dead
      TBSYS_LOG(WARN, "slave(%s) dead", tbsys::CNetUtil::addrToString(ngi.other_side_ip_port_).c_str());
      tbutil::Mutex::Lock lock(ngi);
      ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
      ngi.other_side_status_ = NS_STATUS_UNINITIALIZE;
    }

    SlaveHeartTimerTask::SlaveHeartTimerTask(LayoutManager* mm, tbutil::TimerPtr& timer) :
      meta_mgr_(mm), timer_(timer)
    {

    }

    void SlaveHeartTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if ((ngi.owner_role_ != NS_ROLE_SLAVE) || (ngi.owner_status_ != NS_STATUS_INITIALIZED))
      {
        return;
      }
      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      mashm.set_ip_port(ngi.owner_ip_port_);
      mashm.set_role(ngi.owner_role_);
      mashm.set_status(ngi.owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      int count(0);
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));
      do
      {
        ++count;
        const int32_t iret = message::NewClientManager::get_instance().call(ngi.other_side_ip_port_, &mashm, DEFAULT_NETWORK_CALL_TIMEOUT, rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          if (rmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
            if (tmsg->get_role() == NS_ROLE_MASTER) //i am slave
            {
              if (ngi.other_side_status_ != tmsg->get_status()) // update otherside status
              {
                tbutil::Mutex::Lock lock(ngi);
                ngi.other_side_status_ = static_cast<NsStatus> (tmsg->get_status());
              }
              tbsys::gDelete(rmsg);
              return;
            }
            TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi.owner_role_
                == NS_ROLE_MASTER ? "master" : "slave", tmsg->get_role() == NS_ROLE_MASTER ? "master" : "slave");
          }
        }
        if (tbsys::CNetUtil::isLocalAddr(ngi.vip_)) // vip == local ip
        {
          TBSYS_LOG(WARN, "%s", "the master ns is dead,i'm going to be the master ns");
          tbutil::Mutex::Lock lock(ngi);
          ngi.owner_role_ = NS_ROLE_MASTER;
          ngi.other_side_role_ = NS_ROLE_SLAVE;
          ngi.other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
          meta_mgr_->calc_max_block_id();
          ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
          meta_mgr_->destroy_plan();
          tbsys::gDelete(rmsg);
          return;
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);
      tbsys::gDelete(rmsg);

      //make sure master dead
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
      }

      do
      {
        TBSYS_LOG(DEBUG, "%s", "the master ns is dead,check vip...");
        if (tbsys::CNetUtil::isLocalAddr(ngi.vip_)) // vip == local ip
        {
          TBSYS_LOG(WARN, "%s", "the master ns is dead,I'm going to be the master ns");
          tbutil::Mutex::Lock lock(ngi);
          ngi.owner_role_ = NS_ROLE_MASTER;
          ngi.other_side_role_ = NS_ROLE_SLAVE;
          ngi.other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
          meta_mgr_->calc_max_block_id();
          ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
          meta_mgr_->destroy_plan();
          break;
        }
        usleep(0x01);
      }
      while (ngi.other_side_status_ != NS_STATUS_INITIALIZED && ngi.owner_status_ == NS_STATUS_INITIALIZED);
    }

    MasterAndSlaveHeartManager::MasterAndSlaveHeartManager(LayoutManager* mm, tbutil::TimerPtr& timer) :
      meta_mgr_(mm), timer_(timer)
    {

    }

    MasterAndSlaveHeartManager::~MasterAndSlaveHeartManager()
    {

    }

    int MasterAndSlaveHeartManager::initialize()
    {
      work_thread_.setThreadParameter(1, this, this);
      work_thread_.start();
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::wait_for_shut_down()
    {
      work_thread_.wait();
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::destroy()
    {
      work_thread_.stop(true);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::push(message::Message* message, int32_t max_queue_size, bool block)
    {
      return work_thread_.push(message, max_queue_size, block);
    }

    bool MasterAndSlaveHeartManager::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        Message* message = dynamic_cast<Message*>(packet);
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        int iret = TFS_SUCCESS;

        if (message->getPCode() == HEARTBEAT_AND_NS_HEART_MESSAGE)
        {
          iret = do_heartbeat_and_ns_msg(message, args);//check heartbeat and nameserver heart message
          tbsys::gDelete(message);
          return false; //so tbnet will not delete packet again
        }
        if (ngi.owner_role_ == NS_ROLE_MASTER) //master
          iret = do_master_msg(message, args);
        else if (ngi.owner_role_ == NS_ROLE_SLAVE) //slave
          iret = do_slave_msg(message, args);
        tbsys::gDelete(message);
        return false;//so tbnet will not delete packet again
      }
      return false;
    }

    int MasterAndSlaveHeartManager::do_master_msg(Message* message, void*)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));
      MasterAndSlaveHeartMessage* mashm = dynamic_cast<MasterAndSlaveHeartMessage*> (message);
      if ((mashm->get_force_flags() == HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES) 
          && (ngi.other_side_ip_port_ == mashm->get_ip_port()) && (ngi.owner_role_ != mashm->get_role()))
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.owner_role_ = static_cast<NsRole> (mashm->get_role());
        ngi.other_side_role_ = ngi.owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
        ngi.owner_status_ = static_cast<NsStatus> (mashm->get_status());
        ngi.sync_oplog_flag_ = ngi.owner_role_ == NS_ROLE_MASTER ? NS_SYNC_DATA_FLAG_YES : NS_SYNC_DATA_FLAG_NO;
        if (ngi.sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES)
          meta_mgr_->get_oplog_sync_mgr()->notify_all();

        TBSYS_LOG(DEBUG, "other side modify owner status, owner status(%s), notify all oplog thread",
            ngi.owner_status_ == NS_STATUS_ACCEPT_DS_INFO ? "acceptdsinfo" : ngi.owner_status_
                == NS_STATUS_INITIALIZED ? "initialized"
                : ngi.other_side_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : "unknow");
        goto reply_message_and_exit; 
      }
      if (mashm->get_role() != NS_ROLE_SLAVE)
      {
        TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi.owner_role_
            == NS_ROLE_MASTER ? "master" : "slave", mashm->get_role() == NS_ROLE_MASTER ? "master" : "slave");
        return TFS_SUCCESS;
      }
      if (ngi.other_side_status_ != mashm->get_status()) //update otherside status
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.other_side_status_ = static_cast<NsStatus> (mashm->get_status());
        if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
        {
          ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
          TBSYS_LOG(INFO, "%s", "notify all oplog thread");
        }
      }
      else
      {
        if ((ngi.other_side_status_ == NS_STATUS_INITIALIZED) && (ngi.sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES))
        {
          tbutil::Mutex::Lock lock(ngi);
          ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
          TBSYS_LOG(INFO, "%s", "notify all oplog thread");
        }
        else if ((ngi.other_side_status_ >= NS_STATUS_ACCEPT_DS_INFO) && (ngi.sync_oplog_flag_
            < NS_SYNC_DATA_FLAG_YES))
        {
          tbutil::Mutex::Lock lock(ngi);
          ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_READY;
        }
      }
      ngi.dump(TBSYS_LOG_LEVEL(DEBUG));
      reply_message_and_exit: MasterAndSlaveHeartResponseMessage *mashrm = new MasterAndSlaveHeartResponseMessage();
      mashrm->set_ip_port(ngi.owner_ip_port_);
      mashrm->set_role(ngi.owner_role_);
      mashrm->set_status(ngi.owner_status_);
      mashrm->set_flags(mashm->get_flags());

      if (mashm->get_flags() == HEART_GET_DATASERVER_LIST_FLAGS_YES)
      {
        TBSYS_LOG(INFO, "%s", "ns(slave) register");
        meta_mgr_->get_oplog_sync_mgr()->get_file_queue_thread()->update_queue_information_header();
        VUINT64 ds_list;
        meta_mgr_->get_alive_server(ds_list);
        mashrm->set_ds_list(ds_list);
      }
      message->reply_message(mashrm);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::do_slave_msg(Message* message, void*)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      MasterAndSlaveHeartMessage* mashm = dynamic_cast<MasterAndSlaveHeartMessage*> (message);
      if ((mashm->get_force_flags() == HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES) && (ngi.other_side_ip_port_
          == mashm->get_ip_port()) && (ngi.owner_role_ != mashm->get_role()))
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.owner_role_ = static_cast<NsRole> (mashm->get_role());
        ngi.other_side_role_ = ngi.owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
        ngi.owner_status_ = static_cast<NsStatus> (mashm->get_status());
        ngi.sync_oplog_flag_ = ngi.owner_role_ == NS_ROLE_MASTER ? NS_SYNC_DATA_FLAG_YES : NS_SYNC_DATA_FLAG_NO;
        if (ngi.sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES)
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
        TBSYS_LOG(DEBUG, "other side modify owner status, owner status(%s), notify all oplog thread",
            ngi.owner_status_ == NS_STATUS_ACCEPT_DS_INFO ? "acceptdsinfo" : ngi.owner_status_
                == NS_STATUS_INITIALIZED ? "initialized"
                : ngi.other_side_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : "unknow");
        goto replay_message_and_exit;
      }
      if (mashm->get_role() != NS_ROLE_MASTER)
      {
        TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi.owner_role_
            == NS_ROLE_MASTER ? "master" : "slave", mashm->get_role() == NS_ROLE_MASTER ? "master" : "slave");
        return TFS_SUCCESS;
      }
      if (ngi.other_side_status_ != mashm->get_status()) //update otherside status
      {
        tbutil::Mutex::Lock lock(ngi);
        ngi.other_side_status_ = static_cast<NsStatus> (mashm->get_status());
      }
      replay_message_and_exit: MasterAndSlaveHeartResponseMessage *mashrm = new MasterAndSlaveHeartResponseMessage();
      mashrm->set_ip_port(ngi.owner_ip_port_);
      mashrm->set_role(ngi.owner_role_);
      mashrm->set_status(ngi.owner_status_);
      mashrm->set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);

      message->reply_message(mashrm);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::do_heartbeat_and_ns_msg(Message* message, void*)
    {
      if (message->getPCode() != HEARTBEAT_AND_NS_HEART_MESSAGE)
        return TFS_SUCCESS;

      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      HeartBeatAndNSHeartMessage* hbam = dynamic_cast<HeartBeatAndNSHeartMessage*> (message);
      int32_t ns_switch_flag = hbam->get_ns_switch_flag();
      TBSYS_LOG(DEBUG, "ns_switch_flag(%s), status(%d)",
          hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no" : "yes", hbam->get_ns_status());
      HeartBeatAndNSHeartMessage* mashrm = new HeartBeatAndNSHeartMessage();
      mashrm->set_ns_switch_flag_and_status(0 /*no use*/ , ngi.owner_status_);
      message->reply_message(mashrm);

      if (ns_switch_flag == NS_SWITCH_FLAG_YES)
      {
        TBSYS_LOG(WARN, "ns_switch_flag(%s), status(%d)", hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no"
            : "yes", hbam->get_ns_status());
        do
        {
          TBSYS_LOG(DEBUG, "%s", "the master ns is dead,check vip...");
          if (tbsys::CNetUtil::isLocalAddr(ngi.vip_)) // vip == local ip
          {
            TBSYS_LOG(WARN, "%s", "the master ns is dead,i'm going to be the master ns");
            tbutil::Mutex::Lock lock(ngi);
            ngi.owner_role_ = NS_ROLE_MASTER;
            ngi.owner_status_ = NS_STATUS_INITIALIZED;
            ngi.other_side_role_ = NS_ROLE_SLAVE;
            ngi.other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
            meta_mgr_->calc_max_block_id();
            ngi.switch_time_ = time(NULL) + SYSPARAM_NAMESERVER.safe_mode_time_;
            meta_mgr_->destroy_plan();
            break;
          }
          usleep(0x01);
        }
        while (ngi.other_side_status_ != NS_STATUS_INITIALIZED && ngi.owner_status_ == NS_STATUS_INITIALIZED);
      }
      return TFS_SUCCESS;
    }
  }
}
