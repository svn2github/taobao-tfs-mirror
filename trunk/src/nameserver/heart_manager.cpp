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
#include "common/interval.h"
#include "common/error_msg.h"
#include "heart_manager.h"
#include "message/client.h"
#include "nameserver.h"

using namespace tfs::common;
using namespace tfs::message;

namespace tfs
{
  namespace nameserver
  {

    HeartManagement::HeartManagement(MetaManager& m, ReplicateLauncher& l) :
      meta_mgr_(m), replicate_lancher_(l)
    {
    }

    HeartManagement::~HeartManagement()
    {
    }

    int HeartManagement::initialize(const int32_t thread_count, const int32_t max_queue_size)
    {
      max_queue_size_ = max_queue_size;
      base_type::set_thread_parameter(this, thread_count, NULL);
      base_type::start();
      return TFS_SUCCESS;
    }

    /**
     * push do lot of things.
     * first call base_type::push check if current processing queue size > max_queue_size_
     * if true cannot processing this heart message, directly response to client with busy repsonse.
     * pay special attention to free the message...
     */
    int HeartManagement::push(Message* msg)
    {
      SetDataserverMessage* message = dynamic_cast<SetDataserverMessage*> (msg);
      int32_t max_queue_size = max_queue_size_;

      // normal heartbeat or dead heartbeat, just push
      if ((message->get_ds()->status_ == DATASERVER_STATUS_DEAD) || (message->get_has_block() == HAS_BLOCK_FLAG_NO))
      {
        max_queue_size = 0;
      }

      // cannot blocking!
      bool handled = base_type::push(message, max_queue_size, false);
      if (handled)
        return TFS_SUCCESS;

      // threadpool busy..cannot handle it
      MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(WARN), STATUS_MESSAGE_ERROR, 0,
          "nameserver heartbeat busy! cannot accept this request from (%s)", tbsys::CNetUtil::addrToString(
              message->get_connection()->getPeerId()).c_str());
      // already repsonse, now can free this message object.
      message->free();
      return EXIT_GENERAL_ERROR;
    }

    // event handler
    int HeartManagement::execute(Message* msg, void*)
    {
      SetDataserverMessage* message = dynamic_cast<SetDataserverMessage*> (msg);
      if (message == NULL)
        return EXIT_GENERAL_ERROR;
      return join_ds(message);
    }

    int HeartManagement::join_ds(Message* msg)
    {
      SetDataserverMessage* message = dynamic_cast<SetDataserverMessage*> (msg);
      DataServerStatInfo *ds_stat_info = message->get_ds();
      uint64_t server_id = ds_stat_info->id_;
      RespHeartMessage *result_msg = new RespHeartMessage();

      if (ds_stat_info->status_ == DATASERVER_STATUS_DEAD)
      {
        meta_mgr_.leave_ds(server_id);
        result_msg->set_status(HEART_MESSAGE_OK);
        message->reply_message(result_msg);
        message->free();
        return TFS_SUCCESS;
      }

      MetaManager::EXPIRE_BLOCK_LIST expire_list;

      bool isnew = false;

      meta_mgr_.join_ds(*ds_stat_info, isnew);
      if (isnew)
      {
        TBSYS_LOG(INFO, "dataserver(%s) join: use capacity(%" PRI64_PREFIX "u),total capacity(%" PRI64_PREFIX "u), has_block(%s)",
            tbsys::CNetUtil::addrToString(server_id).c_str(), ds_stat_info->use_capacity_,
            ds_stat_info->total_capacity_, message->get_has_block() == HAS_BLOCK_FLAG_YES ? "Yes" : "No");
        if (meta_mgr_.get_fs_name_system()->get_ns_global_info()->owner_role_ == NS_ROLE_MASTER)
        {
          replicate_lancher_.inc_stop_balance_count();
        }
      }

      if (message->get_has_block() == HAS_BLOCK_FLAG_YES)
      {
        meta_mgr_.report_blocks(server_id, *message->get_blocks(), expire_list);
        if (meta_mgr_.get_fs_name_system()->get_ns_global_info()->owner_role_ == NS_ROLE_MASTER)
        {
          uint32_t expire_blocks_size = 0;
          uint32_t i = 0;
          MetaManager::EXPIRE_BLOCK_LIST::iterator iter = expire_list.begin();
          for (; iter != expire_list.end(); ++iter)
          {
            if (iter->first == server_id)
            {
              vector < uint32_t > &expire_blocks = iter->second;
              expire_blocks_size = expire_blocks.size();
              for (i = 0; i < expire_blocks_size; ++i)
              {
                if (!replicate_lancher_.get_executor().is_replicating_block(expire_blocks[i]))
                  result_msg->add_expire_id(expire_blocks[i]);
              }
            }
            else
            {
              NameServer::rm_block_from_ds(iter->first, iter->second);
            }
          }
        }
        result_msg->set_status(HEART_EXP_BLOCK_ID);
        TBSYS_LOG(INFO, "dataserver(%s) join: use capacity(%" PRI64_PREFIX "u),total capacity(%" PRI64_PREFIX "u), block count(%u)",
            tbsys::CNetUtil::addrToString(server_id).c_str(), ds_stat_info->use_capacity_,
            ds_stat_info->total_capacity_, message->get_blocks()->size());
      }
      else
      {
        ServerCollect* servre_collect = meta_mgr_.get_block_ds_mgr().get_ds_collect(server_id);
        int32_t block_count = -1;
        if (servre_collect != NULL)
          block_count = servre_collect->get_block_list().size();
        if (isnew || servre_collect->get_block_list().size() == 0)
        {
          TBSYS_LOG(INFO, "reply dataserver(%s) heart msg need send block, isnew(%s),current block count(%u)",
              tbsys::CNetUtil::addrToString(server_id).c_str(), isnew ? "true" : "false", block_count);
          result_msg->set_status(HEART_NEED_SEND_BLOCK_INFO);
        }
        else
        {
          result_msg->set_status(HEART_MESSAGE_OK);
        }

        if ((meta_mgr_.get_fs_name_system()->get_ns_global_info()->switch_time_ != 0) && (block_count
            != ds_stat_info->block_count_))
        {
          TBSYS_LOG(DEBUG, "new ds block count(%d): old ds block count(%d)", ds_stat_info->block_count_, block_count);

          if (time(NULL) < (meta_mgr_.get_fs_name_system()->get_ns_global_info()->switch_time_
              + SYSPARAM_NAMESERVER.ds_dead_time_))
            result_msg->set_status(HEART_NEED_SEND_BLOCK_INFO);
          else
            meta_mgr_.get_fs_name_system()->get_ns_global_info()->switch_time_ = 0;
        }

      }

      message->reply_message(result_msg);
      if (message->get_has_block() == HAS_BLOCK_FLAG_YES)
        meta_mgr_.check_primary_writable_block(server_id, SYSPARAM_NAMESERVER.add_primary_block_count_);

      message->free();
      message = NULL;
      return TFS_SUCCESS;
    }

    CheckOwnerIsMasterTimerTask::CheckOwnerIsMasterTimerTask(MetaManager* mm) :
      meta_mgr_(mm)
    {

    }

    void CheckOwnerIsMasterTimerTask::runTimerTask()
    {
      int count = 0;
      int iret = -1;
      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      NsStatus other_side_status = NS_STATUS_OTHERSIDEDEAD;
      NsSyncDataFlag ns_sync_flag = NS_SYNC_DATA_FLAG_NO;
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      if (!tbsys::CNetUtil::isLocalAddr(ngi->vip_)) //vip != local ip
      {
        if (ngi->owner_role_ == NS_ROLE_MASTER)
        {
          if (!tbsys::CNetUtil::isLocalAddr(ngi->vip_))
          {
            TBSYS_LOG(WARN, "the master ns role modify,i'm going to be the slave ns");
            tbutil::Mutex::Lock lock(*ngi);
            ngi->owner_role_ = NS_ROLE_SLAVE;
            ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
            ngi->switch_time_ = time(NULL);
            return;
          }
        }
        return;
      }

      //vip == local ip
      if (ngi->owner_role_ == NS_ROLE_MASTER)//master
      {
        if (ngi->other_side_role_ != NS_ROLE_MASTER)
        {
          return;
        }
        // owner role == master and otherside role == master
        if (tbsys::CNetUtil::isLocalAddr(ngi->vip_)) // make sure owner role master,set otherside role == NS_ROLE_SLAVE
        {
          goto ns_force_modify_other_side;
        }
        return;
      }
      //slave
      mashm.set_ip_port(ngi->owner_ip_port_);
      mashm.set_role(ngi->owner_role_);
      mashm.set_status(ngi->owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));

      iret = send_message_to_server(ngi->other_side_ip_port_, &mashm, &rmsg);
      if ((iret != TFS_SUCCESS) || (rmsg == NULL)) // otherSide dead
      {
        goto ns_switch;
      }
      if ((iret == TFS_SUCCESS) && (rmsg != NULL))
      {
        MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
        if (tmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
        {
          if (tmsg->get_role() == NS_ROLE_SLAVE)
          {
            if (ngi->other_side_status_ != tmsg->get_status()) // update otherside status
            {
              other_side_status = static_cast<NsStatus> (tmsg->get_status());
              if ((other_side_status == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
              {
                ns_sync_flag = NS_SYNC_DATA_FLAG_YES;
              }
            }
            tbsys::gDelete(rmsg);
            goto ns_switch;
            //switch
          }
          // otherside role == master, but vip in local
          if (tbsys::CNetUtil::isLocalAddr(ngi->vip_))// make sure vip in local
          {
            tbsys::gDelete(rmsg);
            TBSYS_LOG(WARN, "the master ns role modify,i'm going to be the master ns");
            tbutil::Mutex::Lock lock(*ngi);
            ngi->owner_role_ = NS_ROLE_MASTER;
            ngi->other_side_role_ = NS_ROLE_SLAVE;
            ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
            meta_mgr_->get_block_ds_mgr().calc_max_block_id();
            ngi->switch_time_ = time(NULL);
            goto ns_force_modify_other_side;
          }
        }
      }
      tbsys::gDelete(rmsg);
      return;
      ns_switch: TBSYS_LOG(WARN, "the master ns(%s) is dead, i'm going to be the master ns(%s)",
          tbsys::CNetUtil::addrToString(ngi->other_side_ip_port_).c_str(), tbsys::CNetUtil::addrToString(
              ngi->owner_ip_port_).c_str());
      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->owner_role_ = NS_ROLE_MASTER;
        ngi->other_side_role_ = NS_ROLE_SLAVE;
        ngi->other_side_status_ = other_side_status;
        ngi->sync_oplog_flag_ = ns_sync_flag;
        meta_mgr_->get_block_ds_mgr().calc_max_block_id();
        ngi->switch_time_ = time(NULL);
        meta_mgr_->get_oplog_sync_mgr()->notify_all();
        TBSYS_LOG(INFO, "notify all oplog thread");
      }
      return;
      ns_force_modify_other_side: mashm.set_ip_port(ngi->owner_ip_port_);
      mashm.set_role(NS_ROLE_SLAVE);
      mashm.set_status(ngi->other_side_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      mashm.set_force_flags(HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES);
      do
      {
        count++;
        iret = send_message_to_server(ngi->other_side_ip_port_, &mashm, &rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
          if (tmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            if (tmsg->get_role() == NS_ROLE_SLAVE)
            {
              if (ngi->other_side_status_ != tmsg->get_status()) // update otherside status
              {
                other_side_status = static_cast<NsStatus> (tmsg->get_status());
                if ((other_side_status == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
                {
                  ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                  meta_mgr_->get_oplog_sync_mgr()->notify_all();
                  TBSYS_LOG(INFO, "notify all oplog thread");
                }
              }
              tbsys::gDelete(rmsg);
              break;
            }
          }
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);
      return;
    }

    MasterHeartTimerTask::MasterHeartTimerTask(MetaManager* mm) :
      meta_mgr_(mm)
    {

    }

    void MasterHeartTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if ((ngi->owner_role_ != NS_ROLE_MASTER) || (ngi->owner_status_ != NS_STATUS_INITIALIZED))
      {
        return;
      }

      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      mashm.set_ip_port(ngi->owner_ip_port_);
      mashm.set_role(ngi->owner_role_);
      mashm.set_status(ngi->owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      int count(0);
      do
      {
        ++count;
        const int iret = send_message_to_server(ngi->other_side_ip_port_, &mashm, &rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
          if (tmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            if (tmsg->get_role() == NS_ROLE_SLAVE)
            {
              if (ngi->other_side_status_ != tmsg->get_status()) // update otherside status
              {
                tbutil::Mutex::Lock lock(*ngi);
                ngi->other_side_status_ = static_cast<NsStatus> (tmsg->get_status());
                if ((ngi->other_side_status_ == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_
                    < NS_SYNC_DATA_FLAG_YES))
                {
                  ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                  meta_mgr_->get_oplog_sync_mgr()->notify_all();
                  TBSYS_LOG(INFO, "notify all oplog thread");
                }
              }
              else
              {
                if ((ngi->other_side_status_ == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_
                    < NS_SYNC_DATA_FLAG_YES))
                {
                  tbutil::Mutex::Lock lock(*ngi);
                  ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
                  meta_mgr_->get_oplog_sync_mgr()->notify_all();
                  TBSYS_LOG(INFO, "notify all oplog thread");
                }
              }
              tbsys::gDelete(rmsg);
              return;
            }
            TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi->owner_role_
                == NS_ROLE_MASTER ? "master" : "slave", tmsg->get_role() == NS_ROLE_MASTER ? "master" : "slave");
          }
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);

      //slave dead
      TBSYS_LOG(WARN, "slave(%s) dead", tbsys::CNetUtil::addrToString(ngi->other_side_ip_port_).c_str());
      tbutil::Mutex::Lock lock(*ngi);
      ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
      ngi->other_side_status_ = NS_STATUS_UNINITIALIZE;
    }

    SlaveHeartTimerTask::SlaveHeartTimerTask(MetaManager* mm, tbutil::TimerPtr& timer) :
      meta_mgr_(mm), timer_(timer)
    {

    }

    void SlaveHeartTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if ((ngi->owner_role_ != NS_ROLE_SLAVE) || (ngi->owner_status_ != NS_STATUS_INITIALIZED))
      {
        return;
      }
      Message* rmsg = NULL;
      MasterAndSlaveHeartMessage mashm;
      mashm.set_ip_port(ngi->owner_ip_port_);
      mashm.set_role(ngi->owner_role_);
      mashm.set_status(ngi->owner_status_);
      mashm.set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);
      int count(0);
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      do
      {
        ++count;
        const int iret = send_message_to_server(ngi->other_side_ip_port_, &mashm, &rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL))
        {
          MasterAndSlaveHeartResponseMessage* tmsg = dynamic_cast<MasterAndSlaveHeartResponseMessage*> (rmsg);
          if (tmsg->getPCode() == MASTER_AND_SLAVE_HEART_RESPONSE_MESSAGE)
          {
            if (tmsg->get_role() == NS_ROLE_MASTER)
            {
              if (ngi->other_side_status_ != tmsg->get_status()) // update otherside status
              {
                tbutil::Mutex::Lock lock(*ngi);
                ngi->other_side_status_ = static_cast<NsStatus> (tmsg->get_status());
              }
              tbsys::gDelete(rmsg);
              return;
            }
            TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi->owner_role_
                == NS_ROLE_MASTER ? "master" : "slave", tmsg->get_role() == NS_ROLE_MASTER ? "master" : "slave");
          }
        }
        if (tbsys::CNetUtil::isLocalAddr(ngi->vip_)) // vip == local ip
        {
          TBSYS_LOG(WARN, "the master ns is dead,i'm going to be the master ns");
          tbutil::Mutex::Lock lock(*ngi);
          ngi->owner_role_ = NS_ROLE_MASTER;
          ngi->other_side_role_ = NS_ROLE_SLAVE;
          ngi->other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
          meta_mgr_->get_block_ds_mgr().calc_max_block_id();
          ngi->switch_time_ = time(NULL);
          return;
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);
      tbsys::gDelete(rmsg);

      //make sure master dead
      ngi->lock();
      ngi->other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
      ngi->unlock();

      do
      {
        TBSYS_LOG(DEBUG, "the master ns is dead,check vip...");
        if (tbsys::CNetUtil::isLocalAddr(ngi->vip_)) // vip == local ip
        {
          TBSYS_LOG(WARN, "the master ns is dead,I'm going to be the master ns");
          tbutil::Mutex::Lock lock(*ngi);
          ngi->owner_role_ = NS_ROLE_MASTER;
          ngi->other_side_role_ = NS_ROLE_SLAVE;
          ngi->other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
          meta_mgr_->get_block_ds_mgr().calc_max_block_id();
          ngi->switch_time_ = time(NULL);
          break;
        }
        usleep(0x01);
      }
      while (ngi->other_side_status_ != NS_STATUS_INITIALIZED && ngi->owner_status_ == NS_STATUS_INITIALIZED);
    }

    MasterAndSlaveHeartManager::MasterAndSlaveHeartManager(MetaManager* mm, tbutil::TimerPtr& timer) :
      meta_mgr_(mm), timer_(timer)
    {

    }

    MasterAndSlaveHeartManager::~MasterAndSlaveHeartManager()
    {

    }

    int MasterAndSlaveHeartManager::initialize()
    {
      set_thread_parameter(this, 0x01, NULL);
      start();
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::wait_for_shut_down()
    {
      wait();
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::destroy()
    {
      stop(true);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::execute(Message* message, void* args)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      int iret = TFS_SUCCESS;

      if (message->getPCode() == HEARTBEAT_AND_NS_HEART_MESSAGE)
      {
        iret = do_heartbeat_and_ns_msg(message, args);//check heartbeat and nameserver heart message
        tbsys::gDelete(message);
        return TFS_SUCCESS;
      }
      if (ngi->owner_role_ == NS_ROLE_MASTER) //master
        iret = do_master_msg(message, args);
      else if (ngi->owner_role_ == NS_ROLE_SLAVE) //slave
        iret = do_slave_msg(message, args);
      tbsys::gDelete(message);
      return iret;
    }

    int MasterAndSlaveHeartManager::do_master_msg(Message* message, void*)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      MasterAndSlaveHeartMessage* mashm = dynamic_cast<MasterAndSlaveHeartMessage*> (message);
      if ((mashm->get_force_flags() == HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES) && (ngi->other_side_ip_port_
          == mashm->get_ip_port()) && (ngi->owner_role_ != mashm->get_role()))
      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->owner_role_ = static_cast<NsRole> (mashm->get_role());
        ngi->other_side_role_ = ngi->owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
        ngi->owner_status_ = static_cast<NsStatus> (mashm->get_status());
        ngi->sync_oplog_flag_ = ngi->owner_role_ == NS_ROLE_MASTER ? NS_SYNC_DATA_FLAG_YES : NS_SYNC_DATA_FLAG_NO;
        if (ngi->sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES)
          meta_mgr_->get_oplog_sync_mgr()->notify_all();

        TBSYS_LOG(DEBUG, "other side modify owner status, owner status(%s), notify all oplog thread",
            ngi->owner_status_ == NS_STATUS_ACCEPT_DS_INFO ? "acceptdsinfo" : ngi->owner_status_
                == NS_STATUS_INITIALIZED ? "initialized"
                : ngi->owner_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : "unknow");
        goto Next;
      }
      if (mashm->get_role() != NS_ROLE_SLAVE)
      {
        TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi->owner_role_
            == NS_ROLE_MASTER ? "master" : "slave", mashm->get_role() == NS_ROLE_MASTER ? "master" : "slave");
        return TFS_SUCCESS;
      }
      if (ngi->other_side_status_ != mashm->get_status()) //update otherside status
      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->other_side_status_ = static_cast<NsStatus> (mashm->get_status());
        if ((ngi->other_side_status_ == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_ != NS_SYNC_DATA_FLAG_YES))
        {
          ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
          TBSYS_LOG(INFO, "notify all oplog thread");
        }
      }
      else
      {
        if ((ngi->other_side_status_ == NS_STATUS_INITIALIZED) && (ngi->sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES))
        {
          tbutil::Mutex::Lock lock(*ngi);
          ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_YES;
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
          TBSYS_LOG(INFO, "notify all oplog thread");
        }
        else if ((ngi->other_side_status_ >= NS_STATUS_ACCEPT_DS_INFO) && (ngi->sync_oplog_flag_
            < NS_SYNC_DATA_FLAG_YES))
        {
          tbutil::Mutex::Lock lock(*ngi);
          ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_READY;
        }
      }
      ngi->dump(TBSYS_LOG_LEVEL(DEBUG));
      Next: MasterAndSlaveHeartResponseMessage *mashrm = new MasterAndSlaveHeartResponseMessage();
      mashrm->set_ip_port(ngi->owner_ip_port_);
      mashrm->set_role(ngi->owner_role_);
      mashrm->set_status(ngi->owner_status_);
      mashrm->set_flags(mashm->get_flags());

      if (mashm->get_flags() == HEART_GET_DATASERVER_LIST_FLAGS_YES)
      {
        TBSYS_LOG(INFO, "ns(slave) register");
        meta_mgr_->get_oplog_sync_mgr()->get_file_queue_thread()->update_queue_information_header();
        const SERVER_MAP* maps = meta_mgr_->get_block_ds_mgr().get_ds_map();
        VUINT64 dsList;
        GetAliveDataServerList store(dsList);
        std::for_each(maps->begin(), maps->end(), store);
        mashrm->set_ds_list(dsList);
      }
      message->reply_message(mashrm);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::do_slave_msg(Message* message, void*)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      MasterAndSlaveHeartMessage* mashm = dynamic_cast<MasterAndSlaveHeartMessage*> (message);
      if ((mashm->get_force_flags() == HEART_FORCE_MODIFY_OTHERSIDE_ROLE_FLAGS_YES) && (ngi->other_side_ip_port_
          == mashm->get_ip_port()) && (ngi->owner_role_ != mashm->get_role()))
      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->owner_role_ = static_cast<NsRole> (mashm->get_role());
        ngi->other_side_role_ = ngi->owner_role_ == NS_ROLE_MASTER ? NS_ROLE_SLAVE : NS_ROLE_MASTER;
        ngi->owner_status_ = static_cast<NsStatus> (mashm->get_status());
        ngi->sync_oplog_flag_ = ngi->owner_role_ == NS_ROLE_MASTER ? NS_SYNC_DATA_FLAG_YES : NS_SYNC_DATA_FLAG_NO;
        if (ngi->sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES)
          meta_mgr_->get_oplog_sync_mgr()->notify_all();
        TBSYS_LOG(DEBUG, "other side modify owner status, owner status(%s), notify all oplog thread",
            ngi->owner_status_ == NS_STATUS_ACCEPT_DS_INFO ? "acceptdsinfo" : ngi->owner_status_
                == NS_STATUS_INITIALIZED ? "initialized"
                : ngi->owner_status_ == NS_STATUS_OTHERSIDEDEAD ? "other side dead" : "unknow");
        goto Next;
      }
      if (mashm->get_role() != NS_ROLE_MASTER)
      {
        TBSYS_LOG(WARN, "do master and slave heart fail: owner role(%s), other side role(%s)", ngi->owner_role_
            == NS_ROLE_MASTER ? "master" : "slave", mashm->get_role() == NS_ROLE_MASTER ? "master" : "slave");
        return TFS_SUCCESS;
      }
      if (ngi->other_side_status_ != mashm->get_status()) //update otherside status
      {
        tbutil::Mutex::Lock lock(*ngi);
        ngi->other_side_status_ = static_cast<NsStatus> (mashm->get_status());
      }
      Next: MasterAndSlaveHeartResponseMessage *mashrm = new MasterAndSlaveHeartResponseMessage();
      mashrm->set_ip_port(ngi->owner_ip_port_);
      mashrm->set_role(ngi->owner_role_);
      mashrm->set_status(ngi->owner_status_);
      mashrm->set_flags(HEART_GET_DATASERVER_LIST_FLAGS_NO);

      message->reply_message(mashrm);
      return TFS_SUCCESS;
    }

    int MasterAndSlaveHeartManager::do_heartbeat_and_ns_msg(Message* message, void*)
    {
      if (message->getPCode() != HEARTBEAT_AND_NS_HEART_MESSAGE)
        return TFS_SUCCESS;

      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      HeartBeatAndNSHeartMessage* hbam = dynamic_cast<HeartBeatAndNSHeartMessage*> (message);
      int32_t ns_switch_flag = hbam->get_ns_switch_flag();
      TBSYS_LOG(DEBUG, "ns_switch_flag(%s), status(%d)",
          hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no" : "yes", hbam->get_ns_status());
      HeartBeatAndNSHeartMessage* mashrm = new HeartBeatAndNSHeartMessage();
      mashrm->set_ns_switch_flag_and_status(ngi->owner_role_, ngi->owner_status_);
      message->reply_message(mashrm);

      if (ns_switch_flag == NS_SWITCH_FLAG_YES)
      {
        TBSYS_LOG(WARN, "ns_switch_flag(%s), status(%d)", hbam->get_ns_switch_flag() == NS_SWITCH_FLAG_NO ? "no"
            : "yes", hbam->get_ns_status());
        do
        {
          TBSYS_LOG(DEBUG, "the master ns is dead,check vip...");
          if (tbsys::CNetUtil::isLocalAddr(ngi->vip_)) // vip == local ip
          {
            TBSYS_LOG(WARN, "the master ns is dead,i'm going to be the master ns");
            tbutil::Mutex::Lock lock(*ngi);
            ngi->owner_role_ = NS_ROLE_MASTER;
            ngi->owner_status_ = NS_STATUS_INITIALIZED;
            ngi->other_side_role_ = NS_ROLE_SLAVE;
            ngi->other_side_status_ = NS_STATUS_OTHERSIDEDEAD;
            meta_mgr_->get_block_ds_mgr().calc_max_block_id();
            ngi->switch_time_ = time(NULL);
            break;
          }
          usleep(0x01);
        }
        while (ngi->other_side_status_ != NS_STATUS_INITIALIZED && ngi->owner_status_ == NS_STATUS_INITIALIZED);
      }
      return TFS_SUCCESS;
    }
  }
}
