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
#include <atomic.h>

#include "ns_define.h"
#include "common/error_msg.h"
#include "message/client.h"
#include "common/config.h"
#include "common/config_item.h"
#include "meta_manager.h"
#include "nameserver.h"
#include "oplog_sync_manager.h"
#include "message/client_pool.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace nameserver
  {

    FlushOpLogTimerTask::FlushOpLogTimerTask(MetaManager* mm) :
      meta_mgr_(mm)
    {

    }

    void FlushOpLogTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      OpLogSyncManager* opLogSync = meta_mgr_->get_oplog_sync_mgr();
      if ((ngi->owner_role_ != NS_ROLE_MASTER) || (opLogSync->is_destroy_))
      {
        TBSYS_LOG(DEBUG, " wait for flush oplog....");
        return;
      }

      if (ngi->other_side_status_ != NS_STATUS_INITIALIZED || ngi->sync_oplog_flag_ == NS_SYNC_DATA_FLAG_NO)
      {
        //wait
        TBSYS_LOG(DEBUG, " wait for flush oplog....");
        return;
      }
      const int iret = opLogSync->flush_oplog();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "flush oplog to filequeue failed(%d)...", iret);
      }
    }

    OpLogSyncManager::OpLogSyncManager(MetaManager* mm) :
      is_destroy_(false), meta_mgr_(mm), ns_fs_image_(mm), oplog_(NULL), file_queue_(NULL), file_queue_thread_(NULL)
    {

    }

    OpLogSyncManager::~OpLogSyncManager()
    {
      tbsys::gDelete( file_queue_);
      tbsys::gDelete( file_queue_thread_);
      tbsys::gDelete( oplog_);
    }

    int OpLogSyncManager::initialize(LayoutManager& block_ds_map)
    {
      int iret = TFS_SUCCESS;
      //initializeation filequeue, filequeuethread
      char path[MAX_PATH_LENGTH];
      std::string file_queue_name = "oplogsync";

      char default_work_dir[MAX_PATH_LENGTH];
      snprintf(default_work_dir, MAX_PATH_LENGTH,  "%s/nameserver",  CONFIG.get_string_value(CONFIG_PUBLIC, CONF_WORK_DIR));
      const char* work_dir = CONFIG.get_string_value(CONFIG_NAMESERVER, CONF_WORK_DIR, default_work_dir);
      snprintf(path, MAX_PATH_LENGTH, "%s/filequeue", work_dir);
      std::string file_path(path);
      ARG_NEW(file_queue_, FileQueue, file_path, file_queue_name, FILE_QUEUE_MAX_FILE_SIZE);
      ARG_NEW(file_queue_thread_, FileQueueThread, file_queue_, this);

      //initializeation oplog
      int32_t max_slots_size = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_OPLOG_SYSNC_MAX_SLOTS_NUM);
      if (max_slots_size <= 0)
        max_slots_size = 1024;//40KB = 40(bytes) * 1024(slots size)
      if (max_slots_size > 4096)
        max_slots_size = 0x03;//100KB
      std::string queue_header_path = std::string(path) + "/" + file_queue_name;
      ARG_NEW(oplog_, OpLog, queue_header_path, max_slots_size);
      iret = oplog_->initialize();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialization oplog fail...");
        return iret;
      }
      const OpLogRotateHeader* head = oplog_->get_oplog_rotate_header();

      //initializeation fsimage
      std::string tmp(path);
      iret = ns_fs_image_.initialize(block_ds_map, *head, file_queue_);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, " load block information form file errors..");
        return iret;
      }

      //reset filequeue
      file_queue_->set_delete_file_flag(false);
      file_queue_->clear();
      const QueueInformationHeader* qhead = file_queue_->get_queue_information_header();
      OpLogRotateHeader rotmp;
      rotmp.rotate_seqno_ = qhead->write_seqno_;
      rotmp.rotate_offset_ = qhead->write_filesize_;

      //update rotate header information
      oplog_->update_oplog_rotate_header(rotmp);

      // add flush oplog timer
      FlushOpLogTimerTaskPtr foltt = new FlushOpLogTimerTask(meta_mgr_);
      //TODO
      meta_mgr_->get_fs_name_system()->get_timer()->scheduleRepeated(foltt, tbutil::Time::seconds(
          SYSPARAM_NAMESERVER.heart_interval_));

      file_queue_thread_->initialize(1, OpLogSyncManager::do_sync_oplog);
      const int queue_thread_num = CONFIG.get_int_value(CONFIG_NAMESERVER, CONF_OPLOGSYNC_THREAD_NUM, 1);
      set_thread_parameter(this, queue_thread_num, NULL);
      start();
      TBSYS_LOG(DEBUG, "start ns's log sync");
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::wait_for_shut_down()
    {
      if (file_queue_thread_ != NULL)
      {
        file_queue_thread_->wait();
      }
      CDefaultRunnable::wait();
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::destroy()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      monitor_.notifyAll();
      is_destroy_ = true;
      if (file_queue_thread_ != NULL)
      {
        file_queue_thread_->destroy();
      }
      stop();
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::register_slots(const char* const data, const int64_t length)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if ((ngi->owner_role_ != NS_ROLE_MASTER) || (is_destroy_))
      {
        return EXIT_REGISTER_OPLOG_SYNC_ERROR;
      }

      file_queue_thread_->write(data, length);
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::register_msg(const Message* msg)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if ((ngi->owner_role_ != NS_ROLE_MASTER) || (ngi->other_side_status_ < NS_STATUS_ACCEPT_DS_INFO)//slave dead or uninitialize
          || (ngi->sync_oplog_flag_ < NS_SYNC_DATA_FLAG_READY) || (is_destroy_))
      {
        return EXIT_REGISTER_OPLOG_SYNC_ERROR;
      }

      Message* smsg = ClientManager::gClientManager.factory_.clone_message(const_cast<Message*> (msg), 2, true);
      if (smsg != NULL)
        push(smsg);
      return TFS_SUCCESS;
    }

    void OpLogSyncManager::notify_all()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      monitor_.notifyAll();
    }

    int OpLogSyncManager::rotate()
    {
      int iret = ns_fs_image_.save(meta_mgr_->get_block_ds_mgr());
      if (iret == TFS_SUCCESS)
      {
        TBSYS_LOG(INFO, "save the file(fsimage) successfully...");
        const QueueInformationHeader* head = file_queue_->get_queue_information_header();
        OpLogRotateHeader ophead;
        ophead.rotate_seqno_ = head->write_seqno_;
        ophead.rotate_offset_ = head->write_filesize_;
        const OpLogRotateHeader* tmp = oplog_->get_oplog_rotate_header();
        oplog_->update_oplog_rotate_header(ophead);
        TBSYS_LOG(
            INFO,
            "queue header: readseqno(%d), read_offset(%d), write_seqno(%d),\
                          write_file_size(%d), queuesize(%d). oplogheader:, rotate_seqno(%d)\
                          rotate_offset(%d), last_rotate_seqno(%d), last_rotate_offset(%d)",
            head->read_seqno_, head->read_offset_, head->write_seqno_, head->write_filesize_, head->queue_size_,
            ophead.rotate_seqno_, ophead.rotate_offset_, tmp->rotate_seqno_, tmp->rotate_offset_);
      }
      else
      {
        TBSYS_LOG(ERROR, "save the file(fsimage)failed...");
      }
      return iret;
    }

    //only debug
    std::string OpLogSyncManager::printDsList(const VUINT64& ds_list)
    {
      std::string ipstr;
      size_t iSize = ds_list.size();
      for (size_t i = 0; i < iSize; ++i)
      {
        ipstr += tbsys::CNetUtil::addrToString(ds_list[i]);
        ipstr += "/";
      }
      return ipstr;
    }

    int OpLogSyncManager::flush_oplog()
    {
      tbutil::Mutex::Lock lock(mutex_);
      time_t iNow = time(NULL);
      if (oplog_->finish(iNow, true))
      {
        register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
        TBSYS_LOG(DEBUG, "oplog size(%d)", oplog_->get_slots_offset());
        oplog_->reset(iNow);
      }
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::log(const BlockInfo* const block_info, int32_t type, const VUINT64& ds_list)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if (ngi->owner_role_ != NS_ROLE_MASTER)
      {
        return TFS_SUCCESS;
      }
      int iret = 0;
      int count = 0;
      tbutil::Mutex::Lock lock(mutex_);
      do
      {
        ++count;
        iret = oplog_->write(type, block_info, ds_list);
        if (iret != TFS_SUCCESS)
        {
          if (iret == EXIT_SLOTS_OFFSET_SIZE_ERROR)
          {
            register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
            TBSYS_LOG(DEBUG, "oplog size(%d)", oplog_->get_slots_offset());
            oplog_->reset();
          }
        }
      }
      while (count < 0x03 && iret != TFS_SUCCESS);
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "write log file, block id(%u)", block_info->block_id_);
        return iret;
      }
      if (oplog_->finish(0))
      {
        register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
        TBSYS_LOG(DEBUG, "oplog size(%d)", oplog_->get_slots_offset());
        oplog_->reset();
      }
      return iret;
    }

    int OpLogSyncManager::do_sync_oplog(const void* const data, const int64_t len, const int32_t, void *arg)
    {
      OpLogSyncManager* op_log_sync = static_cast<OpLogSyncManager*> (arg);
      return op_log_sync->do_sync_oplog(static_cast<const char* const > (data), len);
    }

    int OpLogSyncManager::do_sync_oplog(const char* const data, const int64_t length)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if ((ngi->owner_role_ != NS_ROLE_MASTER) || (is_destroy_))
      {
        file_queue_->clear();
        TBSYS_LOG(INFO, " wait for sync oplog");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.wait();
        return TFS_SUCCESS;
      }

      if (ngi->other_side_status_ < NS_STATUS_INITIALIZED || ngi->sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES)
      {
        //wait
        TBSYS_LOG(INFO, " wait for sync oplog");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.wait();
      }

      //to send data to the slave & wait
      OpLogSyncMessage msg;
      msg.set_data(data, length);
      Message* rmsg = NULL;
      int32_t count = 0x00;
      do
      {
        ++count;
        const int iret = send_message_to_server(ngi->other_side_ip_port_, &msg, &rmsg);
        if ((iret == TFS_SUCCESS) && (rmsg != NULL)) //success
        {
          OpLogSyncResponeMessage* tmsg = dynamic_cast<OpLogSyncResponeMessage*> (rmsg);
          if (tmsg->getPCode() == OPLOG_SYNC_RESPONSE_MESSAGE && tmsg->get_complete_flag()
              == OPLOG_SYNC_MSG_COMPLETE_YES)
          {
            tbsys::gDelete(rmsg);
            return TFS_SUCCESS;
          }
        }
        tbsys::gDelete(rmsg);
      }
      while (count < 0x03);
      tbutil::Mutex::Lock lock(*ngi);
      ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
      TBSYS_LOG(WARN, "synchronization oplog(%s) message failed, count(%d)", data, count);
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::execute(const Message* message, const void* args)
    {
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      int iret = TFS_SUCCESS;
      if (ngi->owner_role_ == NS_ROLE_MASTER) //master
        iret = do_master_msg(message, args);
      else if (ngi->owner_role_ == NS_ROLE_SLAVE) //slave
        iret = do_slave_msg(message, args);
      tbsys::gDelete(message);
      return iret;
    }

    int OpLogSyncManager::do_sync_oplog(const Message* message, const void*)
    {
      if (is_destroy_)
        return TFS_SUCCESS;

      const OpLogSyncMessage* msg = dynamic_cast<const OpLogSyncMessage*> (message);
      const char* data = msg->get_data();
      uint32_t length = msg->get_length();
      uint32_t offset = 0;
      uint32_t ds_list_size = 0;
      BlockInfo *block_info = NULL;
      BlockCollect* block_collect = NULL;
      BlockInfo* dest_block = NULL;
      VUINT64 ds_list;
			TBSYS_LOG(DEBUG, "do slave oplog msg(%d)", length);
      do
      {
        ds_list.clear();
        OpLogHeader* header = reinterpret_cast<OpLogHeader*> (const_cast<char*> (data + offset));
        offset += sizeof(OpLogHeader);
        block_info = reinterpret_cast<BlockInfo*> (const_cast<char*> (data + offset));
        offset += BLOCKINFO_SIZE;
        ds_list_size = data[offset];
        offset += 0x01;
        uint64_t dsSvrId;
        for (size_t i = 0; i < ds_list_size; i++)
        {
          memcpy(&dsSvrId, (data + offset), INT64_SIZE);
          offset += INT64_SIZE;
          ds_list.push_back(dsSvrId);
        }

        std::string dsIpList = OpLogSyncManager::printDsList(ds_list);
        TBSYS_LOG(DEBUG,
            "time(%"PRI64_PREFIX"d), seqno(%d), length(%d), cmd(%s), BlockInfo: id(%u), version(%d), file_count(%d), size(%d),"
            "delete_file_count(%d), delete_size(%d), seqno(%d), ds(%s)", header->time_, header->seqno_, header->length_,
            header->cmd_ == OPLOG_INSERT ? "insert" : header->cmd_ == OPLOG_UPDATE ? "update" : header->cmd_
            == OPLOG_REMOVE ? "remove" : "unknow", block_info->block_id_, block_info->version_,
            block_info->file_count_, block_info->size_, block_info->del_file_count_, block_info->del_size_,
            block_info->seq_no_, dsIpList.c_str());
        switch (header->cmd_)
        {
        case OPLOG_UPDATE:
          meta_mgr_->update_block_info(*block_info, ds_list[0], false); // update block information
          break;
        case OPLOG_INSERT:
          block_collect = meta_mgr_->get_block_ds_mgr().get_block_collect(block_info->block_id_);
          if (!block_collect)
            block_collect = meta_mgr_->get_block_ds_mgr().create_block_collect(block_info->block_id_);
          dest_block = const_cast<BlockInfo*> (block_collect->get_block_info());
          ::memcpy(dest_block, block_info, BLOCKINFO_SIZE);
          for (size_t i = 0; i < ds_list_size; ++i)
          {
            meta_mgr_->get_block_ds_mgr().build_ds_block_relation(block_info->block_id_, ds_list[i], false);
          }
          break;
        }
      }
      while (length > sizeof(OpLogHeader) && length > offset);
      OpLogSyncResponeMessage* rmsg = NULL;
      ARG_NEW(rmsg, OpLogSyncResponeMessage);
      rmsg->set_complete_flag();
      const_cast<Message*> (message)->reply_message(rmsg);
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::do_master_msg(const Message* msg, const void*)
    {
      if (is_destroy_)
      {
        return TFS_SUCCESS;
      }
      NsRuntimeGlobalInformation* ngi = meta_mgr_->get_fs_name_system()->get_ns_global_info();
      if (ngi->other_side_status_ < NS_STATUS_INITIALIZED || ngi->sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES)
      {
        //wait
        TBSYS_LOG(DEBUG, "wait for sync message");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.wait();
      }
      int32_t count(0);
      do
      {
        const int iret = post_message_to_server(ngi->other_side_ip_port_, const_cast<Message*> (msg));
        if (iret == TFS_SUCCESS)
        {
          return TFS_SUCCESS;
        }
        ++count;
      }
      while (count < 0x03);
      tbutil::Mutex::Lock lock(*ngi);
      ngi->sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
      TBSYS_LOG(WARN, "synchronization operation(%s) message failed, count(%d)", NULL, count);
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::do_slave_msg(const Message* msg, const void* args)
    {
      return do_sync_oplog(msg, args);
    }
  }//end namespace nameserver
}//end namespace tfs
