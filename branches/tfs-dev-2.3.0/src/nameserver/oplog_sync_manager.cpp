/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: oplog_sync_manager.cpp 983 2011-10-31 09:59:33Z duanfei $
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
#include <atomic.h>
#include "ns_define.h"
#include "layout_manager.h"
#include "global_factory.h"
#include "common/error_msg.h"
#include "common/base_service.h"
#include "common/config_item.h"
#include "oplog_sync_manager.h"
#include "common/client_manager.h"

using namespace tfs::common;
using namespace tfs::message;
using namespace tbsys;
namespace tfs
{
  namespace nameserver
  {
    FlushOpLogTimerTask::FlushOpLogTimerTask(OpLogSyncManager& manager) :
      manager_(manager)
    {

    }

    void FlushOpLogTimerTask::runTimerTask()
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      int32_t iret = ngi.owner_role_ == NS_ROLE_MASTER && ! manager_.is_destroy_ ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = ngi.other_side_status_ != NS_STATUS_INITIALIZED ? TFS_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == iret)
        {
          iret = manager_.flush_oplog();
          if (TFS_SUCCESS != iret)
          {
            TBSYS_LOG(ERROR, "flush oplog to filequeue failed, iret: %d", iret);
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "%s", " wait for flush oplog....");
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", " wait for flush oplog....");
      }
      return;
    }

    OpLogSyncManager::OpLogSyncManager(LayoutManager& mm) :
      is_destroy_(false), meta_mgr_(mm), oplog_(NULL), file_queue_(NULL), file_queue_thread_(NULL)
    {

    }

    OpLogSyncManager::~OpLogSyncManager()
    {
      tbsys::gDelete( file_queue_);
      tbsys::gDelete( file_queue_thread_);
      tbsys::gDelete( oplog_);
    }

    int OpLogSyncManager::initialize()
    {
      int iret = TFS_ERROR;

      //initializeation filequeue, filequeuethread
      char path[256];
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      const char* work_dir = service->get_work_dir();
      snprintf(path, 256, "%s/nameserver/filequeue", work_dir);
      std::string file_path(path);
      std::string file_queue_name = "oplogsync";
      ARG_NEW(file_queue_, FileQueue, file_path, file_queue_name, FILE_QUEUE_MAX_FILE_SIZE);
      ARG_NEW(file_queue_thread_, FileQueueThread, file_queue_, this);

      //initializeation oplog
      int32_t max_slots_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OPLOG_SYSNC_MAX_SLOTS_NUM);
      max_slots_size = max_slots_size <= 0 ? 1024 : max_slots_size > 4096 ? 4096 : max_slots_size;
      std::string queue_header_path = std::string(path) + "/" + file_queue_name;
      ARG_NEW(oplog_, OpLog, queue_header_path, max_slots_size);
      iret = oplog_->initialize();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize oplog failed, path: %s, iret: %d", queue_header_path.c_str(), iret);
      }

      if (TFS_SUCCESS == iret)
      {
        std::string id_path = std::string(work_dir) + "/nameserver/";
        iret = id_factory_.initialize(id_path);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "initialize block id factory failed, path: %s, iret: %d", id_path.c_str(), iret);
        }
      }

      // replay all oplog
      if (TFS_SUCCESS == iret)
      {
        iret = replay_all();
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "replay all oplogs failed, iret: %d", iret);
        }
      }

      std::string tmp(path);
      //reset filequeue
      if (TFS_SUCCESS == iret)
      {
        file_queue_->set_delete_file_flag(false);
        file_queue_->clear();
        const QueueInformationHeader* qhead = file_queue_->get_queue_information_header();
        OpLogRotateHeader rotmp;
        rotmp.rotate_seqno_ = qhead->write_seqno_;
        rotmp.rotate_offset_ = qhead->write_filesize_;

        //update rotate header information
        oplog_->update_oplog_rotate_header(rotmp);
      }

      // add flush oplog timer
      if (TFS_SUCCESS == iret)
      {
        FlushOpLogTimerTaskPtr foltt = new FlushOpLogTimerTask(*this);
        GFactory::get_timer()->scheduleRepeated(foltt, tbutil::Time::seconds(
            SYSPARAM_NAMESERVER.heart_interval_));
      }

      if (TFS_SUCCESS == iret)
      {
        file_queue_thread_->initialize(1, OpLogSyncManager::do_sync_oplog);
        const int queue_thread_num = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OPLOGSYNC_THREAD_NUM, 1);
        work_thread_.setThreadParameter(queue_thread_num , this, NULL);
        work_thread_.start();
      }
      return iret;
    }

    int OpLogSyncManager::wait_for_shut_down()
    {
      if (file_queue_thread_ != NULL)
      {
        file_queue_thread_->wait();
      }
      work_thread_.wait();

      return id_factory_.destroy();
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
      work_thread_.stop();
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::register_slots(const char* const data, const int64_t length) const
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      int32_t iret = NULL != data && length > 0
                    && ngi.owner_role_ ==  NS_ROLE_MASTER
                    && !is_destroy_ ? TFS_SUCCESS : EXIT_REGISTER_OPLOG_SYNC_ERROR;
      if (TFS_SUCCESS == iret)
      {
        file_queue_thread_->write(data, length);
      }
      return iret;
    }

    void OpLogSyncManager::notify_all()
    {
      tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
      monitor_.notifyAll();
    }

    void OpLogSyncManager::rotate()
    {
      tbutil::Mutex::Lock lock(mutex_);
      const QueueInformationHeader* head = file_queue_->get_queue_information_header();
      OpLogRotateHeader ophead;
      ophead.rotate_seqno_ = head->write_seqno_;
      ophead.rotate_offset_ = head->write_filesize_;
      const OpLogRotateHeader* tmp = oplog_->get_oplog_rotate_header();
      oplog_->update_oplog_rotate_header(ophead);
      TBSYS_LOG(
          INFO,
          "queue header: readseqno: %d, read_offset: %d, write_seqno: %d,\
                        write_file_size: %d, queuesize: %d. oplogheader:, rotate_seqno: %d\
                        rotate_offset: %d, last_rotate_seqno: %d, last_rotate_offset: %d",
          head->read_seqno_, head->read_offset_, head->write_seqno_, head->write_filesize_, head->queue_size_,
          ophead.rotate_seqno_, ophead.rotate_offset_, tmp->rotate_seqno_, tmp->rotate_offset_);
    }

    int OpLogSyncManager::flush_oplog(void) const
    {
      time_t now = time(NULL);
      tbutil::Mutex::Lock lock(mutex_);
      if (oplog_->finish(now, true))
      {
        register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
        TBSYS_LOG(DEBUG, "oplog size: %"PRI64_PREFIX"d", oplog_->get_slots_offset());
        oplog_->reset(now);
      }
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::log(uint8_t type, const char* const data, const int64_t length)
    {
      int iret = TFS_SUCCESS;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if ((ngi.owner_role_ == NS_ROLE_MASTER)
         && (ngi.owner_status_ == NS_STATUS_INITIALIZED)
         && (ngi.sync_oplog_flag_ == NS_SYNC_DATA_FLAG_YES))
      {
        #if !defined(TFS_GTEST) && !defined(TFS_NS_INTEGRATION)
        int count = 0;
        tbutil::Mutex::Lock lock(mutex_);
        do
        {
          ++count;
          iret = oplog_->write(type, data, length);
          if (iret != TFS_SUCCESS)
          {
            if (iret == EXIT_SLOTS_OFFSET_SIZE_ERROR)
            {
              register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
              TBSYS_LOG(DEBUG, "oplog size: %"PRI64_PREFIX"d", oplog_->get_slots_offset());
              oplog_->reset();
            }
          }
        }
        while (count < 0x03 && iret != TFS_SUCCESS);

        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write log file error, type: %d", type);
        }
        else
        {
          if (oplog_->finish(0))
          {
            register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
            TBSYS_LOG(DEBUG, "oplog size: %"PRI64_PREFIX"d", oplog_->get_slots_offset());
            oplog_->reset();
          }
        }
        #else
        UNUSED(data);
        UNUSED(type);
        UNUSED(length);
        #endif
      }
      return iret;
    }

    int OpLogSyncManager::push(common::BasePacket* msg, int32_t max_queue_size, bool block)
    {
      return  work_thread_.push(msg, max_queue_size, block);
    }

    int OpLogSyncManager::do_sync_oplog(const void* const data, const int64_t len, const int32_t, void *arg)
    {
      OpLogSyncManager* op_log_sync = static_cast<OpLogSyncManager*> (arg);
      return op_log_sync->do_sync_oplog(static_cast<const char* const > (data), len);
    }

    int OpLogSyncManager::do_sync_oplog(const char* const data, const int64_t length)
    {
      int32_t iret = TFS_SUCCESS;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if ((ngi.owner_role_ != NS_ROLE_MASTER)
          || (is_destroy_))
      {
        file_queue_->clear();
        TBSYS_LOG(INFO, "%s", " wait for sync oplog");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.wait();
      }
      else
      {
        if (ngi.other_side_status_ < NS_STATUS_INITIALIZED
            || ngi.sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES)
        {
          //wait
          TBSYS_LOG(INFO, "%s", " wait for sync oplog");
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.wait();
        }
        else
        {
          //to send data to the slave & wait
          OpLogSyncMessage msg;
          msg.set_data(data, length);
          tbnet::Packet* rmsg = NULL;
          int32_t count = 0;
          int32_t iret = TFS_ERROR;
          do
          {
            ++count;
            rmsg = NULL;
            NewClient* client = NewClientManager::get_instance().create_client();
            iret = send_msg_to_server(ngi.other_side_ip_port_, client, &msg, rmsg);
            if (TFS_SUCCESS == iret)
            {
              iret = rmsg->getPCode() == OPLOG_SYNC_RESPONSE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                OpLogSyncResponeMessage* tmsg = dynamic_cast<OpLogSyncResponeMessage*> (rmsg);
                iret = tmsg->get_complete_flag() == OPLOG_SYNC_MSG_COMPLETE_YES ? TFS_SUCCESS : TFS_ERROR;
                if (TFS_SUCCESS == iret)
                {
                  NewClientManager::get_instance().destroy_client(client);
                  break;
                }
              }
            }
            NewClientManager::get_instance().destroy_client(client);
          }
          while (count < 0x03);
          if (TFS_SUCCESS != iret)
          {
            ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
            TBSYS_LOG(WARN, "synchronization oplog: %s message failed, count: %d", data, count);
          }
        }
      }
      return iret;
    }

    bool OpLogSyncManager::handlePacketQueue(tbnet::Packet *packet, void * args)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        common::BasePacket* message = dynamic_cast<common::BasePacket*>(packet);
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        int32_t iret = TFS_SUCCESS;
        if (ngi.owner_role_ == NS_ROLE_MASTER) //master
          iret = do_master_msg(message, args);
        else if (ngi.owner_role_ == NS_ROLE_SLAVE) //slave
          iret = do_slave_msg(message, args);
        if (TFS_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "do %s message failed, iret: %d", ngi.owner_role_ == NS_ROLE_MASTER ? "master" : "slave", iret);
        }
      }
      return bret;
    }

    int OpLogSyncManager::do_sync_oplog(const common::BasePacket* message, const void*)
    {
      int32_t iret = TFS_SUCCESS;
      if (!is_destroy_)
      {
        const OpLogSyncMessage* msg = dynamic_cast<const OpLogSyncMessage*> (message);
        const char* data = msg->get_data();
        int64_t length = msg->get_length();
        int64_t offset = 0;
        time_t now = Func::get_monotonic_time();
        while ((offset < length)
            && (GFactory::get_runtime_info().destroy_flag_!= NS_DESTROY_FLAGS_YES))
        {
          iret = replay_helper(data, length, offset, now);
          if ((iret != TFS_SUCCESS)
              && (iret != EXIT_PLAY_LOG_ERROR))
          {
            break;
          }
        }
        OpLogSyncResponeMessage* rmsg = NULL;
        ARG_NEW(rmsg, OpLogSyncResponeMessage);
        rmsg->set_complete_flag();
        iret = const_cast<common::BasePacket*> (message)->reply(rmsg);
      }
      return iret;
    }

    int OpLogSyncManager::do_master_msg(const common::BasePacket* msg, const void*)
    {
      int32_t iret = TFS_SUCCESS;
      if (!is_destroy_)
      {
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (ngi.other_side_status_ < NS_STATUS_INITIALIZED
            || ngi.sync_oplog_flag_ < NS_SYNC_DATA_FLAG_YES)
        {
          //wait
          TBSYS_LOG(DEBUG, "%s", "wait for sync message");
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.wait();
        }
        int32_t count = 0;
        int32_t status = STATUS_MESSAGE_ERROR;
        do
        {
          ++count;
          iret = send_msg_to_server(ngi.other_side_ip_port_, const_cast<common::BasePacket*>(msg), status);
        }
        while (count < 0x03 && TFS_SUCCESS != iret && STATUS_MESSAGE_OK != status);
        iret = STATUS_MESSAGE_OK == status ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_ERROR != iret)
        {
          ngi.sync_oplog_flag_ = NS_SYNC_DATA_FLAG_NO;
          TBSYS_LOG(WARN, "synchronization operation message failed, count: %d", count);
        }
      }
      return iret;
    }

    int OpLogSyncManager::do_slave_msg(const common::BasePacket* msg, const void* args)
    {
      return do_sync_oplog(msg, args);
    }

    int OpLogSyncManager::replay_helper_do_msg(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos)
    {
      int32_t iret = NULL != data && data_len - pos > 0 &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        BasePacket* msg = NULL;
        if (type == OPLOG_TYPE_REPLICATE_MSG)
        {
          ReplicateBlockMessage* tmp = new ReplicateBlockMessage();
          iret = tmp->deserialize(data, data_len, pos);
          msg = tmp;
        }
        else
        {
          CompactBlockCompleteMessage* tmp = new CompactBlockCompleteMessage();
          iret = tmp->deserialize(data, data_len, pos);
          msg = tmp;
        }
        if (TFS_SUCCESS == iret
          && NULL != msg)
        {
          BaseService* base = dynamic_cast<BaseService*>(BaseService::instance());
          iret = base->push(msg) ? TFS_SUCCESS : TFS_ERROR;
        }
        if (TFS_SUCCESS != iret)
        {
          tbsys::gDelete(msg);
          TBSYS_LOG(ERROR, "deserialize error, data: %s, length: %"PRI64_PREFIX"d offset: %"PRI64_PREFIX"d", data, data_len, pos);
        }
      }
      return iret;
    }

    int OpLogSyncManager::replay_helper_do_oplog(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos, time_t now)
    {
      int32_t iret = NULL != data && data_len - pos > 0 &&  pos >= 0 ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        BlockOpLog oplog;
        iret = oplog.deserialize(data, data_len, pos);
        oplog.dump();
        if (TFS_SUCCESS != iret)
        {
          iret = EXIT_DESERIALIZE_ERROR;
          TBSYS_LOG(ERROR, "deserialize error, data: %s, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d", data, data_len, pos);
        }
        if (TFS_SUCCESS == iret)
        {
          if (oplog.servers_.empty()
              || (oplog.blocks_.empty()))
          {
            TBSYS_LOG(ERROR, "play log error, data: %s, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d", data, data_len, pos);
            iret = EXIT_PLAY_LOG_ERROR;
          }
        }
        if (TFS_SUCCESS ==  iret)
        {
          std::vector<uint32_t>::const_iterator iter = oplog.blocks_.begin();
          if (OPLOG_UPDATE == oplog.cmd_)
          {
            bool addnew = false;
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              iret = meta_mgr_.update_block_info(oplog.info_, oplog.servers_[0], now, addnew);
              if (TFS_SUCCESS != iret)
              {
                TBSYS_LOG(WARN, "update block information error, block: %u, server: %s",
                  oplog.info_.block_id_, CNetUtil::addrToString(oplog.servers_[0]).c_str());
              }
            }
          }
          else if (OPLOG_INSERT == oplog.cmd_)
          {
            std::vector<GCObject*> rms;
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              uint32_t block_id = (*iter);
              BlockChunkPtr ptr = meta_mgr_.get_chunk(block_id);
              BlockCollect* block = NULL;

              uint32_t tmp_block_id = id_factory_.generation(block_id);
              iret = BlockIdFactory::INVALID_BLOCK_ID != tmp_block_id ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS == iret)
              {
                RWLock::Lock lock(*ptr, WRITE_LOCKER);
                block = ptr->find(block_id);
                if (NULL == block)
                {
                  block = meta_mgr_.add_block(block_id, now);
                  if (NULL == block)
                  {
                    TBSYS_LOG(WARN, "add block: %u error", block_id);
                    iret = EXIT_PLAY_LOG_ERROR;
                  }
                }
                if (NULL != block)
                {
                  block->update(oplog.info_);
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "generation block id: %u failed, iret: %d", block_id, iret);
              }

              if (TFS_SUCCESS == iret)
              {
                ServerCollect* server = NULL;
                std::vector<uint64_t>::iterator s_iter = oplog.servers_.begin();
                for (; s_iter != oplog.servers_.end(); ++s_iter)
                {
                  server = meta_mgr_.get_server((*s_iter));
                  if (NULL == server)
                  {
                    TBSYS_LOG(WARN, "server object not found by : %s", CNetUtil::addrToString((*s_iter)).c_str());
                    continue;
                  }
                  TBSYS_LOG(DEBUG, "build replation between block: %u and server: %s",
                        block_id, CNetUtil::addrToString((*s_iter)).c_str());
                  RWLock::Lock lock(*ptr, WRITE_LOCKER);
                  block = ptr->find(block_id);
                  iret = NULL == block ? EXIT_NO_BLOCK : TFS_SUCCESS;
                  if (TFS_SUCCESS == iret)
                  {
                    if (meta_mgr_.build_relation(block, server, rms, now) != TFS_SUCCESS)
                    {
                      TBSYS_LOG(WARN, "build relation between block: %u and server: %s failed",
                          block_id, CNetUtil::addrToString((*s_iter)).c_str());
                    }
                  }
                }
              }
            }
            GFactory::get_gc_manager().add(rms);
          }
          else if (OPLOG_REMOVE== oplog.cmd_)
          {
            std::vector<GCObject*> rms;
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              uint32_t block_id = (*iter);
              BlockChunkPtr ptr = meta_mgr_.get_chunk(block_id);
              RWLock::Lock lock(*ptr, WRITE_LOCKER);
              ptr->remove(block_id, rms);
            }
            GFactory::get_gc_manager().add(rms);
          }
          else if (OPLOG_RELIEVE_RELATION == oplog.cmd_)
          {
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              uint32_t block_id = (*iter);
              BlockChunkPtr ptr = meta_mgr_.get_chunk(block_id);
              BlockCollect* block = NULL;
              ServerCollect* server = NULL;
              std::vector<uint64_t>::iterator s_iter = oplog.servers_.begin();
              for (; s_iter != oplog.servers_.end(); ++s_iter)
              {
                server = meta_mgr_.get_server((*s_iter));
                if (NULL == server)
                {
                  TBSYS_LOG(WARN, "server object not found by : %s", CNetUtil::addrToString((*s_iter)).c_str());
                  continue;
                }

                RWLock::Lock lock(*ptr, WRITE_LOCKER);
                block = ptr->find(block_id);
                if (NULL == block)
                {
                  TBSYS_LOG(WARN, "block object not found by : %u", block_id);
                  continue;
                }
                if (!meta_mgr_.relieve_relation(block, server, now))
                {
                  TBSYS_LOG(WARN, "relieve relation between block: %u and server: %s failed",
                    block_id, CNetUtil::addrToString((*s_iter)).c_str());
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "type: %d and  cmd: %d not found", type, oplog.cmd_);
            iret = EXIT_PLAY_LOG_ERROR;
          }
        }
      }
      return iret;
    }

    int OpLogSyncManager::replay_helper(const char* const data, const int64_t data_len, int64_t& pos, const time_t now)
    {
      OpLogHeader header;
      int32_t iret = (NULL != data && data_len - pos >= header.length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        iret = header.deserialize(data, data_len, pos);
        if (TFS_SUCCESS == iret)
        {
          uint32_t crc = 0;
          crc = Func::crc(crc, (data + pos), header.length_);
          if (crc != header.crc_)
          {
            TBSYS_LOG(ERROR, "check crc: %u<>: %u error", header.crc_, crc);
            iret = EXIT_CHECK_CRC_ERROR;
          }
          else
          {
            int8_t type = header.type_;
            TBSYS_LOG(DEBUG, "type: %d", type);
            switch (type)
            {
            case OPLOG_TYPE_REPLICATE_MSG:
            case OPLOG_TYPE_COMPACT_MSG:
              iret = replay_helper_do_msg(type, data, data_len, pos);
            break;
            case OPLOG_TYPE_BLOCK_OP:
              iret = replay_helper_do_oplog(type, data, data_len, pos, now);
            break;
            default:
              TBSYS_LOG(WARN, "type: %d not found", type);
              iret =  EXIT_PLAY_LOG_ERROR;
              break;
            }
          }
        }
      }
      return iret;
    }

    int OpLogSyncManager::replay_all()
    {
      bool has_log = false;
      file_queue_->set_delete_file_flag(true);
      int32_t iret = file_queue_->load_queue_head();
      if (iret != TFS_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "load header file of file_queue errors: %s", strerror(errno));
      }
      else
      {
        OpLogRotateHeader head = *oplog_->get_oplog_rotate_header();
        QueueInformationHeader* qhead = file_queue_->get_queue_information_header();
        QueueInformationHeader tmp = *qhead;
        TBSYS_LOG(DEBUG, "befor load queue header: read seqno: %d, read offset: %d, write seqno: %d,"
          "write file size: %d, queue size: %d. oplog header:, rotate seqno: %d"
          "rotate offset: %d", qhead->read_seqno_, qhead->read_offset_, qhead->write_seqno_, qhead->write_filesize_,
            qhead->queue_size_, head.rotate_seqno_, head.rotate_offset_);
        if (qhead->read_seqno_ > 0x01 && qhead->write_seqno_ > 0x01 && head.rotate_seqno_ > 0)
        {
          has_log = true;
          if (tmp.read_seqno_ <= head.rotate_seqno_)
          {
            tmp.read_seqno_ = head.rotate_seqno_;
            if (tmp.read_seqno_ == head.rotate_seqno_)
              tmp.read_offset_ = head.rotate_offset_;
            file_queue_->update_queue_information_header(&tmp);
          }
        }
        TBSYS_LOG(DEBUG, "after load queue header: read seqno: %d, read offset: %d, write seqno: %d,"
          "write file size: %d, queue size: %d. oplog header:, rotate seqno: %d"
          "rotate offset: %d", qhead->read_seqno_, qhead->read_offset_, qhead->write_seqno_, qhead->write_filesize_,
            qhead->queue_size_, head.rotate_seqno_, head.rotate_offset_);

        iret = file_queue_->initialize();
        if (iret != TFS_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "call FileQueue::finishSetup errors: %s", strerror(errno));
        }
        else
        {
          if (has_log)
          {
            time_t now =Func::get_monotonic_time();
            int64_t length = 0;
            int64_t offset = 0;
            int32_t iret = TFS_SUCCESS;
            do
            {
              QueueItem* item = file_queue_->pop();
              if (item != NULL)
              {
                const char* const data = item->data_;
                length = item->length_;
                offset = 0;
                OpLogHeader header;
                do
                {
                  iret = replay_helper(data, length, offset, now);
                  if ((iret != TFS_SUCCESS)
                      && (iret != EXIT_PLAY_LOG_ERROR))
                  {
                    break;
                  }
                }
                while ((length > header.length())
                        && (length > offset)
                        && (GFactory::get_runtime_info().destroy_flag_!= NS_DESTROY_FLAGS_YES));
                free(item);
                item = NULL;
              }
            }
            while (((qhead->read_seqno_ < qhead->write_seqno_)
                  || ((qhead->read_seqno_ == qhead->write_seqno_) && (qhead->read_offset_ != qhead->write_filesize_)))
                 && (GFactory::get_runtime_info().destroy_flag_ != NS_DESTROY_FLAGS_YES));
          }
        }
      }
      return iret;
    }
  }//end namespace nameserver
}//end namespace tfs
