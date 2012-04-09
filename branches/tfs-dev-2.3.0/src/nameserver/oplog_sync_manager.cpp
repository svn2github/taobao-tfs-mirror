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
#include "nameserver.h"

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
      if (!ngi.is_destroyed() && ngi.is_master() && ngi.peer_is_initialize_complete())
      {
        int ret = manager_.flush_oplog();
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(INFO, "flush oplog to filequeue failed, ret: %d", ret);
      }
    }

    OpLogSyncManager::OpLogSyncManager(LayoutManager& mm) :
      is_destroy_(false), manager_(mm), oplog_(NULL), file_queue_(NULL), file_queue_thread_(NULL)
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
      int ret = TFS_ERROR;

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
      ret = oplog_->initialize();
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "initialize oplog failed, path: %s, ret: %d", queue_header_path.c_str(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        std::string id_path = std::string(work_dir) + "/nameserver/";
        ret = id_factory_.initialize(id_path);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "initialize block id factory failed, path: %s, ret: %d", id_path.c_str(), ret);
        }
      }

      // replay all oplog
      if (TFS_SUCCESS == ret)
      {
        ret = replay_all();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "replay all oplogs failed, ret: %d", ret);
        }
      }

      std::string tmp(path);
      //reset filequeue
      if (TFS_SUCCESS == ret)
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
      if (TFS_SUCCESS == ret)
      {
        FlushOpLogTimerTaskPtr foltt = new FlushOpLogTimerTask(*this);
        manager_.get_name_server().get_timer()->scheduleRepeated(foltt, tbutil::Time::seconds(
              SYSPARAM_NAMESERVER.heart_interval_));
      }

      if (TFS_SUCCESS == ret)
      {
        file_queue_thread_->initialize(1, OpLogSyncManager::do_sync_oplog);
        const int queue_thread_num = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OPLOGSYNC_THREAD_NUM, 1);
        work_thread_.setThreadParameter(queue_thread_num , this, NULL);
        work_thread_.start();
      }
      return ret;
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
      return ((NULL != data) && (length > 0) && (ngi.is_master()) && (!ngi.is_destroyed()))
              ? file_queue_thread_->write(data, length) : EXIT_REGISTER_OPLOG_SYNC_ERROR;
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

    int OpLogSyncManager::flush_oplog(const time_t now) const
    {
      tbutil::Mutex::Lock lock(mutex_);
      if (oplog_->finish(now, true))
      {
        register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
        TBSYS_LOG(DEBUG, "oplog size: %"PRI64_PREFIX"d", oplog_->get_slots_offset());
        oplog_->reset(now);
      }
      return TFS_SUCCESS;
    }


    void OpLogSyncManager::reset_()
    {
      register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
      oplog_->reset();
    }

    int OpLogSyncManager::log(const uint8_t type, const char* const data, const int64_t length, const time_t now)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      int32_t ret = TFS_SUCCESS;
      if (ngi.is_master()
          && !ngi.is_destroyed()
          && ngi.has_valid_lease(now)
          && ngi.own_is_initialize_complete())
      {
        tbutil::Mutex::Lock lock(mutex_);
        for (int32_t i = 0; i < 3 && TFS_SUCCESS != ret; i++)
        {
          ret = oplog_->write(type, data, length);
          if (EXIT_SLOTS_OFFSET_SIZE_ERROR == ret)
          {
            reset_();
          }
        }
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(INFO, "write log file error, type: %d", type);
        }
        else
        {
          if (oplog_->finish(0))
          {
            reset_();
          }
        }
      }
      return ret;
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
      time_t now = Func::get_monotonic_time();
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      if ((!ngi.is_master())
          || (ngi.is_destroyed())
          || (ngi.is_master() &&
            (!ngi.peer_is_initialize_complete() || !ngi.has_valid_lease(now))))
      {
        file_queue_->clear();
        TBSYS_LOG(INFO, "%s", " wait for sync oplog");
        tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
        monitor_.wait();
      }
      //to send data to the slave & wait
      OpLogSyncMessage msg;
      msg.set_data(data, length);
      tbnet::Packet* rmsg = NULL;
      int32_t ret = TFS_ERROR;
      for (int32_t i = 0; i < 3 && TFS_SUCCESS != ret && ngi.has_valid_lease(now); ++i, rmsg = NULL)
      {
        NewClient* client = NewClientManager::get_instance().create_client();
        ret = send_msg_to_server(ngi.peer_ip_port_, client, &msg, rmsg);
        if (TFS_SUCCESS == ret)
        {
          ret = rmsg->getPCode() == OPLOG_SYNC_RESPONSE_MESSAGE ? TFS_SUCCESS : TFS_ERROR;
          if (TFS_SUCCESS == ret)
          {
            OpLogSyncResponeMessage* tmsg = dynamic_cast<OpLogSyncResponeMessage*> (rmsg);
            ret = tmsg->get_complete_flag() == OPLOG_SYNC_MSG_COMPLETE_YES ? TFS_SUCCESS : TFS_ERROR;
          }
        }
        NewClientManager::get_instance().destroy_client(client);
      }
      return ret;
    }

    bool OpLogSyncManager::handlePacketQueue(tbnet::Packet *packet, void * args)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        common::BasePacket* message = dynamic_cast<common::BasePacket*>(packet);
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        int32_t ret = ngi.is_master() ? do_master_msg(message, args) : do_slave_msg(message, args);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "do %s message failed, ret: %d", ngi.owner_role_ == NS_ROLE_MASTER ? "master" : "slave", ret);
        }
      }
      return bret;
    }

    int OpLogSyncManager::do_sync_oplog(const common::BasePacket* message, const void*)
    {
      int32_t ret = TFS_SUCCESS;
      if (!is_destroy_)
      {
        const OpLogSyncMessage* msg = dynamic_cast<const OpLogSyncMessage*> (message);
        const char* data = msg->get_data();
        int64_t length = msg->get_length();
        int64_t offset = 0;
        time_t now = Func::get_monotonic_time();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        while ((offset < length)
            && (!ngi.is_destroyed()))
        {
          ret = replay_helper(data, length, offset, now);
          if ((ret != TFS_SUCCESS)
              && (ret != EXIT_PLAY_LOG_ERROR))
          {
            break;
          }
        }
        OpLogSyncResponeMessage* rmsg = NULL;
        ARG_NEW(rmsg, OpLogSyncResponeMessage);
        rmsg->set_complete_flag();
        ret = const_cast<common::BasePacket*> (message)->reply(rmsg);
      }
      return ret;
    }

    int OpLogSyncManager::do_master_msg(const common::BasePacket* msg, const void*)
    {
      int32_t ret = TFS_ERROR;
      if (!is_destroy_)
      {
        time_t now = Func::get_monotonic_time();
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (!ngi.peer_is_initialize_complete()
            || !ngi.has_valid_lease(now))
        {
          //wait
          tbutil::Monitor<tbutil::Mutex>::Lock lock(monitor_);
          monitor_.wait();
        }
        int32_t i = 0;
        int32_t status = STATUS_MESSAGE_ERROR;
        for (i = 0; i < 3 && TFS_SUCCESS != ret && STATUS_MESSAGE_OK != status; i++)
        {
          ret = send_msg_to_server(ngi.peer_ip_port_, const_cast<common::BasePacket*>(msg), status);
        }
        ret = STATUS_MESSAGE_OK == status ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_ERROR != ret)
        {
          TBSYS_LOG(INFO, "synchronization operation message failed, count: %d", i);
        }
      }
      return ret;
    }

    int OpLogSyncManager::do_slave_msg(const common::BasePacket* msg, const void* args)
    {
      return do_sync_oplog(msg, args);
    }

    int OpLogSyncManager::replay_helper_do_msg(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = (NULL != data) && (data_len - pos > 0) &&  (pos >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BasePacket* msg = NULL;
        if (type == OPLOG_TYPE_REPLICATE_MSG)
        {
          ReplicateBlockMessage* tmp = new (std::nothrow)ReplicateBlockMessage();
          assert(NULL != tmp);
          ret = tmp->deserialize(data, data_len, pos);
          msg = tmp;
        }
        else
        {
          CompactBlockCompleteMessage* tmp = new (std::nothrow)CompactBlockCompleteMessage();
          ret = tmp->deserialize(data, data_len, pos);
          assert(NULL != tmp);
          msg = tmp;
        }
        if (TFS_SUCCESS == ret
            && NULL != msg)
        {
          BaseService* base = dynamic_cast<BaseService*>(BaseService::instance());
          ret = base->push(msg) ? TFS_SUCCESS : TFS_ERROR;
        }
        if (TFS_SUCCESS != ret)
        {
          tbsys::gDelete(msg);
          TBSYS_LOG(INFO, "deserialize error, data: %s, length: %"PRI64_PREFIX"d offset: %"PRI64_PREFIX"d", data, data_len, pos);
        }
      }
      return ret;
    }

    int OpLogSyncManager::replay_helper_do_oplog(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos, time_t now)
    {
      int32_t ret = (NULL != data) && (data_len - pos > 0) &&  (pos >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockOpLog oplog;
        ret = oplog.deserialize(data, data_len, pos);
        if (TFS_SUCCESS != ret)
        {
          oplog.dump();
          ret = EXIT_DESERIALIZE_ERROR;
          TBSYS_LOG(INFO, "deserialize error, data: %s, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d", data, data_len, pos);
        }
        if (TFS_SUCCESS == ret)
        {
          if (oplog.servers_.empty() || (oplog.blocks_.empty()))
          {
            TBSYS_LOG(INFO, "play log error, data: %s, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d", data, data_len, pos);
            ret = EXIT_PLAY_LOG_ERROR;
          }
        }
        if (TFS_SUCCESS == ret)
        {
          std::vector<uint32_t>::const_iterator iter = oplog.blocks_.begin();
          if (OPLOG_UPDATE == oplog.cmd_)
          {
            bool addnew = false;
            ret = manager_.update_block_info(oplog.info_, oplog.servers_[0], now, addnew);
            if (TFS_SUCCESS != ret)
            {
              TBSYS_LOG(INFO, "update block information error, block: %u, server: %s",
                  oplog.info_.block_id_, CNetUtil::addrToString(oplog.servers_[0]).c_str());
            }
          }
          else if (OPLOG_INSERT == oplog.cmd_)
          {
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              uint32_t block_id = (*iter);
              BlockCollect* block = NULL;
              uint32_t tmp_block_id = id_factory_.generation(block_id);
              ret = INVALID_BLOCK_ID != tmp_block_id ? TFS_SUCCESS : TFS_ERROR;
              if (TFS_SUCCESS != ret)
              {
                TBSYS_LOG(INFO, "generation block id: %u failed, ret: %d", block_id, ret);
              }
              else
              {
                block = manager_.get_block_manager().get(block_id);
                if (NULL == block)
                {
                  block = manager_.get_block_manager().insert(block_id, now);
                  ret = NULL == block ? EXIT_PLAY_LOG_ERROR : TFS_SUCCESS;
                }
                if (TFS_SUCCESS == ret)
                {
                  block->update(oplog.info_);
                }
              }

              if (TFS_SUCCESS == ret)
              {
                ServerCollect* server = NULL;
                std::vector<uint64_t>::iterator s_iter = oplog.servers_.begin();
                for (; s_iter != oplog.servers_.end(); ++s_iter)
                {
                  server = manager_.get_server_manager().get((*s_iter));
                  manager_.build_relation(block, server, now);
                }
              }
            }
          }
          else if (OPLOG_REMOVE== oplog.cmd_)
          {
            GCObject* pgcobject = NULL;
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              manager_.get_block_manager().remove(pgcobject, (*iter));
              manager_.get_gc_manager().add(pgcobject);
            }
          }
          else if (OPLOG_RELIEVE_RELATION == oplog.cmd_)
          {
            for (; iter != oplog.blocks_.end(); ++iter)
            {
              std::vector<uint64_t>::iterator s_iter = oplog.servers_.begin();
              for (; s_iter != oplog.servers_.end(); ++s_iter)
              {
                ServerCollect* server = manager_.get_server_manager().get((*s_iter));
                BlockCollect*  block  = manager_.get_block_manager().get((*iter));
                if (!manager_.relieve_relation(block, server, now))
                {
                  TBSYS_LOG(INFO, "relieve relation between block: %u and server: %s failed",
                      (*iter), CNetUtil::addrToString((*s_iter)).c_str());
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "type: %d and  cmd: %d not found", type, oplog.cmd_);
            ret = EXIT_PLAY_LOG_ERROR;
          }
        }
      }
      return ret;
    }

    int OpLogSyncManager::replay_helper(const char* const data, const int64_t data_len, int64_t& pos, const time_t now)
    {
      OpLogHeader header;
      int32_t ret = (NULL != data && data_len - pos >= header.length()) ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = header.deserialize(data, data_len, pos);
        if (TFS_SUCCESS == ret)
        {
          uint32_t crc = 0;
          crc = Func::crc(crc, (data + pos), header.length_);
          if (crc != header.crc_)
          {
            TBSYS_LOG(ERROR, "check crc: %u<>: %u error", header.crc_, crc);
            ret = EXIT_CHECK_CRC_ERROR;
          }
          else
          {
            int8_t type = header.type_;
            TBSYS_LOG(DEBUG, "type: %d", type);
            switch (type)
            {
              case OPLOG_TYPE_REPLICATE_MSG:
              case OPLOG_TYPE_COMPACT_MSG:
                ret = replay_helper_do_msg(type, data, data_len, pos);
                break;
              case OPLOG_TYPE_BLOCK_OP:
                ret = replay_helper_do_oplog(type, data, data_len, pos, now);
                break;
              default:
                TBSYS_LOG(WARN, "type: %d not found", type);
                ret =  EXIT_PLAY_LOG_ERROR;
                break;
            }
          }
        }
      }
      return ret;
    }

    int OpLogSyncManager::replay_all()
    {
      bool has_log = false;
      file_queue_->set_delete_file_flag(true);
      int32_t ret = file_queue_->load_queue_head();
      if (ret != TFS_SUCCESS)
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

        ret = file_queue_->initialize();
        if (ret != TFS_SUCCESS)
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
            int32_t ret = TFS_SUCCESS;
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
                  ret = replay_helper(data, length, offset, now);
                  if ((ret != TFS_SUCCESS)
                      && (ret != EXIT_PLAY_LOG_ERROR))
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
      return ret;
    }
  }//end namespace nameserver
}//end namespace tfs
