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
    OpLogSyncManager::OpLogSyncManager(LayoutManager& mm) :
      manager_(mm), oplog_(NULL), file_queue_(NULL), file_queue_thread_(NULL)
    {
      for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM; ++index)
      {
        dbhelper_[index] = NULL;
        load_family_info_thread_[index] = 0;
      }
    }

    OpLogSyncManager::~OpLogSyncManager()
    {
      tbsys::gDelete( file_queue_);
      tbsys::gDelete( file_queue_thread_);
      tbsys::gDelete( oplog_);
      for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM; ++index)
      {
        tbsys::gDelete(dbhelper_[index]);
        load_family_info_thread_[index] = 0;
      }
    }

    int OpLogSyncManager::initialize()
    {
      //initializeation filequeue, filequeuethread
      char path[256];
      BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
      const char* work_dir = service->get_work_dir();
      snprintf(path, 256, "%s/nameserver/filequeue", work_dir);
      std::string file_path(path);
      std::string file_queue_name = "oplogsync";
      ARG_NEW(file_queue_, FileQueue, file_path, file_queue_name, FILE_QUEUE_MAX_FILE_SIZE);
      file_queue_->set_delete_file_flag(true);
      ARG_NEW(file_queue_thread_, FileQueueThread, file_queue_, this);

      //initializeation oplog
      int32_t max_slots_size = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OPLOG_SYSNC_MAX_SLOTS_NUM);
      max_slots_size = max_slots_size <= 0 ? 1024 : max_slots_size > 4096 ? 4096 : max_slots_size;
      std::string queue_header_path = std::string(path) + "/" + file_queue_name;
      ARG_NEW(oplog_, OpLog, queue_header_path, max_slots_size);
      int32_t ret = oplog_->initialize();
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
        ret = replay_all_();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "replay all oplogs failed, ret: %d", ret);
        }
      }

      std::string tmp(path);
      //reset filequeue
      if (TFS_SUCCESS == ret)
      {
        file_queue_->clear();
        const QueueInformationHeader* qhead = file_queue_->get_queue_information_header();
        OpLogRotateHeader rotmp;
        rotmp.rotate_seqno_ = qhead->write_seqno_;
        rotmp.rotate_offset_ = qhead->write_filesize_;

        //update rotate header information
        oplog_->update_oplog_rotate_header(rotmp);
      }

      if (TFS_SUCCESS == ret)
      {
        file_queue_thread_->initialize(1, OpLogSyncManager::sync_log_func);
        const int queue_thread_num = TBSYS_CONFIG.getInt(CONF_SN_NAMESERVER, CONF_OPLOGSYNC_THREAD_NUM, 1);
        work_thread_.setThreadParameter(queue_thread_num , this, NULL);
        // work_thread_.start();
      }
      if (TFS_SUCCESS == ret)
      {
        std::string tair_info = TBSYS_CONFIG.getString(CONF_SN_NAMESERVER, CONF_TAIR_ADDR, "");
        std::vector<std::string> items;
        common::Func::split_string(tair_info.c_str(), ',', items);
        ret = items.size() < 5 ? EXIT_SYSTEM_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "tair info: %s is invalid", tair_info.c_str());
        }
        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM && TFS_SUCCESS == ret; ++index)
          {
            dbhelper_[index] = new TairHelper(items[0], items[1], items[2], items[3], atoi(items[4].c_str()));
            ret = dbhelper_[index]->initialize();
          }
        }
        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM; ++index)
          {
            load_family_info_thread_[index] = new LoadFamilyInfoThreadHelper(*this, index);
          }
        }
      }
      return ret;
    }

    int OpLogSyncManager::wait_for_shut_down()
    {
      if (file_queue_thread_ != NULL)
      {
        file_queue_thread_->wait();
      }
      // work_thread_.wait();

      for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM; ++index)
      {
        if (load_family_info_thread_[index] != 0)
          load_family_info_thread_[index]->join();
      }
      return id_factory_.destroy();
    }

    int OpLogSyncManager::destroy()
    {
      if (file_queue_thread_ != NULL)
      {
        file_queue_thread_->destroy();
      }
      // work_thread_.stop();
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::register_slots(const char* const data, const int64_t length) const
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      return ((NULL != data) && (length > 0) && (ngi.is_master()) && (!ngi.is_destroyed()))
              ? file_queue_thread_->write(data, length) : EXIT_REGISTER_OPLOG_SYNC_ERROR;
    }

    void OpLogSyncManager::switch_role()
    {
      tbutil::Mutex::Lock lock(mutex_);
      if (NULL != file_queue_)
        file_queue_->clear();
      id_factory_.skip();
      for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM; ++index)
      {
        load_family_info_thread_[index]->set_reload();
      }
    }

    int OpLogSyncManager::log(const uint8_t type, const char* const data, const int64_t length, const time_t now)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      int32_t ret = ((NULL != data) && (length > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        //ngi.dump(TBSYS_LOG_LEVEL(DEBUG), "write oplog");
        if (ngi.is_master()
            && !ngi.is_destroyed()
            && ngi.has_valid_lease(now))
        {
          ret = TFS_ERROR;
          tbutil::Mutex::Lock lock(mutex_);
          for (int32_t i = 0; i < 3 && TFS_SUCCESS != ret; i++)
          {
            ret = oplog_->write(type, data, length);
            if (EXIT_SLOTS_OFFSET_SIZE_ERROR == ret)
            {
              register_slots(oplog_->get_buffer(), oplog_->get_slots_offset());
              oplog_->reset();
            }
          }
        }
      }
      return ret;
    }

    int OpLogSyncManager::push(common::BasePacket* msg, int32_t max_queue_size, bool block)
    {
      return  work_thread_.push(msg, max_queue_size, block);
    }

    int OpLogSyncManager::sync_log_func(const void* const data, const int64_t len, const int32_t, void *args)
    {
      int32_t ret = ((NULL != data) && (NULL != args) && (len > 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        OpLogSyncManager* sync = static_cast<OpLogSyncManager*> (args);
        ret = sync->send_log_(static_cast<const char* const>(data), len);
      }
      return ret;
    }

    int OpLogSyncManager::send_log_(const char* const data, const int64_t length)
    {
      int32_t ret = ((NULL != data) && (length >  0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MAX_SLEEP_TIME_US = 500;
        NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
        if (!ngi.is_master())
        {
          file_queue_->clear();
          while (!ngi.is_master())
            usleep(MAX_SLEEP_TIME_US);
        }
        else
        {
          time_t now = Func::get_monotonic_time();
          while (!ngi.has_valid_lease(now))
          {
            usleep(MAX_SLEEP_TIME_US);
            now = Func::get_monotonic_time();
          }

          if (ngi.is_master())
          {
            ret = TFS_ERROR;
            //to send data to the slave & wait
            tbnet::Packet* rmsg = NULL;
           for (int32_t i = 0; i < 3 && TFS_SUCCESS != ret && ngi.has_valid_lease(now); ++i, rmsg = NULL)
            {
              create_msg_ref(OpLogSyncMessage, request_msg);
              request_msg.set_data(data, length);
              NewClient* client = NewClientManager::get_instance().create_client();
              ret = send_msg_to_server(ngi.sync_log_peer_ip_port_, client, &request_msg, rmsg);
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
          }
        }
      }
      return ret;
    }

    bool OpLogSyncManager::handlePacketQueue(tbnet::Packet *packet, void* args)
    {
      bool bret = packet != NULL;
      if (bret)
      {
        UNUSED(args);
        common::BasePacket* message = dynamic_cast<common::BasePacket*>(packet);
        int32_t ret = GFactory::get_runtime_info().is_master() ? transfer_log_msg_(message) : recv_log_(message);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "%s log message failed, ret: %d",
            GFactory::get_runtime_info().is_master() ? "transfer" : "recv", ret);
        }
      }
      return bret;
    }

    int OpLogSyncManager::handle(common::BasePacket* packet)
    {
      assert(NULL != packet && OPLOG_SYNC_MESSAGE == packet->getPCode());
      int32_t ret = GFactory::get_runtime_info().is_master() ? transfer_log_msg_(packet) : recv_log_(packet);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "%s log message failed, ret: %d",
            GFactory::get_runtime_info().is_master() ? "transfer" : "recv", ret);
      }
      return ret;
    }

    int OpLogSyncManager::recv_log_(common::BasePacket* message)
    {
      int32_t ret = (NULL != message && message->getPCode() == OPLOG_SYNC_MESSAGE ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        OpLogSyncMessage* msg = dynamic_cast<OpLogSyncMessage*> (message);
        const char* data = msg->get_data();
        int64_t length = msg->get_length();
        int64_t offset = 0;
        time_t now = Func::get_monotonic_time();
        int32_t count = 0;
        TIMER_START();
        while ((offset < length)
            && (!GFactory::get_runtime_info().is_destroyed()))
        {
          ret = replay_helper(data, length, offset, now);
          if ((ret != TFS_SUCCESS)
              && (ret != EXIT_PLAY_LOG_ERROR))
          {
            break;
          }
          count++;
        }
        TIMER_END();
        TBSYS_LOG(DEBUG, "replay log, count=%d, length=%"PRI64_PREFIX"d, cost=%"PRI64_PREFIX"d, ret=%d",
            count, length, TIMER_DURATION(), ret);
        OpLogSyncResponeMessage* reply_msg = new (std::nothrow)OpLogSyncResponeMessage();
        reply_msg->set_complete_flag();
        msg->reply(reply_msg);
      }
      return ret;
    }

    int OpLogSyncManager::transfer_log_msg_(common::BasePacket* msg)
    {
      int32_t ret = (NULL != msg && msg->getPCode() == OPLOG_SYNC_MESSAGE ) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        const int32_t MAX_SLEEP_TIME_US = 500;
        time_t now = Func::get_monotonic_time();
        while (!GFactory::get_runtime_info().has_valid_lease(now)
              && GFactory::get_runtime_info().is_master())
        {
          usleep(MAX_SLEEP_TIME_US);
          now = Func::get_monotonic_time();
        }
        int32_t status = STATUS_MESSAGE_ERROR;
        for (int32_t i = 0; i < 3 && TFS_SUCCESS != ret && STATUS_MESSAGE_OK != status; i++)
        {
          ret = send_msg_to_server(GFactory::get_runtime_info().sync_log_peer_ip_port_, msg, status, DEFAULT_NETWORK_CALL_TIMEOUT, true);
        }
        ret = STATUS_MESSAGE_OK == status ? TFS_SUCCESS : TFS_ERROR;
        if (TFS_ERROR != ret)
        {
          TBSYS_LOG(INFO, "transfer message: %d failed", msg->getPCode());
        }
      }
      return ret;
    }

    int OpLogSyncManager::replay_helper_do_msg(const int32_t type, const char* const data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = (NULL != data) && (data_len - pos > 0) &&  (pos >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BasePacket* msg = this->malloc_(type);
        if (NULL != msg)
        {
          ret = msg->deserialize(data, data_len, pos);
        }

        if (TFS_SUCCESS == ret
            && NULL != msg)
        {
          msg->dump();
          NameServer* service = dynamic_cast<NameServer*>(BaseMain::instance());
          ret = service->handle(msg);
        }
        tbsys::gDelete(msg);
      }
      return ret;
    }

    int OpLogSyncManager::replay_helper_do_oplog(const time_t now, const int32_t type, const char* const data, const int64_t data_len, int64_t& pos)
    {
      int32_t ret = ((NULL != data) && (data_len - pos > 0) &&  (pos >= 0)) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        BlockOpLog oplog;
        memset(&oplog, 0, sizeof(oplog));
        ret = oplog.deserialize(data, data_len, pos);
        ret = (TFS_SUCCESS == ret) ? TFS_SUCCESS : EXIT_DESERIALIZE_ERROR;
        if (TFS_SUCCESS != ret)
        {
          Func::hex_dump(data, data_len, true, TBSYS_LOG_LEVEL_DEBUG);
          oplog.dump(TBSYS_LOG_LEVEL_INFO);
          TBSYS_LOG(INFO, "deserialize error, data: %p, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d", data, data_len, pos);
        }
        if (TFS_SUCCESS == ret)
        {
          ret = oplog.server_num_ > 0 ? TFS_SUCCESS : EXIT_PLAY_LOG_ERROR;
          if (TFS_SUCCESS != ret)
          {
            Func::hex_dump(data, data_len, true, TBSYS_LOG_LEVEL_DEBUG);
            oplog.dump(TBSYS_LOG_LEVEL_INFO);
            TBSYS_LOG(INFO, "play log error, data: %p, length: %"PRI64_PREFIX"d, offset: %"PRI64_PREFIX"d, %d", data, data_len, pos, oplog.cmd_);
          }
        }
        if (TFS_SUCCESS == ret)
        {
          if (OPLOG_UPDATE == oplog.cmd_)
          {
            BlockCollect* block = manager_.get_block_manager().get(oplog.info_.block_id_);
            ret = NULL == block ? EXIT_PLAY_LOG_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
              manager_.get_block_manager().update_block_info(oplog.info_, block);
            else
              TBSYS_LOG(INFO, "update block information error, block: %"PRI64_PREFIX"u, server: %s",
                  oplog.info_.block_id_, CNetUtil::addrToString(oplog.servers_[0]).c_str());
          }
          else if (OPLOG_INSERT == oplog.cmd_)
          {
            int32_t result  = id_factory_.update(oplog.info_.block_id_);
            if (TFS_SUCCESS != result)
              TBSYS_LOG(INFO, "update block id: %"PRI64_PREFIX"u failed, result: %d", oplog.info_.block_id_, result);

            BlockCollect* block = manager_.get_block_manager().get(oplog.info_.block_id_);
            if (NULL == block)
            {
              block = manager_.get_block_manager().insert(oplog.info_.block_id_, now, false);
              ret = NULL == block ? EXIT_PLAY_LOG_ERROR : TFS_SUCCESS;
            }
            if (TFS_SUCCESS == ret)
            {
              manager_.get_block_manager().update_block_info(oplog.info_, block);
              for (int8_t index = 0; index < oplog.server_num_; ++index)
              {
                ServerCollect* server = manager_.get_server_manager().get(oplog.servers_[index]);
                manager_.build_relation(block, server, &oplog.info_, now, false);
              }
            }
          }
          else if (OPLOG_REMOVE== oplog.cmd_)
          {
            BlockCollect* block = NULL;
            manager_.get_block_manager().remove(block, oplog.info_.block_id_);
            manager_.get_gc_manager().insert(block, Func::get_monotonic_time());
          }
          else if (OPLOG_RELIEVE_RELATION == oplog.cmd_)
          {
            for (int8_t index = 0; index < oplog.server_num_; ++index)
            {
              int32_t result = manager_.relieve_relation(oplog.info_.block_id_, oplog.servers_[index], now, true);
              if (TFS_SUCCESS != result)
                TBSYS_LOG(INFO, "relieve relation between block: %"PRI64_PREFIX"u and server: %s failed",
                    oplog.info_.block_id_, CNetUtil::addrToString(oplog.servers_[index]).c_str());
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
      int32_t ret = ((NULL != data) && (data_len - pos >= header.length())) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = header.deserialize(data, data_len, pos);
        if (TFS_SUCCESS == ret)
        {
          uint32_t crc = 0;
          crc = Func::crc(crc, (data + pos), header.length_);
          if (crc != header.crc_)
          {
            TBSYS_LOG(ERROR, "check crc: %u<>: %u error, type: %d, length: %d", header.crc_, crc, header.type_, header.length_);
            ret = EXIT_CHECK_CRC_ERROR;
          }
          else
          {
            int8_t type = header.type_;
            switch (type)
            {
              case OPLOG_TYPE_REPLICATE_MSG:
              case OPLOG_TYPE_COMPACT_MSG:
                ret = replay_helper_do_msg(type, data, data_len, pos);
                break;
              case OPLOG_TYPE_BLOCK_OP:
                ret = replay_helper_do_oplog(now, type, data, data_len, pos);
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

    int OpLogSyncManager::replay_all_()
    {
      bool has_log = false;
      int32_t ret = file_queue_->load_queue_head();
      if (ret != TFS_SUCCESS)
      {
        TBSYS_LOG(WARN, "load header file of file_queue errors: %s", strerror(errno));
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
          TBSYS_LOG(WARN, "file queue initialize errors: ret :%d, %s", ret, strerror(errno));
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

    int OpLogSyncManager::create_family_id(int64_t& family_id)
    {
      int32_t ret = NULL != dbhelper_[DEFATUL_TAIR_INDEX] ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = dbhelper_[DEFATUL_TAIR_INDEX]->create_family_id(family_id);
      }
      return ret;
    }

    int OpLogSyncManager::create_family(common::FamilyInfo& family_info)
    {
      int32_t ret = NULL != dbhelper_[DEFATUL_TAIR_INDEX] ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = dbhelper_[DEFATUL_TAIR_INDEX]->create_family(family_info);
      }
      return ret;
    }

    int OpLogSyncManager::del_family(const int64_t family_id)
    {
      int32_t ret = NULL != dbhelper_[DEFATUL_TAIR_INDEX] ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = dbhelper_[DEFATUL_TAIR_INDEX]->del_family(family_id, false, true, GFactory::get_runtime_info().owner_ip_port_);
      }
      return ret;
    }

    common::BasePacket* OpLogSyncManager::malloc_(const int32_t type)
    {
      BasePacket* result = NULL;
      int32_t ret = (type >= OPLOG_TYPE_BLOCK_OP && type <= OPLOG_TYPE_DISSOLVE_MSG) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (OPLOG_TYPE_REPLICATE_MSG == type)
          result = new (std::nothrow)ReplicateBlockMessage();
        else if (OPLOG_TYPE_COMPACT_MSG == type)
          result = new (std::nothrow)DsCommitCompactBlockCompleteToNsMessage();
        else if (OPLOG_TYPE_MARSHALLING_MSG == type)
          result = new (std::nothrow)ECMarshallingCommitMessage(true);
        else if (OPLOG_TYPE_REINSTATE_MSG == type)
          result = new (std::nothrow)ECReinstateCommitMessage(true);
        else if (OPLOG_TYPE_DISSOLVE_MSG == type)
          result = new (std::nothrow)ECDissolveCommitMessage(true);
        assert(NULL != result);
      }
      return result;
    }

    int OpLogSyncManager::load_family_info_(const int32_t thseqno)
    {
      int32_t ret = TAIR_RETURN_SUCCESS, retry = 0;
      int64_t start_family_id = INVALID_FAMILY_ID;
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      FamilyManager& family_manager = manager_.get_family_manager();
      for (int32_t chunk = thseqno; chunk < MAX_FAMILY_CHUNK_NUM && !ngi.is_destroyed() && TFS_SUCCESS == ret; chunk +=MAX_LOAD_FAMILY_INFO_THREAD_NUM)
      {
        retry = 0;
        start_family_id = family_manager.get_max_family_id(chunk);
        do
        {
          ret = scan_all_family_(thseqno, chunk, start_family_id);
        }
        while (TAIR_RETURN_DATA_NOT_EXIST != ret && TAIR_RETURN_SUCCESS != ret && retry++ < 3);
        ret = (TAIR_RETURN_DATA_NOT_EXIST == ret || TAIR_RETURN_SUCCESS) ? TFS_SUCCESS : ret;
      }
      return ret;
    }

    int OpLogSyncManager::load_family_log_(const int32_t thseqno)
    {
      int32_t ret = TAIR_RETURN_SUCCESS, retry = 0;
      if (0 == thseqno)
      {
        retry = 0;
        do
        {
          ret = scan_all_family_log_();
        }
        while (TAIR_RETURN_DATA_NOT_EXIST != ret && TAIR_RETURN_SUCCESS != ret && retry++ < 3);
      }
      ret = (TAIR_RETURN_DATA_NOT_EXIST == ret || TAIR_RETURN_SUCCESS) ? TFS_SUCCESS : ret;
      return ret;
    }

    int OpLogSyncManager::load_all_family_info_(const int32_t thseqno, bool& load_complete)
    {
      NsRuntimeGlobalInformation& ngi = GFactory::get_runtime_info();
      while (!ngi.is_destroyed())
      {
        if (!ngi.load_family_info_complete())
        {
          load_family_info_(thseqno);
          load_family_log_(thseqno);
          load_complete = true;
          TBSYS_LOG(INFO, "thread %d load family complete on startup or switch", thseqno);

          while (!ngi.is_destroyed() && !ngi.load_family_info_complete())
          {
            bool  complete = true;
            for (int32_t index = 0; index < MAX_LOAD_FAMILY_INFO_THREAD_NUM && complete; ++index)
            {
              complete = load_family_info_thread_[index]->load_complete();
            }
            ngi.set_load_family_info_complete(complete);
            if (complete)
            {
              TBSYS_LOG(INFO, "all threads load complete");
              break;
            }
            usleep(1000000);
          }
        }
        else
        {
          if (!ngi.is_master())
          {
            load_family_info_(thseqno);
            load_family_log_(thseqno);
            TBSYS_LOG(INFO, "thread %d load family complete on slave", thseqno);
          }
        }
        usleep(1000000);
      }
      return TFS_SUCCESS;
    }

    int OpLogSyncManager::scan_all_family_(const int32_t thseqno, const int32_t chunk, int64_t& start_family_id)
    {
      uint64_t id = INVALID_BLOCK_ID;
      std::vector<common::FamilyInfo> infos;
      int32_t ret = TAIR_RETURN_SUCCESS, rt = TFS_SUCCESS;
      BlockManager& block_manager = manager_.get_block_manager();
      FamilyManager& family_manager = manager_.get_family_manager();
      std::pair<uint64_t, int32_t> members[MAX_MARSHALLING_NUM];
      common::ArrayHelper<std::pair<uint64_t, int32_t> > helper(MAX_MARSHALLING_NUM, members);
      const uint32_t limit = RANGE_DEFAULT_LIMIT; // tair default limit 1000
      int32_t times = 0;
      int32_t size = 0;
      TIMER_START();
      do
      {
        infos.clear();
        assert(NULL != dbhelper_[thseqno]);
        ret = dbhelper_[thseqno]->scan(infos, start_family_id, chunk, false,  GFactory::get_runtime_info().peer_ip_port_, limit);
        std::vector<common::FamilyInfo>::const_iterator iter = infos.begin();
        int64_t now = Func::get_monotonic_time();
        for (; iter != infos.end(); ++iter)
        {
          helper.clear();
          const FamilyInfo& family = (*iter);
          if (start_family_id < family.family_id_)
            start_family_id = family.family_id_;
          std::stringstream str;
          str << family.family_id_ << ":" << family.family_aid_info_ << ":";
          std::vector<std::pair<uint64_t, int32_t> >::const_iterator it = family.family_member_.begin();
          for (; it != family.family_member_.end(); ++it)
          {
            id = it->first;
            helper.push_back(std::make_pair(id, it->second));
            BlockCollect* block =  block_manager.insert(id, now, false);
            assert(NULL != block);
            block_manager.set_family_id(id, INVALID_SERVER_ID, family.family_id_);
            id_factory_.update(id);
            str << id << ":" << it->second << ":";
          }
          rt = family_manager.insert(family.family_id_, family.family_aid_info_, helper, now);
          if (EXIT_ELEMENT_EXIST == rt)
            rt = TFS_SUCCESS;
          if (TFS_SUCCESS != rt)
            TBSYS_LOG(WARN, "load family information error,family id: %"PRI64_PREFIX"d, ret: %d", family.family_id_, ret);
          TBSYS_LOG(DEBUG, "FAMILY: %s, rt: %d", str.str().c_str(), rt);
        }
        if (!infos.empty())
          ++start_family_id;
        times++;
        size += infos.size();
      }
      while (TAIR_HAS_MORE_DATA == ret || (TAIR_RETURN_SUCCESS == ret && infos.size() == limit));
      TIMER_END();
      TBSYS_LOG(DEBUG, "SCAN FAMILY chunk: %d, scan_times: %d, size: %d, cost: %ld",
          chunk, times, size, TIMER_DURATION());
      return ret;
    }

    int OpLogSyncManager::scan_all_family_log_()
    {
      int64_t start_family_id = 0;
      std::vector<common::FamilyInfo> infos;
      int32_t ret = TAIR_RETURN_SUCCESS, rt = TFS_SUCCESS;
      FamilyManager& family_manager = manager_.get_family_manager();
      const uint32_t limit = RANGE_DEFAULT_LIMIT; // tair default limit 1000
      do
      {
        infos.clear();
        assert(NULL != dbhelper_[DEFATUL_TAIR_INDEX]);
        ret = dbhelper_[DEFATUL_TAIR_INDEX]->scan(infos, start_family_id,DELETE_FAMILY_CHUNK_DEFAULT_VALUE, true,  GFactory::get_runtime_info().peer_ip_port_, limit);
        std::vector<common::FamilyInfo>::const_iterator iter = infos.begin();
        for (; iter != infos.end(); ++iter)
        {
          const FamilyInfo& family = (*iter);
          std::stringstream str;
          str << family.family_id_ << ":" << family.family_aid_info_ << ":";
          if (start_family_id < family.family_id_)
            start_family_id = family.family_id_;
          rt = family_manager.del_family(family.family_id_);
          if (EXIT_NO_FAMILY == rt)
            rt = TFS_SUCCESS;
          if (TFS_SUCCESS != rt)
            TBSYS_LOG(WARN, "del family information error,family id: %"PRI64_PREFIX"d, ret: %d", family.family_id_, rt);
          else
            rt = dbhelper_[DEFATUL_TAIR_INDEX]->del_family(family.family_id_, true, false, GFactory::get_runtime_info().peer_ip_port_);

          TBSYS_LOG(INFO, "DEL FAMILY: %s, rt: %d", str.str().c_str(), rt);
        }
        if (!infos.empty())
          ++start_family_id;
      }
      while (TAIR_HAS_MORE_DATA == ret || (TAIR_RETURN_SUCCESS == ret && infos.size() == limit));
      return ret;
    }

    void OpLogSyncManager::LoadFamilyInfoThreadHelper::run()
    {
      try
      {
        manager_.load_all_family_info_(thread_seqno_, load_complete_);
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
  }//end namespace nameserver
}//end namespace tfs
