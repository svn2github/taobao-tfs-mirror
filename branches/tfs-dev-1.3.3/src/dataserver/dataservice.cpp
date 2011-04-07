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
 *
 */
#include "dataservice.h"
#include "common/func.h"
#include "common/directory_op.h"
#include "client/fsname.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {
    using namespace common;
    using namespace client;
    using namespace message;
    using namespace std;

    DataService::DataService()
    {
      //init dataserver info
      memset(&data_server_info_, 0, sizeof(DataServerStatInfo));

      server_local_port_ = 0;
      stop_ = 0;
      need_send_blockinfo_[0] = 1;
      need_send_blockinfo_[1] = 1;
      set_flag_[0] = false;
      set_flag_[1] = false;
      hb_ip_port_[0] = 0;
      hb_ip_port_[1] = 0;
      ns_ip_port_ = 0; //nameserver ip port;

      repl_block_ = NULL;
      compact_block_ = NULL;
      sync_mirror_ = NULL;
      sync_mirror_status_ = 0;

      hb_client_[0] = NULL;
      hb_client_[1] = NULL;
      client_ = NULL;
      compact_client_ = NULL;
      thread_pids_ = NULL;

      max_cpu_usage_ = SYSPARAM_DATASERVER.max_cpu_usage_;
    }

    DataService::~DataService()
    {
      CLIENT_POOL.release_client(client_);
      CLIENT_POOL.release_client(hb_client_[0]);
      CLIENT_POOL.release_client(hb_client_[1]);
      CLIENT_POOL.release_client(compact_client_);

      client_ = NULL;
      hb_client_[0] = NULL;
      hb_client_[1] = NULL;
      compact_client_ = NULL;
    }

    int DataService::init(const std::string& server_index)
    {
      server_index_ = server_index;
      //set name server ip
      int ret = set_ns_ip();
      if (TFS_SUCCESS != ret)
      {
        return ret;
      }

      data_server_info_.startup_time_ = time(NULL);
      IpAddr* adr = reinterpret_cast<IpAddr*>(&data_server_info_.id_);
      adr->ip_ = Func::get_local_addr(SYSPARAM_DATASERVER.dev_name_);
      adr->port_ = SYSPARAM_DATASERVER.local_ds_port_;
      TBSYS_LOG(INFO, "dataserver listen port: %d", adr->port_);

      server_local_port_ = adr->port_;

      //init file number to management
      uint64_t file_number = ((adr->ip_ & 0xFFFFFF00) | (adr->port_ & 0xFF));
      file_number = file_number << 32;
      data_management_.set_file_number(file_number);
      ds_requester_.init(data_server_info_.id_, ns_ip_port_, &data_management_);
      return TFS_SUCCESS;
    }

    int DataService::set_ns_ip()
    {
      ns_ip_port_ = 0;
      IpAddr* adr = reinterpret_cast<IpAddr*> (&ns_ip_port_);
      uint32_t ip = Func::get_addr(SYSPARAM_DATASERVER.local_ns_ip_);
      if (0 == ip)
      {
        TBSYS_LOG(ERROR, "nameserver ip is error.");
      }
      else
      {
        adr->ip_ = ip;
        adr->port_ = SYSPARAM_DATASERVER.local_ns_port_;
      }

      char* ip_list = SYSPARAM_DATASERVER.ns_addr_list_;
      if (NULL == ip_list)
      {
        TBSYS_LOG(ERROR, "nameserver real ip list is error");
      }
      else
      {
        std::vector <uint64_t> ns_ip_list;
        int32_t buffer_len = 256;
        char buffer[buffer_len];
        memset(buffer, 0, sizeof(buffer));
        strncpy(buffer, ip_list, buffer_len);
        char* t = NULL;
        char* s = buffer;
        while (NULL != (t = strsep(&s, "|")))
        {
          ns_ip_list.push_back(Func::get_addr(t));
        }

        if (ns_ip_list.size() < 1)
        {
          TBSYS_LOG(WARN, "must have one ns, check your ns' list");
          return TFS_ERROR;
        }

        if (ns_ip_list.size() != 2)
        {
          TBSYS_LOG(DEBUG, "must have two ns, check your ns' list");
          need_send_blockinfo_[1] = 0;
        }

        for (uint32_t i = 0; i < ns_ip_list.size(); ++i)
        {
          adr = reinterpret_cast<IpAddr*>(&hb_ip_port_[i]);
          adr->ip_ = ns_ip_list[i];
          if (0 == adr->ip_)
          {
            TBSYS_LOG(ERROR, "nameserver real ip: %s list is error", ip_list);
            if (0 == i)
            {
              return TFS_ERROR;
            }
          }
          else
          {
            adr->port_ = (reinterpret_cast<IpAddr*>(&ns_ip_port_))->port_;
          }
          set_flag_[i] = true;

          if (1 == i)
          {
            break;
          }
        }
      }
      return TFS_SUCCESS;
    }

    int DataService::start(VINT* pids)
    {
      client_ = CLIENT_POOL.get_client(ns_ip_port_);
      hb_client_[0] = CLIENT_POOL.get_client(hb_ip_port_[0]);
      hb_client_[1] = CLIENT_POOL.get_client(hb_ip_port_[1]);
      compact_client_ = CLIENT_POOL.get_client(ns_ip_port_);
      thread_pids_ = pids;

      repl_block_ = new ReplicateBlock(&client_mutex_, client_);
      compact_block_ = new CompactBlock(&compact_mutext_, compact_client_, data_server_info_.id_);

      //backup type:1.tfs 2.nfs
      int backup_type = SYSPARAM_DATASERVER.tfs_backup_type_;
      TBSYS_LOG(INFO, "backup type: %d\n", SYSPARAM_DATASERVER.tfs_backup_type_);
      sync_mirror_ = new SyncBase(backup_type);

      int ret = data_management_.init_block_files(SysParam::instance().filesystem_param());
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "dataservice::start, init block files fail! ret: %d\n", ret);
        return ret;
      }

      data_management_.get_ds_filesystem_info(data_server_info_.block_count_, data_server_info_.use_capacity_,
          data_server_info_.total_capacity_);
      data_server_info_.current_load_ = Func::get_load_avg();
      TBSYS_LOG(INFO,
          "block file status, block count: %d, use capacity: %" PRI64_PREFIX "d, total capacity: %" PRI64_PREFIX "d",
          data_server_info_.block_count_, data_server_info_.use_capacity_, data_server_info_.total_capacity_);

      block_checker_.init(data_server_info_.id_, &ds_requester_);

      //start connect namesever
      if (0 == ns_ip_port_)
      {
        TBSYS_LOG(ERROR, "nameserver ip not set.");
        return TFS_ERROR;
      }

      //retry
      while (0 == stop_ && TFS_SUCCESS != client_->connect())
      {
        TBSYS_LOG(ERROR, "connect to nameserver fail, sleep 5s, retry\n");
        Func::sleep(5, &stop_);
      }
      if (stop_)
        return TFS_ERROR;

      // heartbeat
      while (0 == stop_ && (TFS_SUCCESS != hb_client_[0]->connect() && TFS_SUCCESS != hb_client_[1]->connect()))
      {
        TBSYS_LOG(ERROR, "hb connect to nameserver fail, sleep 5s, retry\n");
        Func::sleep(5, &stop_);
      }
      if (stop_)
        return TFS_ERROR;

      // compact
      while (0 == stop_ && TFS_SUCCESS != compact_client_->connect())
      {
        TBSYS_LOG(ERROR, "compact connect to nameserver fail, sleep 5s, retry\n");
        Func::sleep(5, &stop_);
      }
      if (stop_)
        return TFS_ERROR;

      pthread_t tid;
      //start heartbeat thread
      pthread_create(&tid, NULL, DataService::do_heart, this);
      thread_pids_->push_back(tid);

      //start check expire data thread
      pthread_create(&tid, NULL, DataService::do_check, this);
      thread_pids_->push_back(tid);

      //start replicate thread
      int32_t replicate_thread_count = SYSPARAM_DATASERVER.replicate_thread_count_;
      for (int32_t i = 0; i < replicate_thread_count; ++i)
      {
        pthread_create(&tid, NULL, ReplicateBlock::do_replicate_block, repl_block_);
        thread_pids_->push_back(tid);
      }

      //start compact thread
      pthread_create(&tid, NULL, CompactBlock::do_compact_block, compact_block_);
      thread_pids_->push_back(tid);

      //start sync thread
      pthread_create(&tid, NULL, SyncBase::do_sync_mirror, sync_mirror_);
      thread_pids_->push_back(tid);

      //set process thread num for client
      int32_t thread_count = SYSPARAM_DATASERVER.client_thread_client_;
      task_queue_thread_.setThreadParameter(thread_count, this, NULL);
      task_queue_thread_.start();

      //set process thread num for dataservers
      thread_count = SYSPARAM_DATASERVER.server_thread_client_;
      ds_task_queue_thread_.setThreadParameter(thread_count, this, NULL);
      ds_task_queue_thread_.start();

      //set write and read log
      init_log_file(READ_STAT_LOGGER, SYSPARAM_DATASERVER.read_stat_log_file_);
      init_log_file(WRITE_STAT_LOGGER, SYSPARAM_DATASERVER.write_stat_log_file_);

      TBSYS_LOG(INFO, "dataservice start");

      return TFS_SUCCESS;
    }

    int DataService::stop()
    {
      stop_mutex_.lock();
      if (stop_)
      {
        stop_mutex_.unlock();
        return TFS_SUCCESS;
      }
      TBSYS_LOG(INFO, "Dataserver stopping...");
      //stop task queue
      task_queue_thread_.stop();
      ds_task_queue_thread_.stop();
      task_queue_thread_.wait();
      ds_task_queue_thread_.wait();
      stop_ = 1;
      stop_mutex_.unlock();
      //sleep(1);

      if (NULL != sync_mirror_)
      {
        sync_mirror_->stop();
      }
      if (NULL != repl_block_)
      {
        repl_block_->stop();
      }
      if (NULL != compact_block_)
      {
        compact_block_->stop();
      }

      block_checker_.stop();

      void* retp;
      for (uint32_t i = 0; i < thread_pids_->size(); ++i)
      {
        pthread_join(thread_pids_->at(i), &retp);
      }
      thread_pids_->clear();

      tbsys::gDelete(repl_block_);
      tbsys::gDelete(compact_block_);
      tbsys::gDelete(sync_mirror_);

      TBSYS_LOG(INFO, "Dataserver stopped.");
      return TFS_SUCCESS;
    }

    int DataService::wait()
    {
      task_queue_thread_.wait();
      ds_task_queue_thread_.wait();
      return TFS_SUCCESS;
    }

    void* DataService::do_heart(void *args)
    {
      TBSYS_LOG(INFO, "tid: %u", Func::gettid());
      DataService *ds = reinterpret_cast<DataService*> (args);
      ds->run_heart();
      return NULL;
    }

    int DataService::run_heart()
    {
      //sleep for a while, waiting for listen port establish
      sleep(SYSPARAM_DATASERVER.heart_interval_);
      while (!stop_)
      {
        data_management_.get_ds_filesystem_info(data_server_info_.block_count_, data_server_info_.use_capacity_,
            data_server_info_.total_capacity_);
        data_server_info_.current_load_ = Func::get_load_avg();
        data_server_info_.current_time_ = time(NULL);
        send_blocks_to_ns(0);
        send_blocks_to_ns(1);

        // sleep
        Func::sleep(SYSPARAM_DATASERVER.heart_interval_, &stop_);
        if (DATASERVER_STATUS_DEAD == data_server_info_.status_)
        {
          break;
        }

        cpu_metrics_.summary();
      }
      return TFS_SUCCESS;
    }

    int DataService::stop_heart()
    {
      TBSYS_LOG(INFO, "stop heartbeat...");
      data_server_info_.status_ = DATASERVER_STATUS_DEAD;
      send_blocks_to_ns(0);
      send_blocks_to_ns(1);
      return TFS_SUCCESS;
    }

    void* DataService::do_check(void *args)
    {
      TBSYS_LOG(INFO, "tid: %u", Func::gettid());
      DataService *ds = reinterpret_cast<DataService*> (args);
      ds->run_check();
      return NULL;
    }

    int DataService::run_check()
    {
      int32_t last_rlog = 0;
      tzset();
      int zonesec = 86400 + timezone;
      while (!stop_)
      {
        //check datafile
        data_management_.gc_data_file();
        if (stop_)
          break;

        //check repair block
        block_checker_.consume_repair_task();
        if (stop_)
          break;

        //check clonedblock
        repl_block_->expire_cloned_block_map();
        if (stop_)
          break;

        // check compact block
        compact_block_->expire_compact_block_map();
        if (stop_)
          break;

        int32_t current_time = time(NULL);
        // check log: write a new log everyday and expire error block
        if (current_time % 86400 >= zonesec && current_time % 86400 < zonesec + 300 && last_rlog < current_time - 600)
        {
          last_rlog = current_time;
          TBSYS_LOGGER.rotateLog(SYSPARAM_DATASERVER.log_file_.c_str());
          READ_STAT_LOGGER.rotateLog(SYSPARAM_DATASERVER.read_stat_log_file_.c_str());
          WRITE_STAT_LOGGER.rotateLog(SYSPARAM_DATASERVER.write_stat_log_file_.c_str());

          // expire error block
          block_checker_.expire_error_block();
        }
        if (stop_)
          break;

        // check stat
        count_mutex_.lock();
        visit_stat_.check_visit_stat();
        count_mutex_.unlock();
        if (stop_)
          break;

        if (read_stat_buffer_.size() >= READ_STAT_LOG_BUFFER_LEN)
        {
          int64_t time_start = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "---->START DUMP READ INFO. buffer size: %u, start time: %" PRI64_PREFIX "d", read_stat_buffer_.size(), time_start);
          read_stat_mutex_.lock();
          int per_log_size = FILE_NAME_LEN + 2; //two space
          char read_log_buffer[READ_STAT_LOG_BUFFER_LEN * per_log_size + 1];
          int32_t loops = read_stat_buffer_.size() / READ_STAT_LOG_BUFFER_LEN;
          int32_t remain = read_stat_buffer_.size() % READ_STAT_LOG_BUFFER_LEN;
          int32_t index = 0, offset;
          int32_t inner_loop = 0;
          //flush all record to log
          for (int32_t i = 0; i <= loops; ++i)
          {
            memset(read_log_buffer, 0, READ_STAT_LOG_BUFFER_LEN * per_log_size + 1);
            offset = 0;
            if (i == loops)
            {
              inner_loop = remain;
            }
            else
            {
              inner_loop = READ_STAT_LOG_BUFFER_LEN;
            }

            for (int32_t j = 0; j < inner_loop; ++j)
            {
              index = i * READ_STAT_LOG_BUFFER_LEN + j;
              FSName fs_name;
              fs_name.set_block_id(read_stat_buffer_[index].first);
              fs_name.set_file_id(read_stat_buffer_[index].second);
              /* per_log_size + 1: add null */
              snprintf(read_log_buffer + offset, per_log_size + 1, "  %s", fs_name.get_name());
              offset += per_log_size;
            }
            read_log_buffer[offset] = '\0';
            READ_STAT_LOG(INFO, "%s", read_log_buffer);
          }
          read_stat_buffer_.clear();
          read_stat_mutex_.unlock();
          int64_t time_end = tbsys::CTimeUtil::getTime();
          TBSYS_LOG(INFO, "Dump read info.end time: %" PRI64_PREFIX "d. Cost Time: %" PRI64_PREFIX "d\n", time_end, time_end - time_start);
        }

        Func::sleep(SYSPARAM_DATASERVER.check_interval_, &stop_);
      }

      data_management_.remove_data_file();
      return TFS_SUCCESS;
    }

    int DataService::send_message_to_slave_ds(Message* message, const VUINT64& ds_list)
    {
      int ret = TFS_SUCCESS;
      int32_t ds_size = static_cast<int32_t>(ds_list.size()) - 1;
      for (int32_t i = ds_size; i >= 0; --i)
      {
        if (ds_list[i] == data_server_info_.id_)
        {
          continue;
        }
        // send to port(port+1)
        uint64_t ds_ip = Func::addr_inc_port(ds_list[i], 1);
        // client send
        ret = send_message_to_server(ds_ip, message, NULL);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "send message to server to ds ip: %s fail.\n", tbsys::CNetUtil::addrToString(ds_ip).c_str());
          return ret;
        }
      }

      return TFS_SUCCESS;
    }

    int DataService::post_message_to_server(Message* message, const VUINT64& ds_list)
    {
      VUINT64 erase_self;
      for (uint32_t i = 0; i < ds_list.size(); ++i)
      {
        if (ds_list[i] == data_server_info_.id_)
        {
          continue;
        }
        erase_self.push_back(ds_list[i]);
      }
      if (erase_self.size() == 0)
      {
        return 0;
      }
      if (async_post_message_to_servers(message, erase_self, this) == TFS_SUCCESS)
      {
        return 1;
      }
      else
      {
        return -1;
      }
    }

    int DataService::command_done(Message* send_message, bool status, const string& error)
    {
      if (WRITE_DATA_MESSAGE == send_message->get_message_type())
      {
        WriteDataMessage* message = dynamic_cast<WriteDataMessage*> (send_message);
        if (!status)
        {
          TBSYS_LOG(ERROR,
              "WriteDataMessage fail. chid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, number: %" PRI64_PREFIX "u\n",
              message->getChannelId(), message->get_block_id(), message->get_file_id(), message->get_file_number());
          data_management_.erase_data_file(message->get_file_number());
        }

        return DefaultAsyncCallback::command_done(send_message, status, error);
      }
      else if (RENAME_FILE_MESSAGE == send_message->get_message_type())
      {
        RenameFileMessage* message = dynamic_cast<RenameFileMessage*> (send_message);
        uint32_t block_id = message->get_block_id();
        uint64_t file_id = message->get_file_id();
        uint64_t new_file_id = message->get_new_file_id();
        int32_t option_flag = message->get_option_flag();
        if (status && 0 == (option_flag & TFS_FILE_NO_SYNC_LOG))
        {
          sync_mirror_->write_sync_log(OPLOG_RENAME, block_id, new_file_id, file_id);
        }
        else if (!status)
        {
          TBSYS_LOG(
              ERROR,
              "RenameFileMessage fail. chid: %d, blockid: %u, fileid: %" PRI64_PREFIX "u, new fileid: %" PRI64_PREFIX "u\n",
              message->getChannelId(), block_id, file_id, new_file_id);
        }
        return DefaultAsyncCallback::command_done(send_message, status, error);
      }
      else if (send_message->get_message_type() == UNLINK_FILE_MESSAGE)
      {
        // do nothing. unlink don't care other ds response
        UnlinkFileMessage* message = dynamic_cast<UnlinkFileMessage*> (send_message);
        if (!status)
        {
          TBSYS_LOG(WARN, "UnlinkFileMessage fail. chid:%d, blockid: %u, fileid: %" PRI64_PREFIX "u\n",
              message->getChannelId(), message->get_block_id(), message->get_file_id());
        }
        return TFS_SUCCESS;
      }
      else
      {
        TBSYS_LOG(ERROR, "command done handle error message type: %d\n", send_message->get_message_type());
      }

      return TFS_ERROR;
    }

    bool DataService::access_deny(Message* message)
    {
      tbnet::Connection* conn = message->get_connection();
      if (!conn)
        return false;
      uint64_t peer_id = conn->getPeerId();
      int32_t type = message->get_message_type();
      if (type == READ_DATA_MESSAGE || type == READ_DATA_MESSAGE_V2)
        return acl_.deny(peer_id, AccessControl::READ);
      if (type == WRITE_DATA_MESSAGE || type == CLOSE_FILE_MESSAGE)
        return acl_.deny(peer_id, AccessControl::WRITE);
      if (type == UNLINK_FILE_MESSAGE)
        return acl_.deny(peer_id, AccessControl::UNLINK);
      return false;
    }

    tbnet::IPacketHandler::HPRetCode DataService::handlePacket(tbnet::Connection* connection, tbnet::Packet* packet)
    {
      if (NULL == connection || NULL == packet)
      {
        TBSYS_LOG(ERROR, "connection or packet ptr NULL");
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      //control packet
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(ERROR, "ControlPacket, cmd: %d", dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
        return tbnet::IPacketHandler::FREE_CHANNEL;
      }

      Message* bp = dynamic_cast<Message*>(packet);
      bp->set_connection(connection);
      bp->set_direction(DIRECTION_RECEIVE);

      if (connection != NULL && connection->getLocalPort() == server_local_port_ + 1)
      {
        ds_task_queue_thread_.push(bp);
      }
      else
      {
        // add access control by message type
        if (!access_deny(bp))
        {
          task_queue_thread_.push(bp);
        }
        else
        {
          MessageFactory::send_error_message(bp, TBSYS_LOG_LEVEL(WARN), STATUS_MESSAGE_ACCESS_DENIED,
              data_server_info_.id_, "you client %s access been denied. msgtype: %d", tbsys::CNetUtil::addrToString(
                  connection->getPeerId()).c_str(), bp->get_message_type());
          // packet denied, must free
          bp->free();
          return tbnet::IPacketHandler::FREE_CHANNEL;
        }
      }

      return tbnet::IPacketHandler::KEEP_CHANNEL;
    }

    bool DataService::handlePacketQueue(tbnet::Packet* packet, void* )
    {
      Message* message = dynamic_cast<Message*>(packet);
      if (NULL == message)
      {
        TBSYS_LOG(ERROR, "process packet can not convert to message\n");
        return true;
      }

      int ret = TFS_SUCCESS;
      switch (message->get_message_type())
      {
      case CREATE_FILENAME_MESSAGE:
        ret = create_file_number(dynamic_cast<CreateFilenameMessage*>(message));
        break;
      case WRITE_DATA_MESSAGE:
        ret = write_data(dynamic_cast<WriteDataMessage*>(message));
        break;
      case CLOSE_FILE_MESSAGE:
        ret = close_write_file(dynamic_cast<CloseFileMessage*>(message));
        break;
      case WRITE_RAW_DATA_MESSAGE:
        ret = write_raw_data(dynamic_cast<WriteRawDataMessage*>(message));
        break;
      case WRITE_INFO_BATCH_MESSAGE:
        ret = batch_write_info(dynamic_cast<WriteInfoBatchMessage*>(message));
        break;
      case READ_DATA_MESSAGE_V2:
        ret = read_data_v2(dynamic_cast<ReadDataMessageV2*>(message));
        break;
      case READ_DATA_MESSAGE:
        ret = read_data(dynamic_cast<ReadDataMessage*>(message));
        break;
      case READ_RAW_DATA_MESSAGE:
        ret = read_raw_data(dynamic_cast<ReadRawDataMessage*>(message));
        break;
      case FILE_INFO_MESSAGE:
        ret = read_file_info(dynamic_cast<FileInfoMessage*>(message));
        break;

      case UNLINK_FILE_MESSAGE:
        ret = unlink_file(dynamic_cast<UnlinkFileMessage*>(message));
        break;
      case RENAME_FILE_MESSAGE:
        ret = rename_file(dynamic_cast<RenameFileMessage*>(message));
        break;

      case NEW_BLOCK_MESSAGE:
        ret = new_block(dynamic_cast<NewBlockMessage*>(message));
        break;
      case REMOVE_BLOCK_MESSAGE:
        ret = remove_block(dynamic_cast<RemoveBlockMessage*>(message));
        break;

      case LIST_BLOCK_MESSAGE:
        ret = list_blocks(dynamic_cast<ListBlockMessage*>(message));
        break;

      case LIST_BITMAP_MESSAGE:
        ret = query_bit_map(dynamic_cast<ListBitMapMessage*>(message));
        break;

      case REPLICATE_BLOCK_MESSAGE:
        ret = replicate_block_cmd(dynamic_cast<ReplicateBlockMessage*>(message));
        break;

      case COMPACT_BLOCK_MESSAGE:
        ret = compact_block_cmd(dynamic_cast<CompactBlockMessage*>(message));
        break;

      case CRC_ERROR_MESSAGE:
        ret = crc_error_cmd(dynamic_cast<CrcErrorMessage*>(message));
        break;

      case GET_BLOCK_INFO_MESSAGE:
        ret = get_block_info(dynamic_cast<GetBlockInfoMessage*>(message));
        break;
      case RESET_BLOCK_VERSION_MESSAGE:
        ret = reset_block_version(dynamic_cast<ResetBlockVersionMessage*>(message));
        break;
      case GET_SERVER_STATUS_MESSAGE:
        ret = get_server_status(dynamic_cast<GetServerStatusMessage*>(message));
        break;
      case RELOAD_CONFIG_MESSAGE:
        ret = reload_config(dynamic_cast<ReloadConfigMessage*>(message));
        break;
      case STATUS_MESSAGE:
        ret = get_ping_status(dynamic_cast<StatusMessage*>(message));
        break;
      case CLIENT_CMD_MESSAGE:
        ret = client_command(dynamic_cast<ClientCmdMessage*>(message));
        break;
      default:
        TBSYS_LOG(ERROR, "process packet: %s, type: %d\n", message->get_name(), message->get_message_type());
        ret = TFS_ERROR;
        break;
      }
      if (TFS_SUCCESS != ret)
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "execute message fail, type: %d. ret: %d\n", message->get_message_type(), ret);
      }
      return true;
    }

    int DataService::create_file_number(CreateFilenameMessage* message)
    {
      TIMER_START();
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();

      TBSYS_LOG(DEBUG, "create file: blockid: %u, fileid: %" PRI64_PREFIX "u", block_id, file_id);
      uint64_t file_number = 0;
      int ret = data_management_.create_file(block_id, file_id, file_number);
      if (TFS_SUCCESS != ret)
      {
        if (EXIT_NO_LOGICBLOCK_ERROR == ret) //need to update BlockInfo
        {
          TBSYS_LOG(ERROR, "create file: blockid: %u is lost. ask master to update.", block_id);
          if (TFS_SUCCESS != ds_requester_.req_update_block_info(block_id, UPDATE_BLOCK_MISSING))
          {
            TBSYS_LOG(ERROR, "create file: blockid: %u is null. req update BlockInfo failed", block_id);
          }
        }
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "create file failed. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d.", block_id, file_id, ret);
      }
      else
      {
        RespCreateFilenameMessage* resp_cfn_msg = new RespCreateFilenameMessage();
        resp_cfn_msg->set_block_id(block_id);
        resp_cfn_msg->set_file_id(file_id);
        resp_cfn_msg->set_file_number(file_number);
        message->reply_message(resp_cfn_msg);
      }

      TIMER_END();
      TBSYS_LOG(INFO,
          "create file %s. filenumber: %" PRI64_PREFIX "u, blockid: %u, fileid: %" PRI64_PREFIX "u, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", file_number, block_id, file_id, TIMER_DURATION());
      return TFS_SUCCESS;
    }

    int DataService::write_data(WriteDataMessage* message)
    {
      TIMER_START();
      WriteDataInfo write_info = message->get_write_info();
      int32_t lease_id = message->get_lease_id();
      int32_t version = message->get_block_version();
      char* msg_data = message->get_data();

      TBSYS_LOG(
          DEBUG,
          "write data start, blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, version: %u, leaseid: %u, isserver: %d\n",
          write_info.block_id_, write_info.file_id_, write_info.file_number_, version, lease_id, write_info.is_server_);

      UpdateBlockType repair = UPDATE_BLOCK_NORMAL;
      int ret = data_management_.write_data(write_info, lease_id, version, msg_data, repair);
      if (EXIT_NO_LOGICBLOCK_ERROR == ret)
      {
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "write data failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
            write_info.block_id_, write_info.file_id_, ret);
      }
      else if (EXIT_BLOCK_DS_VERSION_ERROR == ret || EXIT_BLOCK_NS_VERSION_ERROR == ret)
      {
        MessageFactory::send_error_message(
            message,
            TBSYS_LOG_LEVEL(ERROR), ret,
            data_server_info_.id_,
            "write data failed. block version error. blockid: %u, fileid: %" PRI64_PREFIX "u, error ret: %d, repair: %d",
            write_info.block_id_, write_info.file_id_, ret, repair);
        if (TFS_SUCCESS != ds_requester_.req_update_block_info(write_info.block_id_, repair))
        {
          TBSYS_LOG(ERROR, "req update block info failed. blockid: %u, repair: %d", write_info.block_id_, repair);
        }
      }
      else if (EXIT_DATAFILE_OVERLOAD == ret || EXIT_DATA_FILE_ERROR == ret)
      {
        if (Master_Server_Role == write_info.is_server_)
        {
          ds_requester_.req_block_write_complete(write_info.block_id_, lease_id, TFS_ERROR);
        }
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "write data fail. blockid: %u, fileid: %" PRI64_PREFIX "u. ret: %d", write_info.block_id_,
            write_info.file_id_, ret);
      }
      else
      {
        // if master ds, write data to other slave ds
        // == Write_Master_Server is master
        if (Master_Server_Role == write_info.is_server_)
        {
          message->set_server(Slave_Server_Role);
          message->set_lease_id(lease_id);
          message->set_block_version(version);
          ret = post_message_to_server(message, message->get_ds_list());
          if (0 == ret)
          {
            //no slave
            message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
          }
          else if (ret < 0)
          {
            ds_requester_.req_block_write_complete(write_info.block_id_, lease_id, EXIT_SENDMSG_ERROR);
            MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
                "write data fail to other dataserver (send): blockid: %u, fileid: %" PRI64_PREFIX "u, datalen: %d",
                write_info.block_id_, write_info.file_id_, write_info.length_);
          }
        }
        else
        {
          message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
        }
      }

      TIMER_END();
      TBSYS_LOG(
          INFO,
          "write data %s. filenumber: %" PRI64_PREFIX "u, blockid: %u, fileid: %" PRI64_PREFIX "u, version: %u, leaseid: %u, role: %s, cost time: %" PRI64_PREFIX "d",
          ret >= 0 ? "success": "fail", write_info.file_number_, write_info.block_id_, write_info.file_id_, version, lease_id, Master_Server_Role == write_info.is_server_ ? "master" : "slave", TIMER_DURATION());
      return TFS_SUCCESS;
    }

    int DataService::close_write_file(CloseFileMessage* message)
    {
      TIMER_START();
      CloseFileInfo close_file_info = message->get_close_file_info();

      int32_t lease_id = message->get_lease_id();
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(
          DEBUG,
          "close write file, blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, leaseid: %u, from: %s\n",
          close_file_info.block_id_, close_file_info.file_id_, close_file_info.file_number_, lease_id,
          tbsys::CNetUtil::addrToString(peer_id).c_str());

      int32_t write_file_size = 0;
      int ret = data_management_.close_write_file(close_file_info, write_file_size);
      if (TFS_SUCCESS != ret)
      {
        if (EXIT_DATAFILE_EXPIRE_ERROR == ret)
        {
          MessageFactory::send_error_message(
              message,
              TBSYS_LOG_LEVEL(ERROR), ret,
              data_server_info_.id_,
              "datafile is null(maybe expired). blockid: %u, fileid: %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u, ret: %d",
              close_file_info.block_id_, close_file_info.file_id_, close_file_info.file_number_, ret);
        }
        else if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "close write file failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
              close_file_info.block_id_, close_file_info.file_id_, ret);
        }
        else if (TFS_SUCCESS != ret)
        {
          try_add_repair_task(close_file_info.block_id_, ret);
          if (CLOSE_FILE_SLAVER != close_file_info.mode_)
          {
            ds_requester_.req_block_write_complete(close_file_info.block_id_, lease_id, ret);
          }
          MessageFactory::send_error_message(
              message,
              TBSYS_LOG_LEVEL(ERROR), ret,
              data_server_info_.id_,
              "close write file error. blockid: %u, fileid : %" PRI64_PREFIX "u, filenumber: %" PRI64_PREFIX "u. ret: %d",
              close_file_info.block_id_, close_file_info.file_id_, close_file_info.file_number_, ret);
        }
      }
      else
      {
        BlockInfo* blk = NULL;
        int32_t visit_count = 0;
        ret = data_management_.get_block_info(close_file_info.block_id_, blk, visit_count);
        if (TFS_SUCCESS != ret)
        {
          MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "close write file failed. block is not exist. blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
              close_file_info.block_id_, close_file_info.file_id_, ret);
        }
        else
        {
          //if it is master DS. Send to other slave ds
          if (CLOSE_FILE_SLAVER != close_file_info.mode_)
          {
            do_stat(peer_id, write_file_size, write_file_size, 0, AccessStat::WRITE_BYTES);

            message->set_mode(CLOSE_FILE_SLAVER);
            message->set_block(blk);

            ret = send_message_to_slave_ds(message, message->get_ds_list());
            if (TFS_SUCCESS != ret)
            {
              // other ds failed, release lease
              ds_requester_.req_block_write_complete(close_file_info.block_id_, lease_id, TFS_ERROR);
              MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
                  "close write file to other dataserver fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                  close_file_info.block_id_, close_file_info.file_id_, ret);
            }
            else
            {
              //commit
              ret = ds_requester_.req_block_write_complete(close_file_info.block_id_, lease_id, TFS_SUCCESS);
              if (TFS_SUCCESS == ret)
              {
                //sync to mirror
                int option_flag = message->get_option_flag();
                if (0 == (option_flag & TFS_FILE_NO_SYNC_LOG))
                {
                  TBSYS_LOG(INFO, " write sync log, blockid: %u, fileid: %" PRI64_PREFIX "u", close_file_info.block_id_,
                      close_file_info.file_id_);
                  ret = sync_mirror_->write_sync_log(OPLOG_INSERT, close_file_info.block_id_,
                      close_file_info.file_id_);
                }
              }

              if (TFS_SUCCESS == ret)
              {
                message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
              }
              else
              {
                TBSYS_LOG(ERROR,
                    "rep block write complete or write sync log fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d",
                    close_file_info.block_id_, close_file_info.file_id_, ret);
                message->reply_message(new StatusMessage(STATUS_MESSAGE_ERROR));
              }
            }
          }
          else
          {
            //slave will save seqno to prevent from the conflict when this block change to master block
            BlockInfo* copyblk = message->get_block();
            if (NULL != copyblk)
            {
              blk->seq_no_ = copyblk->seq_no_;
            }
            message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
          }
        }
      }

      TIMER_END();
      TBSYS_LOG(INFO, "close file %s. filenumber: %" PRI64_PREFIX "u, blockid: %u, fileid: %" PRI64_PREFIX "u, peerip: %s, role: %s, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", close_file_info.file_number_, close_file_info.block_id_,
          close_file_info.file_id_, tbsys::CNetUtil::addrToString(peer_id).c_str(),
          CLOSE_FILE_SLAVER != close_file_info.mode_ ? "master" : "slave", TIMER_DURATION());
      WRITE_STAT_LOG(INFO, "blockid: %u, fileid: %" PRI64_PREFIX "u, peerip: %s",
          close_file_info.block_id_, close_file_info.file_id_, tbsys::CNetUtil::addrToString(peer_id).c_str());
      return TFS_SUCCESS;
    }

    int DataService::read_data_v2(ReadDataMessageV2* message)
    {
      TIMER_START();
      RespReadDataMessageV2* resp_rd_v2_msg = new RespReadDataMessageV2();
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t read_len = message->get_length();
      int32_t read_offset = message->get_offset();
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(DEBUG, "blockid: %u, fileid: %" PRI64_PREFIX "u, read len: %d, read offset: %d, resp: %p", block_id,
          file_id, read_len, read_offset, resp_rd_v2_msg);
      //add FileInfo size if the first fragment
      int32_t real_read_len = 0;
      if (0 == read_offset)
      {
        real_read_len = read_len + FILEINFO_SIZE;
      }
      else //not the first fragment
      {
        real_read_len = read_len;
        read_offset += FILEINFO_SIZE;
      }

      char* tmp_data_buffer = new char[real_read_len];
      int ret = data_management_.read_data(block_id, file_id, read_offset, real_read_len, tmp_data_buffer);
      if (TFS_SUCCESS != ret)
      {
        try_add_repair_task(block_id, ret);
        tbsys::gDeleteA(tmp_data_buffer);
        resp_rd_v2_msg->set_length(ret);
        message->reply_message(resp_rd_v2_msg);
      }
      else
      {
        if (0 == read_offset)
        {
          real_read_len -= FILEINFO_SIZE;
        }

        int32_t visit_file_size = reinterpret_cast<FileInfo*>(tmp_data_buffer)->size_;
        char* packet_data = resp_rd_v2_msg->alloc_data(real_read_len);
        if (0 != real_read_len)
        {
          if (NULL == packet_data)
          {
            tbsys::gDelete(resp_rd_v2_msg);
            tbsys::gDeleteA(tmp_data_buffer);
            TBSYS_LOG(ERROR, "alloc data failed, blockid: %u, fileid: %" PRI64_PREFIX "u, real len: %d", block_id,
                file_id, real_read_len);
            ret = TFS_ERROR;
          }

          if (TFS_SUCCESS == ret)
          {
            if (0 == read_offset)
            {
              //set FileInfo
              reinterpret_cast<FileInfo*>(tmp_data_buffer)->size_ -= FILEINFO_SIZE;
              resp_rd_v2_msg->set_file_info(reinterpret_cast<FileInfo*>(tmp_data_buffer));
              memcpy(packet_data, tmp_data_buffer + FILEINFO_SIZE, real_read_len);
            }
            else
            {
              memcpy(packet_data, tmp_data_buffer, real_read_len);
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          //set to connection
          message->reply_message(resp_rd_v2_msg);
          tbsys::gDeleteA(tmp_data_buffer);
          do_stat(peer_id, visit_file_size, real_read_len, read_offset, AccessStat::READ_BYTES);
        }
      }

      TIMER_END();
      TBSYS_LOG(INFO, "read v2 %s. blockid: %u, fileid: %" PRI64_PREFIX "u, read len: %d, read offset: %d, peer ip: %s, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, real_read_len, read_offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());

      read_stat_mutex_.lock();
      read_stat_buffer_.push_back(make_pair(block_id, file_id));
      read_stat_mutex_.unlock();
      return ret;
    }

    int DataService::read_data(ReadDataMessage* message)
    {
      TIMER_START();
      RespReadDataMessage* resp_rd_msg = new RespReadDataMessage();
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t read_len = message->get_length();
      int32_t read_offset = message->get_offset();
      uint64_t peer_id = message->get_connection()->getPeerId();

      //add FileInfo if the first fragment
      int32_t real_read_len = 0;
      if (0 == read_offset)
      {
        real_read_len = read_len + FILEINFO_SIZE;
      }
      else //not the first fragment
      {
        real_read_len = read_len;
        read_offset += FILEINFO_SIZE;
      }

      char* tmp_data_buffer = new char[real_read_len];
      int ret = data_management_.read_data(block_id, file_id, read_offset, real_read_len, tmp_data_buffer);
      if (TFS_SUCCESS != ret)
      {
        try_add_repair_task(block_id, ret);
        tbsys::gDeleteA(tmp_data_buffer);
        resp_rd_msg->set_length(ret);
        message->reply_message(resp_rd_msg);
      }
      else
      {
        if (0 == read_offset)
        {
          real_read_len -= FILEINFO_SIZE;
        }

        int32_t visit_file_size = reinterpret_cast<FileInfo *>(tmp_data_buffer)->size_;
        char* packet_data = resp_rd_msg->alloc_data(real_read_len);
        if (0 != real_read_len)
        {
          if (NULL == packet_data)
          {
            tbsys::gDelete(resp_rd_msg);
            tbsys::gDeleteA(tmp_data_buffer);
            TBSYS_LOG(ERROR, "alloc data failed, blockid: %u, fileid: %" PRI64_PREFIX "u, real len: %d",
                block_id, file_id, real_read_len);
            ret = TFS_ERROR;
          }

          if (TFS_SUCCESS == ret)
          {
            if (0 == read_offset)
            {
              memcpy(packet_data, tmp_data_buffer + FILEINFO_SIZE, real_read_len);
            }
            else
            {
              memcpy(packet_data, tmp_data_buffer, real_read_len);
            }
          }
        }

        if (TFS_SUCCESS == ret)
        {
          // set to connection
          message->reply_message(resp_rd_msg);
          tbsys::gDeleteA(tmp_data_buffer);
          do_stat(peer_id, visit_file_size, real_read_len, read_offset, AccessStat::READ_BYTES);
        }
      }

      TIMER_END();
      TBSYS_LOG(INFO, "read %s. blockid: %u, fileid: %" PRI64_PREFIX "u, read len: %d, read offset: %d, peer ip: %s, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, real_read_len, read_offset,
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());

      read_stat_mutex_.lock();
      read_stat_buffer_.push_back(make_pair(block_id, file_id));
      read_stat_mutex_.unlock();

      return ret;
    }

    int DataService::read_raw_data(ReadRawDataMessage* message)
    {
      RespReadRawDataMessage* resp_rrd_msg = new RespReadRawDataMessage();
      uint32_t block_id = message->get_block_id();
      int32_t read_len = message->get_length();
      int32_t read_offset = message->get_offset();

      TBSYS_LOG(DEBUG, "blockid: %u read data batch, read size: %d, offset: %d", block_id, read_len, read_offset);

      char* tmp_data_buffer = new char[read_len];
      int32_t real_read_len = read_len;
      int ret = data_management_.read_raw_data(block_id, read_offset, real_read_len, tmp_data_buffer);
      if (TFS_SUCCESS != ret)
      {
        try_add_repair_task(block_id, ret);
        tbsys::gDeleteA(tmp_data_buffer);
        resp_rrd_msg->set_length(ret);
        message->reply_message(resp_rrd_msg);
        return ret;
      }

      char* packet_data = resp_rrd_msg->alloc_data(real_read_len);
      if (0 != real_read_len)
      {
        if (NULL == packet_data)
        {
          tbsys::gDelete(resp_rrd_msg);
          tbsys::gDeleteA(tmp_data_buffer);
          TBSYS_LOG(ERROR, "allocdata fail, blockid: %u, realreadlen: %" PRI64_PREFIX "d", block_id, real_read_len);
          return TFS_ERROR;
        }
        else
        {
          memcpy(packet_data, tmp_data_buffer, real_read_len);
        }
      }
      message->set_length(real_read_len);
      message->reply_message(resp_rrd_msg);
      tbsys::gDeleteA(tmp_data_buffer);

      do_stat(0, 0, real_read_len, read_offset, AccessStat::READ_COUNT);

      return TFS_SUCCESS;
    }

    int DataService::read_file_info(FileInfoMessage* message)
    {
      TIMER_START();
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t mode = message->get_mode();

      TBSYS_LOG(DEBUG, "read file info, blockid: %u, fileid: %" PRI64_PREFIX "u, mode: %d",
          block_id, file_id, mode);
      FileInfo finfo;
      int ret = data_management_.read_file_info(block_id, file_id, mode, finfo);
      if (TFS_SUCCESS != ret)
      {
        try_add_repair_task(block_id, ret);
        MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "readfileinfo fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", block_id, file_id, ret);
      }

      RespFileInfoMessage* resp_fi_msg = new RespFileInfoMessage();
      resp_fi_msg->set_file_info(&finfo);
      message->reply_message(resp_fi_msg);
      TIMER_END();
      TBSYS_LOG(DEBUG, "read fileinfo %s. blockid: %u, fileid: %" PRI64_PREFIX "u, mode: %d, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, mode, TIMER_DURATION());
      return TFS_SUCCESS;
    }

    int DataService::rename_file(RenameFileMessage* message)
    {
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      uint64_t new_file_id = message->get_new_file_id();
      TBSYS_LOG(INFO,
          "renamefile, blockid: %u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u, ds list size: %u",
          block_id, file_id, new_file_id, message->get_ds_list().size());

      int ret = data_management_.rename_file(block_id, file_id, new_file_id);
      if (TFS_SUCCESS != ret)
      {
        try_add_repair_task(block_id, ret);
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "rename file fail, blockid: %u, fileid: %" PRI64_PREFIX "u, newfileid: %" PRI64_PREFIX "u, ret: %d",
            block_id, file_id, new_file_id, ret);
      }

      //is master
      bool is_master = false;
      if (0 == (message->is_server() & 1))
      {
        is_master = true;
      }
      // send to other ds
      if (is_master)
      {
        message->set_server();
        ret = post_message_to_server(message, message->get_ds_list());
        if (ret >= 0)
        {
          if (0 == ret)
          {
            message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
          }
          return TFS_SUCCESS;
        }
        else
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "renamefile to other dataserver");
        }
      }

      // return
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::unlink_file(UnlinkFileMessage* message)
    {
      TIMER_START();
      uint32_t block_id = message->get_block_id();
      uint64_t file_id = message->get_file_id();
      int32_t option_flag = message->get_option_flag();
      int32_t action = message->get_unlink_type();
      uint64_t peer_id = message->get_connection()->getPeerId();
      //is master
      bool is_master = false;
      if ((message->get_server() & 1) == 0)
      {
        is_master = true;
      }

      int ret = data_management_.unlink_file(block_id, file_id, action);
      if (TFS_SUCCESS != ret)
      {
        if (EXIT_NO_LOGICBLOCK_ERROR == ret)
        {
          MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "block not exist, blockid: %u", block_id);
        }
        else
        {
          try_add_repair_task(block_id, ret);
          MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(WARN), ret, data_server_info_.id_,
              "file unlink fail, blockid: %u, fileid: %" PRI64_PREFIX "u, ret: %d", block_id, file_id, ret);
        }
      }
      else
      {
        if (is_master)
        {
          message->set_server();
          //do not concern the return value of other ds
          post_message_to_server(message, message->get_ds_list());
        }

        message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));

        //sync log
        if (is_master && 0 == (option_flag & TFS_FILE_NO_SYNC_LOG))
        {
          TBSYS_LOG(DEBUG, "master dataserver: delete synclog. blockid: %d, fileid: %" PRI64_PREFIX "u, action: %d\n",
              block_id, file_id, action);
          sync_mirror_->write_sync_log(OPLOG_REMOVE, block_id, file_id, action);
        }
      }

      if (is_master && message->get_lease_id())
      {
        ds_requester_.req_block_write_complete(block_id, message->get_lease_id(), ret, UNLINK_FLAG_YES);
      }

      TIMER_END();
      TBSYS_LOG(INFO, "unlink file %s. blockid: %d, fileid: %" PRI64_PREFIX "u, action: %d, isserver: %s, peer ip: %s, cost time: %" PRI64_PREFIX "d",
          TFS_SUCCESS == ret ? "success" : "fail", block_id, file_id, action, is_master ? "master" : "slave",
          tbsys::CNetUtil::addrToString(peer_id).c_str(), TIMER_DURATION());

      return TFS_SUCCESS;
    }

    int DataService::new_block(NewBlockMessage* message)
    {
      const VUINT32* new_blocks = message->get_new_blocks();

      int ret = data_management_.batch_new_block(new_blocks);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "newblock error, ret: %d", ret);
      }

      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::remove_block(RemoveBlockMessage* message)
    {
      const VUINT32* remove_blocks = message->get_remove_blocks();
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(DEBUG, "remove block. peer id: %s", tbsys::CNetUtil::addrToString(peer_id).c_str());

      int ret = data_management_.batch_remove_block(remove_blocks);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "removeblock error, ret: %d", ret);
      }

      if (remove_blocks->size() == 1U)
      {
        RemoveBlockResponseMessage* msg = new RemoveBlockResponseMessage();
        msg->set_block_id(*remove_blocks->begin());
        message->reply_message(msg); 
      }
      else
      {
        message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      }
      return TFS_SUCCESS;
    }

    int DataService::replicate_block_cmd(ReplicateBlockMessage* message)
    {
      if (message->get_command() != PLAN_STATUS_BEGIN)
      {
        return TFS_ERROR;
      }

      ReplBlock* b = new ReplBlock();
      memcpy(b, message->get_repl_block(), sizeof(ReplBlock));
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(
          INFO,
          "receive replicate command. blockid: %u, source_id: %s, destination_id: %s, server_count: %u, peer id: %s\n",
          b->block_id_, tbsys::CNetUtil::addrToString(b->source_id_).c_str(), tbsys::CNetUtil::addrToString(
              b->destination_id_).c_str(), b->server_count_, tbsys::CNetUtil::addrToString(peer_id).c_str());
      repl_block_->add_repl_task(b);

      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::compact_block_cmd(CompactBlockMessage* message)
    {
      CompactBlkInfo* cblk = new CompactBlkInfo();
      cblk->block_id_ = message->get_block_id();
      cblk->owner_ = message->get_owner();
      cblk->preserve_time_ = message->get_preserve_time();
      uint64_t peer_id = message->get_connection()->getPeerId();

      int ret = compact_block_->add_cpt_task(cblk);

      TBSYS_LOG(INFO, "receive compact cmd. blockid: %u, owner: %d, preserve_time: %d, haverb: %d, peer_id: %s\n",
          cblk->block_id_, cblk->owner_, cblk->preserve_time_, ret, tbsys::CNetUtil::addrToString(peer_id).c_str());
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::query_bit_map(ListBitMapMessage* message)
    {
      int32_t list_type = message->get_bitmap_type();

      int32_t bit_map_len = 0, set_count = 0;
      char* tmp_data_buffer = NULL;

      data_management_.query_bit_map(list_type, &tmp_data_buffer, bit_map_len, set_count);

      RespListBitMapMessage* resp_lbm_msg = new RespListBitMapMessage();
      char* packet_data = resp_lbm_msg->alloc_data(bit_map_len);
      if (NULL == packet_data)
      {
        tbsys::gDeleteA(tmp_data_buffer);
        tbsys::gDelete(resp_lbm_msg);
        TBSYS_LOG(ERROR, "query bitmap. allocate memory fail. type: %d", list_type);
        return TFS_ERROR;
      }

      TBSYS_LOG(DEBUG, "query bitmap. type: %d, bitmaplen: %u, setcount: %u", list_type, bit_map_len, set_count);
      memcpy(packet_data, tmp_data_buffer, bit_map_len);
      resp_lbm_msg->set_length(bit_map_len);
      resp_lbm_msg->set_use_count(set_count);
      message->reply_message(resp_lbm_msg);
      tbsys::gDeleteA(tmp_data_buffer);
      return TFS_SUCCESS;
    }

    int DataService::list_blocks(ListBlockMessage* message)
    {
      int32_t list_type = message->get_block_type();
      VUINT block_ids;
      map <uint32_t, vector<uint32_t> > logic_2_physic_blocks;
      map<uint32_t, BlockInfo*> block_2_info;

      data_management_.query_block_status(list_type, block_ids, logic_2_physic_blocks, block_2_info);

      RespListBlockMessage* resp_lb_msg = new RespListBlockMessage();
      resp_lb_msg->set_blocks(list_type, &block_ids);
      if (list_type & LB_PAIRS)
      {
        resp_lb_msg->set_pairs(list_type, &logic_2_physic_blocks);
      }
      if (list_type & LB_INFOS)
      {
        resp_lb_msg->set_infos(list_type, &block_2_info);
      }

      message->reply_message(resp_lb_msg);
      return TFS_SUCCESS;
    }

    int DataService::crc_error_cmd(CrcErrorMessage* message)
    {
      CrcCheckFile* check_file_item = new CrcCheckFile();
      check_file_item->block_id_ = message->get_block_id();
      check_file_item->file_id_ = message->get_file_id();
      check_file_item->crc_ = message->get_crc();
      check_file_item->flag_ = message->get_error_flag();
      check_file_item->fail_servers_ = *(message->get_fail_server());

      int ret = block_checker_.add_repair_task(check_file_item);
      TBSYS_LOG(
          INFO,
          "receive crc error cmd, blockid: %u, fileid: %" PRI64_PREFIX "u, crc: %u, flag: %d, failserver size: %d, ret: %d\n",
          check_file_item->block_id_, check_file_item->file_id_, check_file_item->crc_, check_file_item->flag_,
          check_file_item->fail_servers_.size(), ret);
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));

      return TFS_SUCCESS;
    }

    int DataService::get_block_info(GetBlockInfoMessage *message)
    {
      uint32_t block_id = message->get_block_id();
      BlockInfo* blk = NULL;
      int32_t visit_count = 0;

      int ret = data_management_.get_block_info(block_id, blk, visit_count);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "block is not exist, blockid: %u, ret: %d", block_id, ret);
      }

      UpdateBlockInfoMessage* resp_ubi_msg = new UpdateBlockInfoMessage();
      resp_ubi_msg->set_block(blk);
      //serverid has discarded
      resp_ubi_msg->set_server_id(0);
      resp_ubi_msg->set_repair(visit_count);

      message->reply_message(resp_ubi_msg);
      return TFS_SUCCESS;
    }

    int DataService::get_server_status(GetServerStatusMessage *message)
    {
      int32_t type = message->get_status_type();
      int32_t ret_row = message->get_return_row();

      if (GSS_MAX_VISIT_COUNT == type)
      {
        //get max visit count block
        vector<LogicBlock*> block_vecs;
        data_management_.get_visit_sorted_blockids(block_vecs);
        CarryBlockMessage* resp_cb_msg = new CarryBlockMessage();
        for (int32_t i = 0; i < ret_row && i < static_cast<int32_t>(block_vecs.size()); ++i)
        {
          resp_cb_msg->add_expire_id(block_vecs[i]->get_block_info()->block_id_);
          resp_cb_msg->add_new_id(block_vecs[i]->get_visit_count());
        }

        message->reply_message(resp_cb_msg);
        return TFS_SUCCESS;
      }
      else if (GSS_BLOCK_FILE_INFO == type)
      {
        uint32_t block_id = ret_row;
        //get block file list
        vector <FileInfo> fileinfos;
        int ret = data_management_.get_block_file_list(block_id, fileinfos);
        if (TFS_SUCCESS != ret)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "GSS_BLOCK_FILE_INFO fail, blockid: %u, ret: %d", block_id, ret);
        }

        BlockFileInfoMessage* resp_bfi_msg = new BlockFileInfoMessage();
        FILE_INFO_LIST* v = resp_bfi_msg->get_fileinfo_list();
        for (uint32_t i = 0; i < fileinfos.size(); ++i)
        {
          v->push_back(&(fileinfos[i]));
        }
        message->reply_message(resp_bfi_msg);
        return TFS_SUCCESS;
      }
      else if (GSS_BLOCK_RAW_META_INFO == type)
      {
        //get block inode info
        uint32_t block_id = ret_row;
        RawMetaVec meta_vec;
        int ret = data_management_.get_block_meta_info(block_id, meta_vec);
        if (TFS_SUCCESS != ret)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "GSS_BLOCK_RAW_META_INFO fail, blockid: %u, ret: %d", block_id, ret);
        }

        BlockRawMetaMessage* resp_brm_msg = new BlockRawMetaMessage();
        RawMetaVec* v = resp_brm_msg->get_raw_meta_list();
        v->assign(meta_vec.begin(), meta_vec.end());
        message->reply_message(resp_brm_msg);
        return TFS_SUCCESS;
      }
      else if (GSS_CLIENT_ACCESS_INFO == type)
      {
        //ret_row as ip
        //uint32_t ip = ret_row;
        TBSYS_LOG(DEBUG, "GSS_CLIENT_ACCESS_INFO: from row: %d, return row: %d", message->get_from_row(),
            message->get_return_row());

        AccessStatInfoMessage* result_message = new AccessStatInfoMessage();
        result_message->set_from_row(message->get_from_row());
        result_message->set_return_row(message->get_return_row());
        result_message->set(acs_.get_stat());

        message->reply_message(result_message);
        return TFS_SUCCESS;
      }

      return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), data_server_info_.id_,
          "get server status type unsupport: %d", type);
    }

    int32_t DataService::client_command(ClientCmdMessage* message)
    {
      StatusMessage* resp = new StatusMessage(STATUS_MESSAGE_ERROR, "unknown client cmd.");
      int32_t type = message->get_cmd();
      uint64_t from_server_id = message->get_value2();
      do
      {
        // load a lost block
        if (CLIENT_CMD_SET_PARAM == type)
        {
          uint32_t block_id = message->get_value3();
          uint64_t server_id = message->get_value1();
          TBSYS_LOG(DEBUG, "set run param block_id: %u, server: %" PRI64_PREFIX "u, from: %s", block_id, server_id,
              tbsys::CNetUtil::addrToString(from_server_id).c_str());
          // block_id as param type,
          int32_t ret = 0;
          if (AccessControl::ACL_FLAG == block_id)
          {
            // server_id as flag value;
            ret = acl_.set_flag(server_id & 0xFF);
          }
          else if (AccessControl::ACL_IPMASK == block_id)
          {
            // server_id as ip; version as mask
            ret = acl_.insert_ipmask(getip(server_id), message->get_version());
          }
          else if (AccessControl::ACL_IPLIST == block_id)
          {
            ret = acl_.insert_iplist(getip(server_id));
          }
          else if (AccessControl::ACL_CLEAR == block_id)
          {
            ret = acl_.clear();
          }
          else if (AccessControl::ACL_RELOAD == block_id )
          {
            ret = acl_.reload();
          }
          if (ret < 0)
            ret = STATUS_MESSAGE_ERROR;
          else
            ret = STATUS_MESSAGE_OK;
          resp->set_message(ret, "cannot set acl, flag not 0");
        }
      }
      while (0);
      message->reply_message(resp);
      return TFS_SUCCESS;
    }

    int DataService::reload_config(ReloadConfigMessage* message)
    {
      int ret = TFS_SUCCESS;
      if ((ret = SysParam::instance().load_data_server(CONFIG.get_config_file_name(), server_index_)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "reload config sysparam load failed: %s\n", CONFIG.get_config_file_name().c_str());
        return ret;
      }
      if (message->get_switch_cluster_flag() && sync_mirror_)
      {
        ret = sync_mirror_->reload_slave_ip();
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "reload slave ip error. ret: %d\n", ret);
          return ret;
        }
      }

      TBSYS_LOG(INFO, "reload config ret: %d\n", ret);
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::get_ping_status(StatusMessage* message)
    {
      int ret = TFS_SUCCESS;
      if (STATUS_MESSAGE_PING == message->get_status())
      {
        StatusMessage *statusmessage = new StatusMessage(STATUS_MESSAGE_PING);
        message->reply_message(statusmessage);
      }
      else
      {
        ret = TFS_ERROR;
      }
      return ret;
    }

    int DataService::reset_block_version(ResetBlockVersionMessage* message)
    {
      uint32_t block_id = message->get_block_id();
      int ret = data_management_.reset_block_version(block_id);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "reset block version fail, blockid: %u", block_id);
      }

      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::write_raw_data(WriteRawDataMessage* message)
    {
      uint32_t block_id = message->get_block_id();
      int32_t msg_len = message->get_length();
      int32_t data_offset = message->get_offset();
      int32_t new_flag = message->get_new_block();
      char* data_buffer = message->get_data();
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(DEBUG, "writeblockdatafile start, blockid: %u, len: %d, offset: %d, new flag: %d, peer id: %s",
          block_id, msg_len, data_offset, new_flag, tbsys::CNetUtil::addrToString(peer_id).c_str());

      int ret = 0;
      if (new_flag)
      {
        ret = data_management_.new_single_block(block_id);
        if (TFS_SUCCESS != ret)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "write data batch fail, blockid: %u, ret: %d", block_id, ret);
        }

        //add to m_clonedBlockMap
        repl_block_->add_cloned_block_map(block_id);
      }

      ret = data_management_.write_raw_data(block_id, data_offset, msg_len, data_buffer);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "write data batch fail, blockid: %u, ret: %d", block_id, ret);
      }

      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    int DataService::batch_write_info(WriteInfoBatchMessage* message)
    {
      uint32_t block_id = message->get_block_id();
      BlockInfo* blk = message->get_block_info();
      const RawMetaVec* raw_metas = message->get_raw_meta_list();
      uint64_t peer_id = message->get_connection()->getPeerId();

      TBSYS_LOG(DEBUG, "write block fileinfo start, blockid: %u, cluster flag: %d, meta size: %d, peer id: %s", block_id,
          message->get_cluster(), static_cast<int32_t>(raw_metas->size()), tbsys::CNetUtil::addrToString(peer_id).c_str());
      int ret = data_management_.batch_write_meta(block_id, blk, raw_metas);
      if (TFS_SUCCESS != ret)
      {
        return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
            "write block fileinfo fail, blockid: %u, ret: %d", block_id, ret);
      }

      //between cluster copy
      if (message->get_cluster())
      {
        ret = ds_requester_.req_update_block_info(block_id);
        if (TFS_SUCCESS != ret)
        {
          return MessageFactory::send_error_message(message, TBSYS_LOG_LEVEL(ERROR), ret, data_server_info_.id_,
              "write block FileInfo fail: update block, blockid: %u, ret: %d", block_id, ret);
        }
      }

      //clear m_clonedBlockMap
      repl_block_->del_cloned_block_map(block_id);

      TBSYS_LOG(DEBUG, "write block fileinfo successful, blockid: %u", block_id);
      message->reply_message(new StatusMessage(STATUS_MESSAGE_OK));
      return TFS_SUCCESS;
    }

    // send blockinfos to nameserver
    void DataService::send_blocks_to_ns(const int32_t who)
    {
      if (0 != who && 1 != who)
        return;

      // ns who is not set
      if (!set_flag_[who])
        return;

      SetDataserverMessage req_sds_msg;
      req_sds_msg.set_ds(&data_server_info_);
      if (need_send_blockinfo_[who])
      {
        req_sds_msg.set_has_block(HAS_BLOCK_FLAG_YES);

        list<LogicBlock*> logic_block_list;
        data_management_.get_all_logic_block(logic_block_list);
        for (list<LogicBlock*>::iterator lit = logic_block_list.begin(); lit != logic_block_list.end(); ++lit)
        {
          TBSYS_LOG(DEBUG, "send block to ns: %d, blockid: %u\n", who, (*lit)->get_logic_block_id());
          req_sds_msg.add_block((*lit)->get_block_info());
        }
      }

      Message *message = hb_client_[who]->call(&req_sds_msg);
      if (NULL != message)
      {
        if (RESP_HEART_MESSAGE == message->get_message_type())
        {
          RespHeartMessage* resp_hb_msg = dynamic_cast<RespHeartMessage*>(message);
          need_send_blockinfo_[who] = 0;
          if (resp_hb_msg->get_status() == HEART_NEED_SEND_BLOCK_INFO)
          {
            TBSYS_LOG(DEBUG, "nameserver %d ask for send block\n", who + 1);
            need_send_blockinfo_[who] = 1;
          }
          else if (resp_hb_msg->get_status() == HEART_EXP_BLOCK_ID)
          {
            TBSYS_LOG(INFO, "nameserver %d ask for expire block\n", who + 1);
            data_management_.add_new_expire_block(resp_hb_msg->get_expire_blocks(), NULL, resp_hb_msg->get_new_blocks());
          }

          int32_t old_sync_mirror_status = sync_mirror_status_;
          sync_mirror_status_ = resp_hb_msg->get_sync_mirror_status();

          if (old_sync_mirror_status != sync_mirror_status_)
          {
            //has modified
            if ((sync_mirror_status_ & 1))
            {
              TBSYS_LOG(ERROR, "sync pause.");
              sync_mirror_->set_pause(1);
            }
            if ((sync_mirror_status_ & 3) == 2)
            {
              TBSYS_LOG(ERROR, "sync start.");
              sync_mirror_->set_pause(0);
            }
            if ((sync_mirror_status_ & 4))
            {
              TBSYS_LOG(ERROR, "sync disable log.");
              sync_mirror_->disable_log();
            }
            if ((sync_mirror_status_ & 12) == 8)
            {
              TBSYS_LOG(ERROR, "sync reset log.");
              sync_mirror_->reset_log();
            }
            if ((sync_mirror_status_ & 16))
            {
              TBSYS_LOG(ERROR, "sync set slave ip.");
              sync_mirror_->reload_slave_ip();
            }
          }

        }
        tbsys::gDelete(message);
      }
      else
      {
        TBSYS_LOG(ERROR, "Message is NULL");
        hb_client_[who]->disconnect();
      }
    }

    void DataService::do_stat(const uint64_t peer_id,
        const int32_t visit_file_size, const int32_t real_len, const int32_t offset, const int32_t mode)
    {
      count_mutex_.lock();
      if (AccessStat::READ_BYTES == mode)
      {
        data_server_info_.total_tp_.read_byte_ += real_len;
        acs_.incr(peer_id, AccessStat::READ_BYTES, real_len);
        if (0 == offset)
        {
          data_server_info_.total_tp_.read_file_count_++;
          visit_stat_.stat_visit_count(visit_file_size);
          acs_.incr(peer_id, AccessStat::READ_COUNT, 1);
        }
      }
      else if (AccessStat::WRITE_BYTES == mode)
      {
        data_server_info_.total_tp_.write_byte_ += visit_file_size;
        data_server_info_.total_tp_.write_file_count_++;
        acs_.incr(peer_id, AccessStat::WRITE_BYTES, visit_file_size);
        acs_.incr(peer_id, AccessStat::WRITE_COUNT, 1);
      }
      else
      {
        data_server_info_.total_tp_.read_byte_ += real_len;
        if (0 == offset)
        {
          data_server_info_.total_tp_.read_file_count_++;
        }
      }
      count_mutex_.unlock();
      return;
    }

    void DataService::try_add_repair_task(const uint32_t block_id, const int ret)
    {
      TBSYS_LOG(INFO, "add repair task, blockid: %u, ret: %d", block_id, ret);
      if (ret == -EIO)
      {
        CrcCheckFile* check_file_item = new CrcCheckFile(block_id, CHECK_BLOCK_EIO);
        block_checker_.add_repair_task(check_file_item);
      }
      return;
    }

    int DataService::init_log_file(tbsys::CLogger& LOGGER, const std::string& log_file)
    {
      LOGGER.setLogLevel(CONFIG.get_string_value(CONFIG_PUBLIC, CONF_LOG_LEVEL));

      if (log_file.size() != 0 && access(log_file.c_str(), R_OK) == 0)
      {
        char old_log_file[256];
        sprintf(old_log_file, "%s.%s", log_file.c_str(), Func::time_to_str(time(NULL), 1).c_str());
        rename(log_file.c_str(), old_log_file);
      }
      LOGGER.setFileName(log_file.c_str(), true);
      LOGGER.setMaxFileSize(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_SIZE, 1024 * 1024 * 1024));
      LOGGER.setMaxFileIndex(CONFIG.get_int_value(CONFIG_PUBLIC, CONF_LOG_NUM, 30));
      return TFS_SUCCESS;
    }
  }
}
