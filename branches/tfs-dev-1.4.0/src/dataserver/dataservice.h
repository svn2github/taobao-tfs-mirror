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
#ifndef TFS_DATASERVER_DATASERVICE_H_
#define TFS_DATASERVER_DATASERVICE_H_

#include "replicate_block.h"
#include "compact_block.h"
#include "sync_base.h"
#include "visit_stat.h"
#include "cpu_metrics.h"
#include "data_management.h"
#include "requester.h"
#include "block_checker.h"
#include "common/internal.h"
#include "common/config.h"
#include "common/statistics.h"
#include "message/message.h"
#include "message/message_factory.h"
#include <Timer.h>
#include <Mutex.h>
#include <string>

namespace tfs
{
  namespace dataserver
  {
#define WRITE_STAT_LOGGER write_stat_log_
#define WRITE_STAT_PRINT(level, ...) WRITE_STAT_LOGGER.logMessage(TBSYS_LOG_LEVEL(level), __VA_ARGS__)
#define WRITE_STAT_LOG(level, ...) (TBSYS_LOG_LEVEL_##level>WRITE_STAT_LOGGER._level) ? (void)0 : WRITE_STAT_PRINT(level, __VA_ARGS__)

#define READ_STAT_LOGGER read_stat_log_
#define READ_STAT_PRINT(level, ...) READ_STAT_LOGGER.logMessage(TBSYS_LOG_LEVEL(level), __VA_ARGS__)
#define READ_STAT_LOG(level, ...) (TBSYS_LOG_LEVEL_##level>READ_STAT_LOGGER._level) ? (void)0 : READ_STAT_PRINT(level, __VA_ARGS__)
    class DataService: public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler, public message::DefaultAsyncCallback
    {
      public:
        DataService();
        virtual ~DataService();

        int init(const std::string& server_index);
        int start(common::VINT* pids);
        int stop();
        int wait();

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection* connection, tbnet::Packet* packet);
        bool handlePacketQueue(tbnet::Packet* packet, void* args);

        int command_done(message::Message* send_message, bool status, const std::string& error);
        int send_message_to_slave_ds(message::Message* message, const common::VUINT64& ds_list);
        int post_message_to_server(message::Message* message, const common::VUINT64& ds_list);

        static void* do_heart(void* args);
        static void* do_check(void* args);
        int stop_heart();

      private:
        int run_heart();
        int run_check();

        int create_file_number(message::CreateFilenameMessage* message);
        int write_data(message::WriteDataMessage* message);
        int close_write_file(message::CloseFileMessage* message);

        int write_raw_data(message::WriteRawDataMessage* message);
        int batch_write_info(message::WriteInfoBatchMessage* message);

        int read_data(message::ReadDataMessage* message);
        int read_data_extra(message::ReadDataMessageV2* message, int32_t version);
        int read_raw_data(message::ReadRawDataMessage* message);
        int read_file_info(message::FileInfoMessage* message);

        int rename_file(message::RenameFileMessage* message);
        int unlink_file(message::UnlinkFileMessage* message);

        //NS <-> DS
        int new_block(message::NewBlockMessage* message);
        int remove_block(message::RemoveBlockMessage* message);

        int replicate_block_cmd(message::ReplicateBlockMessage* message);
        int compact_block_cmd(message::CompactBlockMessage* message);
        int crc_error_cmd(message::CrcErrorMessage* message);

        //get single blockinfo
        int get_block_info(message::GetBlockInfoMessage* message);

        int query_bit_map(message::ListBitMapMessage* message);
        //get blockinfos on this server
        int list_blocks(message::ListBlockMessage* message);
        int reset_block_version(message::ResetBlockVersionMessage* message);

        int get_server_status(message::GetServerStatusMessage* message);
        int get_ping_status(message::StatusMessage* message);
        int client_command(message::ClientCmdMessage* message);

        int get_server_memory_info(message::ServerMetaInfoMessage* message);
        int reload_config(message::ReloadConfigMessage* message);
        void send_blocks_to_ns(const int32_t who);

      private:
        bool access_deny(message::Message* message);
        void do_stat(const uint64_t peer_id,
            const int32_t visit_file_size, const int32_t real_len, const int32_t offset, const int32_t mode);
        int set_ns_ip();
        void try_add_repair_task(const uint32_t block_id, const int ret);
        int init_log_file(tbsys::CLogger& LOGGER, const std::string& log_file);

      private:
        DISALLOW_COPY_AND_ASSIGN(DataService);

        common::DataServerStatInfo data_server_info_; //dataserver info
        std::string server_index_;
        DataManagement data_management_;
        Requester ds_requester_;
        BlockChecker block_checker_;

        int32_t server_local_port_;
        int32_t stop_;
        int32_t need_send_blockinfo_[2];
        bool set_flag_[2];
        uint64_t hb_ip_port_[2];
        uint64_t ns_ip_port_; //nameserver ip port;

        ReplicateBlock* repl_block_; //replicate
        CompactBlock* compact_block_; //compact
        SyncBase* sync_mirror_; //mirror
        int32_t sync_mirror_status_;

        message::Client* hb_client_[2]; //heartbeat socket
        message::Client* client_; //update
        message::Client* compact_client_;

        tbutil::Mutex stop_mutex_;
        tbutil::Mutex client_mutex_;
        tbutil::Mutex compact_mutext_;
        tbutil::Mutex count_mutex_;
        tbutil::Mutex read_stat_mutex_;

        common::VINT* thread_pids_;

        AccessControl acl_;
        AccessStat acs_;
        VisitStat visit_stat_;
        CpuMetrics cpu_metrics_;
        int32_t max_cpu_usage_;

        //task queue
        tbnet::PacketQueueThread task_queue_thread_;
        tbnet::PacketQueueThread ds_task_queue_thread_;

        //write and read log
        tbsys::CLogger write_stat_log_;
        tbsys::CLogger read_stat_log_;
        std::vector<std::pair<uint32_t, uint64_t> > read_stat_buffer_;
        static const unsigned READ_STAT_LOG_BUFFER_LEN = 100;

        //global stat
        tbutil::TimerPtr timer_;
        common::StatManager<std::string, std::string, common::StatEntry > stat_mgr_;
        std::string tfs_ds_stat_;
    };
  }
}

#endif //TFS_DATASERVER_DATASERVICE_H_
