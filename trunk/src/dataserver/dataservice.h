/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: dataservice.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2012-12-12
 *
 */
#ifndef TFS_DATASERVER_DATASERVICE_H_
#define TFS_DATASERVER_DATASERVICE_H_

#include <Timer.h>
#include <Mutex.h>
#include <string>
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/statistics.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "sync_base.h"
#include "data_management.h"
#include "requester.h"
#include "gc.h"
#include "client_request_server.h"
#include "op_manager.h"
#include "lease_managerv2.h"
#include "data_helper.h"
#include "task_manager.h"
#include "traffic_control.h"
#include "check_manager.h"
#include "writable_block_manager.h"


namespace tfs
{
  namespace dataserver
  {
    class DataService: public common::BaseService
    {

      friend int SyncBase::run_sync_mirror();

      public:
      DataService();

      virtual ~DataService();

      /** application parse args*/
      virtual int parse_common_line_args(int argc, char* argv[], std::string& errmsg);

      /** get listen port*/
      virtual int get_listen_port() const ;

      /** initialize application data*/
      virtual int initialize(int argc, char* argv[]);

      /** destroy application data*/
      virtual int destroy_service();

      /** create the packet streamer, this is used to create packet according to packet code */
      virtual tbnet::IPacketStreamer* create_packet_streamer()
      {
        return new common::BasePacketStreamer();
      }

      /** destroy the packet streamer*/
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
      {
        tbsys::gDelete(streamer);
      }

      /** create the packet streamer, this is used to create packet*/
      virtual common::BasePacketFactory* create_packet_factory()
      {
        return new message::MessageFactory();
      }

      /** destroy packet factory*/
      virtual void destroy_packet_factory(common::BasePacketFactory* factory)
      {
        tbsys::gDelete(factory);
      }

      /** handle single packet */
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      /** handle packet*/
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

      bool check_response(common::NewClient* client);
      int callback(common::NewClient* client);

      std::string get_real_work_dir();

      // common interfaces
      inline BlockManager& get_block_manager() { return *block_manager_;}
      inline OpManager& get_op_manager() { return op_manager_; }
      inline LeaseManager& get_lease_manager() { return *lease_manager_; }
      inline DataHelper&  get_data_helper() { return data_helper_;}
      inline TaskManager&  get_task_manager() { return task_manager_;}
      inline TrafficControl& get_traffic_control() { return traffic_control_;}
      inline std::vector<SyncBase*>& get_sync_mirror() { return sync_mirror_; }
      inline WritableBlockManager& get_writable_block_manager() { return writable_block_manager_; }

      protected:
      virtual const char* get_log_file_path();
      virtual const char* get_pid_file_path();

      private:
      int create_file_number(message::CreateFilenameMessage* message);
      int write_data(message::WriteDataMessage* message);
      int close_write_file(message::CloseFileMessage* message);

      //int write_raw_data(message::WriteRawDataMessage* message);
      //int batch_write_info(message::WriteInfoBatchMessage* message);

      int read_data(message::ReadDataMessage* message);
      int read_data_extra(message::ReadDataMessageV2* message, int32_t version);
      int read_raw_data(message::ReadRawDataMessage* message);
      int read_file_info(message::FileInfoMessage* message);

      //int rename_file(message::RenameFileMessage* message);
      int unlink_file(message::UnlinkFileMessage* message);

      //NS <-> DS
      int new_block(message::NewBlockMessage* message);
      int remove_block(message::RemoveBlockMessage* message);

      //get single blockinfo
      int get_block_info(message::GetBlockInfoMessageV2* message);

      //get blockinfos on this server
      int list_blocks(message::ListBlockMessage* message);

      int get_server_status(message::GetServerStatusMessage* message);
      int get_ping_status(common::StatusMessage* message);
      int client_command(message::ClientCmdMessage* message);

      int get_dataserver_information(common::BasePacket* packet);

      private:
      int initialize_nameserver_ip_addr_(std::vector<uint64_t>& ns_ip_port);
      int initialize_sync_mirror_();
      void timeout_();
      void run_task_();
      void run_check_();
      void rotate_(time_t& last_rotate_log_time, time_t now, time_t zonesec);

      private:
      class TimeoutThreadHelper: public tbutil::Thread
      {
        public:
          explicit TimeoutThreadHelper(DataService& service):
            service_(service)
          {
            start();
          }
          virtual ~TimeoutThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(TimeoutThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<TimeoutThreadHelper> TimeoutThreadHelperPtr;

      class RunTaskThreadHelper: public tbutil::Thread
      {
        public:
          explicit RunTaskThreadHelper(DataService& service):
            service_(service)
          {
            start();
          }
          virtual ~RunTaskThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(RunTaskThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<RunTaskThreadHelper> RunTaskThreadHelperPtr;

      class RunCheckThreadHelper: public tbutil::Thread
      {
        public:
          explicit RunCheckThreadHelper(DataService& service):
            service_(service)
          {
            start();
          }
          virtual ~RunCheckThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(RunCheckThreadHelper);
          DataService& service_;
      };
      typedef tbutil::Handle<RunCheckThreadHelper> RunCheckThreadHelperPtr;

     private:
      DISALLOW_COPY_AND_ASSIGN(DataService);

      std::string server_index_;
      Requester ds_requester_;
      OpManager op_manager_;
      LeaseManager *lease_manager_;
      DataHelper data_helper_;
      TaskManager task_manager_;
      BlockManager *block_manager_;
      DataManagement data_management_;
      TrafficControl traffic_control_;
      ClientRequestServer client_request_server_;
      WritableBlockManager writable_block_manager_;
      CheckManager check_manager_;
      std::vector<SyncBase*> sync_mirror_;
      TimeoutThreadHelperPtr  timeout_thread_;
      RunTaskThreadHelperPtr  task_thread_;
      RunCheckThreadHelperPtr check_thread_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif //TFS_DATASERVER_DATASERVICE_H_
