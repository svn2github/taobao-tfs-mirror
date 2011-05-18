/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_service.h 185 2011-04-21 14:44:43Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_SERVICE_H_
#define TFS_COMMON_BASE_SERVICE_H_

#include <tbnet.h>
#include <tbsys.h>
#include <Timer.h>
#include <Service.h>
#include "base_packet.h"
#include "base_packet_factory.h"
#include "base_packet_streamer.h"

namespace tfs
{
  namespace common
  {
    class NewClient;
    class BaseService :  public tbutil::Service ,
                         public tbnet::IServerAdapter,
                         public tbnet::IPacketQueueHandler 
    {
    public:
      BaseService();
      virtual ~BaseService();

      /** golbal aysnc callback function*/
      static int golbal_async_callback_func(NewClient* client, void* args);

      inline BasePacketFactory* get_packet_factory() { return packet_factory_;}

      /** get transport*/
      tbnet::Transport& get_transport();

      /** stop this service*/
      bool destroy();

      /** get timer*/
      inline tbutil::TimerPtr& get_timer() { return timer_;}
  protected:
      /** application parse args*/
      virtual int parse_common_line_args(int argc, char* argv[]) { return TFS_SUCCESS;}

      /** get listen port*/
      virtual int get_listen_port() { return get_port();}

      /** initialize application data*/
      virtual int initialize(int argc, char* argv[]) { return TFS_SUCCESS;}

      /** destroy application data*/
      virtual int destroy_service() {return TFS_SUCCESS;}

      /** create the packet streamer, this is used to create packet according to packet code */
      virtual tbnet::IPacketStreamer* create_packet_streamer() = 0;

      /** create the packet streamer, this is used to create packet*/
      virtual BasePacketFactory* create_packet_factory() = 0;

      /** get log file path*/
      virtual const char* get_log_file_path() = 0;

      /** async callback function*/
      virtual int async_callback(NewClient* client, void* args);

      /** push workitem to workers*/
      bool push(BasePacket* packet);

      /** handle single packet */
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      /** handle packet*/
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

    protected:
      /** get work directory*/
      const char* get_work_dir();

      /** get listen port*/
      int32_t get_port();

      /** get log file level*/
      const char* get_log_file_level();

      /** get log file size*/
      int64_t get_log_file_size();

      /** get log file count*/
      int32_t get_log_file_count();

      /** get network device*/
      const char* const get_dev();

      /** get main work thread count*/
      int32_t get_work_thread_count();

      /** get work queue size */
      int32_t get_work_queue_size();

      /** get ip addr*/
      const char* get_ip_addr();

    private:
      /** application main entry*/
      virtual int run(int argc , char*argv[], const std::string& config, std::string& error_msg);

      /** interrupt callback*/
      virtual int interruptCallback(int sig);

      /** load config file*/
      virtual int load_config(const std::string& config, std::string& error_msg){ return TFS_SUCCESS;}

      /** initialize work directory && log file*/ 
      int initialize_work_dir(const char* app_name, std::string& error_msg);

      /** initialize tbnet*/
      int initialize_network(const char* app_name, std::string& error_msg);

    private:
      BasePacketFactory* packet_factory_;
      BasePacketStreamer* streamer_;
      tbutil::TimerPtr timer_;
      tbnet::Transport transport_;
      tbnet::PacketQueueThread main_workers_;

      int32_t work_queue_size_;
    };
  }
}

#endif //TFS_COMMON_BASE_SERVICE_H_
