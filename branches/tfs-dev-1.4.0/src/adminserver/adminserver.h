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
 *   nayan<nayan@taobao.com>
 *      - modify 2009-03-27
 *
 */
#ifndef TFS_ADMINSERVER_ADMINSERVER_H_
#define TFS_ADMINSERVER_ADMINSERVER_H_

#include <vector>
#include <string>
#include "common/func.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "common/new_client.h"
#include "common/client_manager.h"
#include "message/message_factory.h"
#include "message/admin_cmd_message.h"

namespace tfs
{
  namespace adminserver
  {
    enum ServiceName
    {
      SERVICE_NONE = 0,
      SERVICE_NS,
      SERVICE_DS
    };

    struct MonitorParam
    {
      std::string index_;
      common::IpAddr adr_;
      std::string lock_file_;
      std::string script_;
      std::string description_;
      int32_t fkill_waittime_;
      int32_t active_;
    };

    typedef std::map<std::string, MonitorParam*> MSTR_PARA;
    typedef std::map<std::string, MonitorParam*>::iterator MSTR_PARA_ITER;
    typedef std::map<std::string, message::MonitorStatus*> MSTR_STAT;
    typedef std::map<std::string, message::MonitorStatus*>::iterator MSTR_STAT_ITER;

    class AdminServer : public tbnet::IServerAdapter, public tbnet::IPacketQueueHandler
    {
    public:
      AdminServer();
      AdminServer(const char* conf_file, ServiceName service_name, bool is_daemon, bool is_old);
      virtual ~AdminServer();

      void destruct();
      int start(bool run_now = true);
      void wait();
      int stop();

      int start_monitor();
      int stop_monitor();
      static void* do_monitor(void* args);
      int run_monitor();

      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
      bool handlePacketQueue(tbnet::Packet* packet, void* args);

      // command response
      int cmd_reply_status(message::AdminCmdMessage* message);
      int cmd_check(message::AdminCmdMessage* message);
      int cmd_start_monitor(message::AdminCmdMessage* message);
      int cmd_restart_monitor(message::AdminCmdMessage* message);
      int cmd_stop_monitor(message::AdminCmdMessage* message);
      int cmd_start_monitor_index(message::AdminCmdMessage* message);
      int cmd_restart_monitor_index(message::AdminCmdMessage* message);
      int cmd_stop_monitor_index(message::AdminCmdMessage* message);
      int cmd_exit(message::AdminCmdMessage* message);

    private:
      void add_index(std::string& index, bool add_conf = false);
      void clear_index(std::string& index, bool del_conf = true);
      void modify_conf(std::string& index, int32_t type);
      void set_ds_list(char* index_range, std::vector<std::string>& ds_index);
      int get_param(std::string& index);
      int ping(const uint64_t ip);
      int ping_nameserver(const int status);
      int kill_process(message::MonitorStatus* status, int32_t wait_time, bool clear = false);

    private:
      bool is_old_;
      bool is_daemon_;
      char conf_file_[common::MAX_PATH_LENGTH];

      // monitor stuff
      ServiceName service_name_;
      int32_t stop_;
      bool running_;
      int32_t check_interval_;
      int32_t check_count_;
      int32_t warn_dead_count_;

      MSTR_PARA monitor_param_;
      MSTR_STAT monitor_status_;

      // server stuff
      message::MessageFactory msg_factory_;
      common::BasePacketStreamer packet_streamer_;
      tbnet::Transport transport_;
      tbnet::PacketQueueThread task_queue_thread_;
    };

  }
}
#endif //TFS_ADMINSERVER_ADMINSERVER_H_
