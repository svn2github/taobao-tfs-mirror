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

#include "common/config.h"
#include "common/func.h"
#include "common/config_item.h"
#include "common/parameter.h"
#include "message/client.h"
#include "message/message_factory.h"
#include <vector>
#include <string>

namespace tfs
{
  namespace adminserver
  {
    class AdminServer;
    struct MonitorParam
    {
      AdminServer* server_;
      int32_t port_;
      char* lock_file_;
      char* script_;
      char* description_;
      int32_t isds_;
      int32_t fkill_waittime_;
    };

    struct MonitorStatus
    {
      common::IpAddr adr_;
      int32_t restarting_;
      int32_t failure_;
      int32_t retry_;
      int32_t pid_;
    };

    class AdminServer
    {
    public:
      AdminServer();
      virtual ~AdminServer();

      int main(const char* conf_file, const int32_t service_name, const int32_t is_daemon);
      void set_ds_list(char* index_range, std::vector<std::string>& ds_index);

      static void stop();
      static void signal_handler(int sig);
      static void* do_monitor(void* args);

    private:
      static int32_t stop_;
      int32_t check_interval_;
      int32_t check_count_;

    private:
      int run_monitor(std::vector<MonitorParam*>* vmp);
      int ping_nameserver(const int status);
      int ping(message::Client *client, const uint64_t ip);
      int ping(const uint64_t ip);
    };

  }
}
#endif //TFS_ADMINSERVER_ADMINSERVER_H_
