/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: meta_server_service.h 49 2011-08-08 09:58:57Z nayan@taobao.com $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#ifndef TFS_LIFE_CYCLE_MANAGER_EXPIRE_SERVICE_H_
#define TFS_LIFE_CYCLE_MANAGER_EXPIRE_SERVICE_H_

#include "common/parameter.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "message/message_factory.h"

#include "clean_task_helper.h"
#include "expire_heart_manager.h"
namespace tfs
{
  namespace expireserver
  {
    class ExpireService : public common::BaseService
    {
    public:
      ExpireService();
      virtual ~ExpireService();

    public:
      // override
      virtual tbnet::IPacketStreamer* create_packet_streamer();
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer);
      virtual common::BasePacketFactory* create_packet_factory();
      virtual void destroy_packet_factory(common::BasePacketFactory* factory);
      virtual const char* get_log_file_path();
      virtual const char* get_pid_file_path();
      virtual bool handlePacketQueue(tbnet::Packet* packet, void* args);
      virtual int initialize(int argc, char* argv[]);
      virtual int destroy_service();

      //clean task
      int clean_task(message::ReqCleanTaskFromRtsMessage* req_clean_task_msg);

    private:
      DISALLOW_COPY_AND_ASSIGN(ExpireService);

      uint64_t expireroot_ipport_id_;
      uint64_t local_ipport_id_;
      int64_t server_start_time_;

      CleanTaskHelper clean_task_helper_;
      common::KvEngineHelper* kv_engine_helper_;
      ExpireHeartManager heart_manager_;
    };
  }
}

#endif
