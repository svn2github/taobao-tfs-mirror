/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_
#define TFS_NAMEMETASERVER_NAMEMETASERVERSERVICE_H_

#include "common/base_service.h"
#include "message/message_factory.h"
#include "meta_store_manager.h"

namespace tfs
{
  namespace namemetaserver
  {
    class MetaServerService : public common::BaseService
    {
    public:
      MetaServerService();
      ~MetaServerService();

    public:
      // override
      tbnet::IPacketStreamer* create_packet_streamer();
      void destroy_packet_streamer(tbnet::IPacketStreamer* streamer);
      common::BasePacketFactory* create_packet_factory();
      void destroy_packet_factory(common::BasePacketFactory* factory);
      const char* get_log_file_path();
      const char* get_pid_file_path();
      bool handlePacketQueue(tbnet::Packet* packet, void* args);

    private:
      // override
      int initialize(int argc, char* argv[]);
      int destroy_service();

    private:
      DISALLOW_COPY_AND_ASSIGN(MetaServerService);

      MetaStoreManager* store_manager_;
      tbutil::Mutex mutex_;
    };
  }
}

#endif
