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
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_RCSERVER_RCSERVICE_H_
#define TFS_RCSERVER_RCSERVICE_H_

#include "common/base_service.h"

namespace tfs
{
  namespace rcserver
  {
    class IResourceManager;
    class SessionManager;
    class RcService : public common::BaseService
    {
      public:
        RcService();
        ~RcService();

      public:
        // override
        tbnet::IPacketStreamer* create_packet_streamer()
        {
          return streamer_;
        }

        // override
        common::BasePacketFactory* create_packet_factory()
        {
          return factory_;
        }

        // override
        const char* get_log_file_path();
        // override
        bool handlePacketQueue(tbnet::Packet* packet, void* args);

      private:
        // override
        int initialize(int argc, char* argv[]);
        // override
        int destroy_service();

        int req_login();
        int req_keep_alive();
        int req_logout();

        // reload config // or monitor the modify time of conf file?
        int req_reload_config();

        // reload resource from db
        int req_reload_resource();
      private:
        DISALLOW_COPY_AND_ASSIGN(RcService);

        IResourceManager* resource_manager_;
        SessionManager* session_manager_;

        common::BasePacketStreamer* streamer_;
        common::BasePacketFactory* factory_;
    };

  }
}
#endif //TFS_RCSERVER_RCSERVICE_H_
