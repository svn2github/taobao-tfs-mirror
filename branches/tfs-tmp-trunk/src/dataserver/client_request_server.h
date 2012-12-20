/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_DATASERVER_CLIENT_REQUEST_SERVER_H_
#define TFS_DATASERVER_CLIENT_REQUEST_SERVER_H_

#include <Timer.h>
#include <Mutex.h>
#include <string>
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/statistics.h"
#include "common/status_message.h"
#include "message/message_factory.h"
#include "common/client_manager.h"
#include "sync_base.h"
#include "visit_stat.h"
#include "cpu_metrics.h"
#include "lease_manager.h"
#include "data_manager.h"

namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class ClientRequestServer
    {
      public:
        ClientRequestServer() {}
        virtual ~ClientRequestServer() {}

        int initialize(const uint64_t ns_ip_port, const uint64_t ds_ip_port)
        {
          ns_ip_port_ = ns_ip_port;
          ds_ip_port_ = ds_ip_port;
          return common::TFS_SUCCESS;
        }

        /** main entrance, dispatch task */
        int handle(tbnet::Packet* packet);

        /** file service interface */
        int stat_file(message::StatFileMessageV2* message);
        int read_file(message::ReadFileMessageV2* message);
        int write_file(message::WriteFileMessageV2* message);
        int close_file(message::CloseFileMessageV2* message);
        int unlink_file(message::UnlinkFileMessageV2* messsage);
        int callback(common::NewClient* client);

        /** block service interface */
        int new_block(message::NewBlockMessageV2* message);
        int remove_block(message::RemoveBlockMessageV2* message);

        /** tool support interface */

      private:
        int create_file_id_(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id);
        int resolve_block_version_conflict_(uint64_t block_id, Lease& lease);
        int update_block_info_(const uint64_t block_id, const common::UnlinkFlag unlink_flag);
        int check_write_response_(tbnet::Packet* msg);
        int check_close_response_(tbnet::Packet* msg);
        int check_unlink_response(tbnet::Packet* msg);
        int write_file_callback_(message::WriteFileMessageV2* message, Lease* lease);
        int close_file_callback_(message::CloseFileMessageV2* message, Lease* lease);
        int unlink_file_callback_(message::UnlinkFileMessageV2* message, Lease* lease);

      private:
        LeaseManager lease_manager_;
        uint64_t ns_ip_port_;
        uint64_t ds_ip_port_;
    };

  }
}
#endif /* CLIENT_REQUEST_SERVER_H_ */
