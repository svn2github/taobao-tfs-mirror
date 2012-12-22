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
        explicit ClientRequestServer(DataService& service);
        virtual ~ClientRequestServer();
        inline BlockManager& block_manager();
        inline DataManager& data_manager();

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
        int write_file_callback_(message::WriteFileMessageV2* message, const int32_t status);
        int close_file_callback_(message::CloseFileMessageV2* message, const int32_t status);
        int unlink_file_callback_(message::UnlinkFileMessageV2* message, const int32_t status);

      private:
        DataService& service_;
    };

  }
}
#endif /* CLIENT_REQUEST_SERVER_H_ */
