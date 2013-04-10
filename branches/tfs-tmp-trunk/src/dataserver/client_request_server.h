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
#include "lease_manager.h"
#include "data_manager.h"
#include "data_helper.h"

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
        inline BlockManager& get_block_manager();
        inline DataManager& get_data_manager();
        inline DataHelper& get_data_helper();
        inline TrafficControl& get_traffic_control();

        /** main entrance, dispatch task */
        int handle(tbnet::Packet* packet);

        /** main callback, used in write, close, unlink */
        int callback(common::NewClient* client);

      private:
        int report_block(message::CallDsReportBlockRequestMessage* message);

        /** file service interface */
        int stat_file(message::StatFileMessageV2* message);
        int read_file(message::ReadFileMessageV2* message);
        int write_file(message::WriteFileMessageV2* message);
        int close_file(message::CloseFileMessageV2* message);
        int unlink_file(message::UnlinkFileMessageV2* messsage);

        /** write, close, unlink callback */
        int write_file_callback(message::WriteFileMessageV2* message);
        int close_file_callback(message::CloseFileMessageV2* message);
        int prepare_unlink_file_callback(message::UnlinkFileMessageV2* message);
        int unlink_file_callback(message::UnlinkFileMessageV2* message);

        /** block service interface */
        int new_block(message::NewBlockMessageV2* message);
        int remove_block(message::RemoveBlockMessageV2* message);

        /** data & index interface */
        int read_raw_data(message::ReadRawdataMessageV2* message);
        int write_raw_data(message::WriteRawdataMessageV2* message);
        int read_index(message::ReadIndexMessageV2* message);
        int write_index(message::WriteIndexMessageV2* message);
        int query_ec_meta(message::QueryEcMetaMessage* message);
        int commit_ec_meta(message::CommitEcMetaMessage* message);

        /** tool support interface */
      private:
        DataService& service_;
    };
  }
}
#endif /* CLIENT_REQUEST_SERVER_H_ */
