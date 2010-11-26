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
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_DATASERVER_H_
#define TFS_DATASERVER_DATASERVER_H_

#include <tbsys.h>
#include <tbnet.h>
#include "message/tfs_packet_streamer.h"
#include "dataservice.h"

namespace tfs
{
  namespace dataserver
  {
    class DataServer 
    {
      public:
        DataServer();
        DataServer(std::string server_index);
        virtual ~DataServer();
        int start();
        void stop();
        std::string get_server_index();

      private:
        DISALLOW_COPY_AND_ASSIGN(DataServer);
        static const int32_t SPEC_LEN = 32;

        std::string server_index_;         // int index
        message::MessageFactory msg_factory_;
        message::TfsPacketStreamer packet_streamer_;
        tbnet::Transport tran_sport_;
        DataService data_service_;
    };
  }
}
#endif //TFS_DATASERVER_DATASERVER_H_
