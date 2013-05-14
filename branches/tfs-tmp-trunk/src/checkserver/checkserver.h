/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_CHECKSERVER_CHECKSERVER_H
#define TFS_CHECKSERVER_CHECKSERVER_H

#include <iostream>
#include <string>
#include <vector>
#include "tbsys.h"
#include "TbThread.h"
#include "common/internal.h"
#include "common/base_service.h"
#include "message/message_factory.h"
#include "check_manager.h"


namespace tfs
{
  namespace checkserver
  {
    class CheckServer: public common::BaseService
    {
      public:
        CheckServer();
        virtual ~CheckServer();

        /** initialize application data*/
        virtual int initialize(int argc, char* argv[]);

        /** destroy application data*/
        virtual int destroy_service();

        /** create the packet streamer, this is used to create packet according to packet code */
        virtual tbnet::IPacketStreamer* create_packet_streamer()
        {
          return new common::BasePacketStreamer();
        }

        /** destroy the packet streamer*/
        virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer)
        {
          tbsys::gDelete(streamer);
        }

        /** create the packet streamer, this is used to create packet*/
        virtual common::BasePacketFactory* create_packet_factory()
        {
          return new message::MessageFactory();
        }

        /** destroy packet factory*/
        virtual void destroy_packet_factory(common::BasePacketFactory* factory)
        {
          tbsys::gDelete(factory);
        }

        /** handle single packet */
        virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

        /** handle packet*/
        virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

        void run_check()
        {
          check_manager_.run_check();
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(CheckServer);

      private:
        class RunCheckThreadHelper: public tbutil::Thread
        {
          public:
            explicit RunCheckThreadHelper(CheckServer& service):
              service_(service)
          {
            start();
          }
            virtual ~RunCheckThreadHelper(){}
            void run();
          private:
            DISALLOW_COPY_AND_ASSIGN(RunCheckThreadHelper);
            CheckServer& service_;
        };
        typedef tbutil::Handle<RunCheckThreadHelper> RunCheckThreadHelperPtr;

      private:
        CheckManager check_manager_;
        RunCheckThreadHelperPtr check_thread_;
    };
  }
}

#endif

