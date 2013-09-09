/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrateervice.h 746 2013-09-02 09:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_MIGRATESERVER_MIGRATESERVER_H_
#define TFS_MIGRATESERVER_MIGRATESERVER_H_

#include <Timer.h>
#include <Mutex.h>
#include "common/internal.h"
#include "common/base_packet.h"
#include "common/base_service.h"
#include "common/status_message.h"
#include "message/message_factory.h"

#include "migrate_manager.h"

namespace tfs
{
  namespace migrateserver
  {
    class MigrateService: public common::BaseService
    {
      public:
      MigrateService();

      virtual ~MigrateService();

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

      bool check_response(common::NewClient* client);
      int callback(common::NewClient* client);

      protected:
      virtual const char* get_log_file_path() { return NULL;}
      virtual const char* get_pid_file_path() { return NULL;}

      private:
      //int get_migrateserver_information(common::BasePacket* packet);

      private:
      void rotate_(time_t& last_rotate_log_time, time_t now, time_t zonesec);
      void timeout_();

      private:
      class TimeoutThreadHelper: public tbutil::Thread
      {
        public:
          explicit TimeoutThreadHelper(MigrateService& service):
            service_(service)
          {
            start();
          }
          virtual ~TimeoutThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(TimeoutThreadHelper);
          MigrateService& service_;
      };
      typedef tbutil::Handle<TimeoutThreadHelper> TimeoutThreadHelperPtr;
     private:
      DISALLOW_COPY_AND_ASSIGN(MigrateService);
      TimeoutThreadHelperPtr  timeout_thread_;
      MigrateManager* manager_;
    };
  }/** end namespace migrateserver **/
}/** end namespace tfs **/
#endif //TFS_MIGRATESERVER_MIGRATESERVER_H_
