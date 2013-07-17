/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: kvrootserver.h  $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#ifndef TFS_EXPROOTSERVER_H_
#define TFS_EXPROOTSERVER_H_

#include <map>
#include <deque>
#include <vector>
#include "message/message_factory.h"
#include "common/internal.h"
#include "common/base_service.h"
#include "exp_server_manager.h"
#include "query_task_helper.h"

namespace tfs
{
  namespace exprootserver
  {
    class ExpRootServer: public common::BaseService
    {
    public:
      ExpRootServer();
      virtual ~ExpRootServer();
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

    private:
      DISALLOW_COPY_AND_ASSIGN(ExpRootServer);

    protected:
      /** get log file path*/
      virtual const char* get_log_file_path() { return NULL;}

      /** get pid file path */
      virtual const char* get_pid_file_path() { return NULL;}

    private:
      class KeepAliveIPacketQueueHandlerHelper : public tbnet::IPacketQueueHandler
      {
      public:
        explicit KeepAliveIPacketQueueHandlerHelper(ExpRootServer& manager): manager_(manager){ }
        virtual ~KeepAliveIPacketQueueHandlerHelper() {}
        virtual bool handlePacketQueue(tbnet::Packet* packet, void *args);
      private:
        DISALLOW_COPY_AND_ASSIGN(KeepAliveIPacketQueueHandlerHelper);
        ExpRootServer& manager_;
      };

    private :
      class AssignTaskThreadHelper : public tbutil::Thread
      {
        public:
          explicit AssignTaskThreadHelper(ExpRootServer &manager)
            :manager_(manager){start();}
          virtual ~AssignTaskThreadHelper(){}
          void run();
        private:
          ExpRootServer &manager_;
          DISALLOW_COPY_AND_ASSIGN(AssignTaskThreadHelper);
      };
      typedef tbutil::Handle<AssignTaskThreadHelper> AssignTaskThreadHelperPtr;

    private:
      int rt_es_keepalive(common::BasePacket* packet);
      int32_t handle_finish_task(common::BasePacket *packet);
      int32_t query_progress(common::BasePacket *packet);
      int assign(const uint64_t es_id, const common::ExpireDeleteTask &del_task);
      int assign_task(void);

    public:
      int handle_fail_servers(common::VUINT64 &down_servers);

    private:
      tbnet::PacketQueueThread rt_es_heartbeat_workers_;

      AssignTaskThreadHelperPtr assign_task_thread_;

      tbutil::Mutex mutex_task_; //lock for m_task_info_

      tbutil::Mutex mutex_task_wait_;   //lock for task_queue_

      std::map<uint64_t, common::ExpireDeleteTask> m_task_info_;
      typedef std::map<uint64_t, common::ExpireDeleteTask>::iterator TASK_INFO_ITER;

      std::deque<common::ExpireDeleteTask> task_wait_;

      KeepAliveIPacketQueueHandlerHelper rt_es_heartbeat_handler_;
      ExpServerManager manager_;

      QueryTaskHelper query_task_helper_;
      common::KvEngineHelper *kv_engine_helper_;
    };
  }/** exprootserver **/
}/** tfs **/
#endif
