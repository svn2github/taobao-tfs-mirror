/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: base_service.h 185 2011-04-21 14:44:43Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_COMMON_BASE_SERVICE_H_
#define TFS_COMMON_BASE_SERVICE_H_

#define EASY_MULTIPLICITY
#include <tbnet.h>
#include <tbsys.h>
#include <Timer.h>
#include <Service.h>
#include "base_main.h"
#include "base_packet.h"
#include "base_packet_factory.h"
#include "base_packet_streamer.h"
#include "easy_helper.h"

namespace tfs
{
  namespace common
  {
    class NewClient;
    class BaseService :  public BaseMain,
                         public tbnet::IServerAdapter,
                         public tbnet::IPacketQueueHandler
    {
    public:
      BaseService();
      virtual ~BaseService();

      /** golbal aysnc callback function*/
      static int golbal_async_callback_func(NewClient* client, void* args);

      inline BasePacketFactory* get_packet_factory() { return packet_factory_;}

      /** get the packete streamer */
      inline BasePacketStreamer* get_packet_streamer() { return  streamer_;}

      /** get transport*/
      tbnet::Transport* get_transport() const { return transport_;}

      /** stop this service*/
      bool destroy();

      /** get timer*/
      inline tbutil::TimerPtr& get_timer() { return timer_;}

      /** handle single packet */
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

      /** handle packet*/
      virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);

      /** get listen port*/
      virtual int get_listen_port() const { return get_port();}

      /** initialize application data*/
      virtual int initialize(int, char* []) { return TFS_SUCCESS;}

      /** destroy application data*/
      virtual int destroy_service() {return TFS_SUCCESS;}

      /** create the packet streamer, this is used to create packet according to packet code */
      virtual tbnet::IPacketStreamer* create_packet_streamer() = 0;

      /** destroy the packet streamer*/
      virtual void destroy_packet_streamer(tbnet::IPacketStreamer* streamer) = 0;

      /** create the packet factory, this is used to create packet*/
      virtual BasePacketFactory* create_packet_factory() = 0;

      /** destroy packet factory*/
      virtual void destroy_packet_factory(BasePacketFactory* factory) = 0;

      /** async callback function*/
      virtual int async_callback(NewClient* client, void* args);

      /** push workitem to workers*/
      bool push(BasePacket* packet, bool block = false);

      /** get listen port*/
      int32_t get_port() const;

      /** get network device*/
      const char* const get_dev() const;

      /** get main work thread count*/
      int32_t get_work_thread_count() const;
      int32_t get_slow_work_thread_count() const;

      /** get work queue size */
      int32_t get_work_queue_size() const;

      /** get ip addr*/
      const char* get_ip_addr() const;

    private:
      /** application main entry*/
      virtual int run(int argc , char*argv[]);

      /** initialize tbnet*/
      int initialize_network(const char* app_name);

    // easy support
    public:
      virtual int handle(BasePacket* packet) = 0;
      virtual bool is_slow_packet(BasePacket* packet) { UNUSED(packet); return false; }

    private:
      static uint64_t get_packet_id_cb(easy_connection_t *c, void *packet)
      {
        BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
        return service->get_packet_streamer()->get_packet_id_handler(c, packet);
      }

      static void* decode_cb(easy_message_t *m)
      {
        BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
        return service->get_packet_streamer()->decode_handler(m);
      }

      static int encode_cb(easy_request_t *r, void *packet)
      {
        BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
        return service->get_packet_streamer()->encode_handler(r, packet);
      }

      static int process_cb(easy_request_t *r)
      {
        BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
        return service->process_handler(r);
      }

      static int slow_request_cb(easy_request_t *r, void *args)
      {
        BaseService* service = dynamic_cast<BaseService*>(BaseService::instance());
        return service->slow_request_handler(r, args);
      }

      int process_handler(easy_request_t *r);
      int slow_request_handler(easy_request_t *r, void* args);
      int packet_handler(BasePacket* packet);

    private:
      easy_io_t eio_;
      easy_io_handler_pt eio_handler_;
      int easy_io_initialize();

    private:
      BasePacketFactory* packet_factory_;
      BasePacketStreamer* streamer_;
      tbutil::TimerPtr timer_;
    protected:
      tbnet::Transport* transport_;
      tbnet::PacketQueueThread main_workers_;
      int32_t work_queue_size_;
      easy_thread_pool_t *slow_task_queue_;
    };
  }
}

#endif //TFS_COMMON_BASE_SERVICE_H_
