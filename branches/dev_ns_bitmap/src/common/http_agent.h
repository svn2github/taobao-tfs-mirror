/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: http_agent.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#include <tbsys.h>
#include <tbnet.h>

#include "http_packet_streamer.h"
#include "http_message_factory.h"
#include "base_http_message.h"

namespace tfs
{
  namespace common
  {
    class HttpAgent : public tbnet::IPacketHandler
    {
      public:
        explicit HttpAgent(const uint64_t server);
        virtual ~HttpAgent();

        void destroy();

        int send(const std::string& method, const std::string& host, const std::string& url, const std::string& header, const std::string& body);
      private:
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);

      private:
        DISALLOW_COPY_AND_ASSIGN(HttpAgent);
        uint64_t server_;
        tbnet::Transport   transport_;
        HttpMessageFactory factory_;
        HttpPacketStreamer streamer_;
        tbnet::ConnectionManager   connmgr_;
    };
  }/** end namespace commond **/
}/** end namesapce tfs **/
