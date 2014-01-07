/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: http_packet_streamer.h 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_COMMON_HTTP_PACKET_STREAMER_H_
#define TFS_COMMON_HTTP_PACKET_STREAMER_H_

#include <tbsys.h>
#include <tbnet.h>

namespace tfs
{
  namespace common
  {
    class HttpPacketStreamer: public tbnet::IPacketStreamer
    {
      public:
        HttpPacketStreamer();
        explicit HttpPacketStreamer(tbnet::IPacketFactory* factory);
        virtual ~HttpPacketStreamer(){};

        virtual bool getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool *broken);
        virtual tbnet::Packet* decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header);
        virtual bool encode(tbnet::Packet *packet, tbnet::DataBuffer *output);
    };
  }
}

#endif
