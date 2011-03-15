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
 *
 */
#ifndef TFS_MESSAGE_PACKET_STREAMER_H_
#define TFS_MESSAGE_PACKET_STREAMER_H_

#include <tbsys.h>
#include <tbnet.h>
#include "message.h"

namespace tfs
{
  namespace message
  {
    class TfsPacketStreamer: public tbnet::IPacketStreamer
    {
      public:
        TfsPacketStreamer();
        TfsPacketStreamer(tbnet::IPacketFactory* factory);
        ~TfsPacketStreamer();
        void set_packet_factory(tbnet::IPacketFactory* factory);
        bool getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool* broken);
        tbnet::Packet* decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);
        bool encode(tbnet::Packet* packet, tbnet::DataBuffer* output);
    };
  }
}
#endif
