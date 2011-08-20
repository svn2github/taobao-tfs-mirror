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
#ifndef TFS_COMMON_BASE_PACKET_STREAMER_H_
#define TFS_COMMON_BASE_PACKET_STREAMER_H_ 
#include <tbsys.h>
#include <tbnet.h>
namespace tfs
{
  namespace common 
  {
    class BasePacketStreamer: public tbnet::IPacketStreamer
    {
    public:
      BasePacketStreamer();
      explicit BasePacketStreamer(tbnet::IPacketFactory* factory);
      virtual ~BasePacketStreamer();
      void set_packet_factory(tbnet::IPacketFactory* factory);
    #ifdef TFS_GTEST
    public:
    #else
    protected:
    #endif
      virtual bool getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool* broken);
      virtual tbnet::Packet* decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header);
      virtual bool encode(tbnet::Packet* packet, tbnet::DataBuffer* output);
    };
  }
}
#endif
