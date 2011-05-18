/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: message_factory.h 186 2011-04-22 13:47:20Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#include "base_packet_factory.h"
#include "local_packet.h"
#include "status_packet.h"
namespace tfs
{
  namespace common 
  {
    int BasePacketFactory::initialize()
    {
      packet_maps_[LOCAL_PACKET] = LocalPacket::create;
      packet_maps_[STATUS_PACKET] = StatusPacket::create;
      return TFS_SUCCESS;
    }

    void BasePacketFactory::destroy()
    {
      packet_maps_.clear();
    }

    tbnet::Packet* BasePacketFactory::createPacket(int pcode)
    {
      pcode = (pcode & 0xFFFF);
      BasePacket::CREATE_PACKET_MAP_ITER iter = packet_maps_.find(pcode);
      if (iter == packet_maps_.end())
      {
        TBSYS_LOG(ERROR, "create packet error, pcode: %d", pcode);
        return NULL;
      }
      return (iter->second)(pcode);
    }

    tbnet::Packet* BasePacketFactory::clone_packet(tbnet::Packet* packet, const int32_t version, const bool deserialize)
    {
      tbnet::Packet* clone_packet = NULL;
      if (NULL != packet)
      {
        clone_packet = createPacket(packet->getPCode());
        if (NULL != clone_packet)
        {
          BasePacket* bpacket = dynamic_cast<BasePacket*>(clone_packet);
          if (!bpacket->copy(dynamic_cast<BasePacket*>(packet), version, deserialize))
          {
            TBSYS_LOG(ERROR, "clone packet error, pcode: %d", packet->getPCode());
            bpacket->set_auto_free();
            bpacket->free();
            clone_packet = NULL;
          }
        }
      }
      return clone_packet;
    }
  }
}
