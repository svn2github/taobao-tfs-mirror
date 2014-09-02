/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: http_packet_streamer.cpp 213 2011-04-22 16:22:51Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include "http_packet.h"
#include "http_packet_streamer.h"

namespace tfs
{
  namespace common
  {
    HttpPacketStreamer::HttpPacketStreamer()
    {
      _existPacketHeader = false;
    }

    HttpPacketStreamer::HttpPacketStreamer(tbnet::IPacketFactory *factory)
      :IPacketStreamer(factory)
    {
      _existPacketHeader = false;
    }

    bool HttpPacketStreamer::getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken)
    {
      bool bret = NULL != input && NULL != header;

      if (bret)
      {
        bret = input->getDataLen() > HTTP_PROTOCOL_LENGTH + HTTP_BLANK_LENGTH;
        if (bret)
        {
          int32_t nr = input->findBytes("\r\n\r\n", 4);
          bret = nr > 0;
          if (bret)
          {
            header->_pcode = common::HTTP_RESPONSE_MESSAGE;
            header->_chid = 0;
            header->_dataLen = input->getDataLen();
            *broken = false;
          }
          else
          {
            *broken = true;
            input->clear();
          }
        }
      }

      return bret;
    }

    bool HttpPacketStreamer::encode(tbnet::Packet* packet, tbnet::DataBuffer *output)
    {
      bool ret = (NULL != packet && NULL != output);
      if (ret)
      {
        int32_t length = output->getDataLen();
        ret = packet->encode(output);
        if (!ret)
          output->stripData(output->getDataLen() - length);
      }
      return ret;
    }

    tbnet::Packet* HttpPacketStreamer::decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      tbnet::Packet* packet = NULL;
      if (NULL != input && NULL != header)
      {
        packet = _factory->createPacket(header->_pcode);
        if (NULL != packet)
        {
          if (!packet->decode(input, header))
          {
            packet->free();
            packet = NULL;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "create packet: %d fail, drain data length: %d",
              header->_pcode, header->_dataLen);
          input->drainData(header->_dataLen);
        }
      }
      return packet;
    }
  }
}
