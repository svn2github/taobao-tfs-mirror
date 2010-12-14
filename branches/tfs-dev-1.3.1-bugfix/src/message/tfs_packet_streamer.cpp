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
#include "tfs_packet_streamer.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    TfsPacketStreamer::TfsPacketStreamer()
    {
    }

    TfsPacketStreamer::TfsPacketStreamer(tbnet::IPacketFactory* factory) :
      IPacketStreamer(factory)
    {
    }

    TfsPacketStreamer::~TfsPacketStreamer()
    {
    }

    void TfsPacketStreamer::set_packet_factory(tbnet::IPacketFactory* factory)
    {
      _factory = factory;
    }

    bool TfsPacketStreamer::getPacketInfo(tbnet::DataBuffer* input, tbnet::PacketHeader* header, bool* broken)
    {
      if (input->getDataLen() < (static_cast<int32_t> (TFS_PACKET_HEADER_V0_SIZE)))
      {
        return false;
      }

      // first peek header size, check the flag , if flag == v1, need more data;
      TfsPacketHeaderV0 tfs_packet_header;
      memcpy(&tfs_packet_header, input->getData(), TFS_PACKET_HEADER_V0_SIZE);
      if ((tfs_packet_header.flag_ == TFS_PACKET_FLAG_V1) && ((input->getDataLen())
          < static_cast<int32_t> (TFS_PACKET_HEADER_V1_SIZE)))
      {
        return false;
      }

      // okay then consume TFS_PACKET_HEADER_V0_SIZE bytes from input buffer
      input->drainData(TFS_PACKET_HEADER_V0_SIZE);

      header->_dataLen = tfs_packet_header.length_;
      header->_pcode = tfs_packet_header.type_;
      header->_chid = 1;

      int64_t channel_id = 0;

      if (((tfs_packet_header.flag_ != TFS_PACKET_FLAG_V0) && (tfs_packet_header.flag_ != TFS_PACKET_FLAG_V1))
          || header->_dataLen < 0 || header->_dataLen > 0x4000000)
      { // 64M
        TBSYS_LOG(ERROR, "stream error: %x<>%x, dataLen: %d", tfs_packet_header.flag_, TFS_PACKET_FLAG_V1,
            header->_dataLen);
        *broken = true;
      }
      if (tfs_packet_header.flag_ == TFS_PACKET_FLAG_V1)
      {
        header->_pcode |= (tfs_packet_header.check_ << 16);
        header->_dataLen += TFS_PACKET_HEADER_DIFF_SIZE;

        memcpy(&channel_id, input->getData(), sizeof(int64_t));
        if (tfs_packet_header.check_ == 2)
        {
          header->_chid = (uint32_t)(channel_id & 0xFFFFFFFF);
        }
        else if (tfs_packet_header.check_ == 1)
        { // tbnet version client got origin version 1 packet
          header->_chid = (uint32_t)(channel_id & 0xFFFFFFFF) - 1;
        }
        if (header->_chid == 0)
        {
          TBSYS_LOG(ERROR, "get a packet _chid==0, pcode:%d,channel_id:%lld", header->_pcode, channel_id);
        }
      }

      return true;
    }

    tbnet::Packet* TfsPacketStreamer::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header)
    {
      assert(_factory != NULL);
      tbnet::Packet* packet = _factory->createPacket(header->_pcode);
      if (packet != NULL)
      {
        if (!packet->decode(input, header))
        {
          packet->free();
          packet = NULL;
        }
      }
      else
      {
        input->drainData(header->_dataLen);
      }
      return packet;
    }

    bool TfsPacketStreamer::encode(tbnet::Packet* packet, tbnet::DataBuffer* output)
    {
      Message* msg = (Message*) packet;
      tbnet::PacketHeader* header = msg->getPacketHeader();

      int old_len = output->getDataLen();
      int header_size = 0;

      TfsPacketHeaderV1 tfs_packet_header;
      tfs_packet_header.version_ = msg->get_version();
      tfs_packet_header.length_ = msg->get_raw_size();
      tfs_packet_header.type_ = header->_pcode;

      // v1
      if (tfs_packet_header.version_ == 1)
      {
        tfs_packet_header.flag_ = TFS_PACKET_FLAG_V1;
        tfs_packet_header.id_ = msg->get_id();
        tfs_packet_header.crc_ = msg->get_crc();
        header_size = TFS_PACKET_HEADER_V1_SIZE;
      }
      // v2, tbnet
      else if (tfs_packet_header.version_ == 2)
      {
        tfs_packet_header.flag_ = TFS_PACKET_FLAG_V1;
        tfs_packet_header.id_ = msg->getChannelId();
        tfs_packet_header.crc_ = msg->get_crc();
        header_size = TFS_PACKET_HEADER_V1_SIZE;
      }
      else
      { // v0
        tfs_packet_header.flag_ = TFS_PACKET_FLAG_V0;
        char* header = reinterpret_cast<char*> (&tfs_packet_header);
        int32_t i = 0;
        int32_t length = static_cast<int32_t> (TFS_PACKET_HEADER_V0_SIZE) - 2;
        for (i = 0; i < length; i++)
        {
          tfs_packet_header.version_ += static_cast<short> (*(header + i));
        }
        header_size = TFS_PACKET_HEADER_V0_SIZE;
      }

      output->writeBytes(&tfs_packet_header, header_size);

      if (msg->encode(output) == false)
      {
        TBSYS_LOG(ERROR, "encode error");
        output->stripData(output->getDataLen() - old_len);
        return false;
      }

      return true;
    }
  }
}
