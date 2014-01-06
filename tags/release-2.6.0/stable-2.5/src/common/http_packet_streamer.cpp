
#include "http_packet_streamer.h"
#include "http_packet.h"

namespace tfs
{
  namespace common
  {
    HttpPacketStreamer::HttpPacketStreamer()
    {
      setNoPacketHeader();
    }

    HttpPacketStreamer::HttpPacketStreamer(tbnet::IPacketFactory *factory)
      :BasePacketStreamer(factory)
    {
    }

    bool HttpPacketStreamer::getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken) 
    {
      bool bret = NULL != input && NULL != header;

      if (bret)
      {
        bret = input->getDataLen() > HTTP_PROTOCOL_LENGTH + HTTP_BLANK_LENGTH;
        if (bret)
        {
          int nr = input->findBytes("\r\n\r\n", 4);
          if (nr < 0) 
          {
            bret = false;
            *broken = true;
            input->clear();
            TBSYS_LOG(ERROR, "header deserialize error");
          }

          if (bret)
          {
            header->_pcode = common::HTTP_RESPONSE_MESSAGE;
            header->_chid = 0;
            header->_dataLen = input->getDataLen();
            *broken = false;
          }
        }
      }

      return bret;
    }

    bool HttpPacketStreamer::encode(tbnet::Packet* packet, tbnet::DataBuffer *output)
    {
      int oldLen = output->getDataLen();

      if (packet->encode(output) == false) {
        TBSYS_LOG(ERROR, "encode error");
        output->stripData(output->getDataLen() - oldLen);
        return false;
      }

      return true;
    }

    tbnet::Packet* HttpPacketStreamer::decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      tbnet::IPacketFactory* _factory = get_packet_factory();

      assert(_factory != NULL);
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
