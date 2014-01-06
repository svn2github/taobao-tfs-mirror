
#ifndef TFS_COMMON_HTTP_PACKET_STREAMER_H_
#define TFS_COMMON_HTTP_PACKET_STREAMER_H_ 

#include "base_packet_streamer.h"

namespace tfs
{
  namespace common
  {
    class HttpPacketStreamer: public BasePacketStreamer
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
