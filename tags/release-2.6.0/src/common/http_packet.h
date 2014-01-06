
#ifndef TFS_COMMON_HTTP_PACKET_H_
#define TFS_COMMON_HTTP_PACKET_H_

#include "base_packet.h"

#define   HTTP_PROTOCOL       "HTTP/1.1"
#define   HTTP_PROTOCOL_LENGTH          8 
#define   HTTP_BLANK_LENGTH             1
#define   HTTP_RESPONSE_OK              200
#define   HTTP_RESPONSE_STATUS_LENGTH   3


namespace tfs
{
  namespace  common
  {
    class HttpPacket : public BasePacket
    {
      public:
        HttpPacket();
        virtual ~HttpPacket();
        bool encode(tbnet::DataBuffer* output);
        bool decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header );
    };
  }
}

#endif
