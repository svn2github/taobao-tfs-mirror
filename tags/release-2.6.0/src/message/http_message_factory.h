#ifndef TFS_COMMON_HTTP_PACKET_FACTORY_H_
#define TFS_COMMON_HTTP_PACKET_FACTORY_H_

#include "common/base_packet_factory.h"

namespace tfs
{
  using namespace common;
  namespace message 
  {
    class HttpMessageFactory : public common::BasePacketFactory
    {
      public:
        HttpMessageFactory(){};
        virtual ~HttpMessageFactory(){};
        virtual tbnet::Packet* createPacket(const int pcode);
    };
  }
}

#endif
