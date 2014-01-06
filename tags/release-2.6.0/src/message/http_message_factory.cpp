
#include "http_packet_message.h"
#include "http_message_factory.h"
#include "common/base_packet.h"

namespace tfs
{
  namespace message 
  {
    tbnet::Packet* HttpMessageFactory::createPacket(const int pcode)
    {
      TBSYS_LOG(DEBUG, "HttpMessageFactory createPacket");
      switch (pcode) 
      {
        case HTTP_REQUEST_MESSAGE:
          return new (std::nothrow)HttpRequestMessage();
        case HTTP_RESPONSE_MESSAGE:
          return new (std::nothrow)HttpResponseMessage();
        default:
          TBSYS_LOG(WARN, "HttpMessageFactory wrong pcode =%d", pcode);
          return NULL;
      }
    }
  }
}
