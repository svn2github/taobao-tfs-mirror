#include "common/client_manager.h" 
#include "common/new_client.h"
#include "message/http_message_factory.h"
#include "message/http_packet_message.h"
#include "common/http_packet_streamer.h"
namespace tfs
{
  namespace tools
  {

    class HttpAgent : public tbnet::IPacketHandler
    {
      public:
        explicit HttpAgent(const char* spec);
        virtual ~HttpAgent();

        int initialize();
        virtual int http_request(const char *method, const char* url, const char* header);
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);

      private:
        void destroy();
        DISALLOW_COPY_AND_ASSIGN(HttpAgent);

      private:
        char* spec_;
        uint64_t server_id_;  
        message::HttpMessageFactory factory_;
        common::HttpPacketStreamer streamer_;
        tbnet::ConnectionManager*   connmgr_;
        tbnet::Transport*   transport_;
    };

  }
}
