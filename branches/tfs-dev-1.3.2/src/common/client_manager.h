#ifndef TFS_COMMON_CLIENT_MANAGER_H_
#define TFS_COMMON_CLIENT_MANAGER_H_

#include <tbnet.h>
#include "wait_object.h"

namespace tfs
{
  namespace common
  {
    class WaitObjectManager;
    class ClientManager : public tbnet::IPacketHandler
    {
      public:
        ClientManager();
        ~ClientManager();

      public:
        int initialize(tbnet::Transport *transport, tbnet::IPacketStreamer* streamer);

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);
        int get_wait_id(int64_t& wait_id) const;
        int post_request(const int64_t server, tbnet::Packet* packet, const int64_t wait_id) const;
        int get_response(const int64_t wait_id, const int64_t wait_count,
              const int64_t wait_timeout, std::map<int64_t, tbnet::Packet*>& packets);

        int call(const int64_t server, tbnet::Packet* packet, const int64_t timeout, tbnet::Packet*& response) const;

      private:
        void destroy();

      private:
        int32_t inited_;
        tbnet::ConnectionManager* connmgr_;
        WaitObjectManager* waitmgr_;
    };

    static ClientManager global_client_manager;
  }
}
#endif
