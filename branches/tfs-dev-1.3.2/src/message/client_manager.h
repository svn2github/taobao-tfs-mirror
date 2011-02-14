#ifndef TFS_MESSAGE_CLIENT_MANAGER_H_
#define TFS_MESSAGE_CLIENT_MANAGER_H_

#include <tbnet.h>
#include <Mutex.h>
#include "message.h"
#include "message_factory.h"
#include "tfs_packet_streamer.h"
#include "wait_object.h"
#include "common/interval.h"

namespace tfs
{
  namespace message 
  {
    class WaitObjectManager;
    class NewClientManager : public tbnet::IPacketHandler
    {
      public:
        static NewClientManager* get_instance()
        {
          static NewClientManager client_manager;
          return &client_manager;
        }

      public:
        void initialize();
        void initialize_with_transport(tbnet::Transport* transport);

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);
        int get_wait_id(uint16_t& wait_id) const;
        int post_request(const int64_t server, Message* packet, const uint16_t wait_id);
        int get_response(const uint16_t wait_id, const int64_t wait_count,
              const int64_t wait_timeout, std::map<uint16_t, Message*>& packets);

        int call(const int64_t server, Message* packet, const int64_t timeout, Message*& response);

      private:
        NewClientManager();
        ~NewClientManager();
        DISALLOW_COPY_AND_ASSIGN(NewClientManager);

        void destroy();

      private:
        bool inited_;
        bool own_transport_;
        tbutil::Mutex mutex_;

        MessageFactory factory_;
        TfsPacketStreamer streamer_;
        tbnet::Transport* transport_;
        tbnet::ConnectionManager* connmgr_;

        WaitObjectManager* waitmgr_;
    };
  }
}
#endif
