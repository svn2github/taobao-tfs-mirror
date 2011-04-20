#ifndef TFS_MESSAGE_CLIENT_MANAGER_H_
#define TFS_MESSAGE_CLIENT_MANAGER_H_

#include <tbnet.h>
#include <Mutex.h>
#include <ext/hash_map>
#include "message.h"
#include "message_factory.h"
#include "tfs_packet_streamer.h"
#include "new_client.h"
#include "common/define.h"

namespace tfs
{
  namespace message 
  {
    class NewClientManager : public tbnet::IPacketHandler
    {
        friend int NewClient::post_request(const uint64_t server, Message* packet, uint8_t& send_id);
        typedef __gnu_cxx::hash_map<uint32_t, NewClient*> NEWCLIENT_MAP;
        typedef NEWCLIENT_MAP::iterator NEWCLIENT_MAP_ITER;
      public:
        NewClientManager();
        virtual ~NewClientManager();
        static NewClientManager& get_instance()
        {
          static NewClientManager client_manager;
          return client_manager;
        }
        int initialize(tbnet::Transport* transport = NULL);
        void destroy();
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void* args);
        NewClient* create_client();
        bool destroy_client(NewClient* client);

        Message* clone_message(Message* message, int32_t version = 2, bool deserialize = false);

      private:
        bool handlePacket(const WaitId& id, tbnet::Packet* response);

      private:
        DISALLOW_COPY_AND_ASSIGN(NewClientManager);
        NEWCLIENT_MAP new_clients_;
        MessageFactory factory_;                                                                   
        TfsPacketStreamer streamer_;
        tbnet::Transport* transport_;
        tbnet::ConnectionManager* connmgr_; 

        tbutil::Mutex mutex_;
        static const uint32_t MAX_SEQ_ID = 0xFFFFFF - 1;
        uint32_t seq_id_;

        bool initialize_;
        bool own_transport_;
    };
    int test_server_alive(const uint64_t server_id, const int64_t timeout = common::DEFAULT_NETWORK_CALL_TIMEOUT/*ms*/);
    //int send_message_to_server(const uint64_t server_id, Message* msg, Message** response, const int64_t timeout = common::DEFAULT_NETWORK_CALL_TIMEOUT/*ms*/);
  }/* message */
}/* tfs */
#endif
