#ifndef TFS_MESSAGE_NEW_CLIENT_H_
#define TFS_MESSAGE_NEW_CLIENT_H_
#include <tbsys.h>
#include <tbnet.h>
#include <map>
#include <ext/hash_map>
#include <Monitor.h>
#include <Mutex.h>
#include "message.h"

namespace tfs
{
  namespace message 
  {
    struct WaitId
    {
      uint32_t seq_id_:24;
      uint8_t  send_id_;
    };
    class NewClientManager;
    class NewClient 
    {
      friend class NewClientManager;
      public:
        typedef std::map<uint8_t, std::pair<uint64_t, Message*> > RESPONSE_MSG_MAP;
        typedef RESPONSE_MSG_MAP::iterator RESPONSE_MSG_MAP_ITER;
        typedef std::pair<uint8_t, uint64_t> SEND_SIGN_PAIR;
        typedef int (*callback_func)(NewClient* client);
      public:
        explicit NewClient(const uint32_t& seq_id);
        virtual ~NewClient();
        bool wait(const int64_t timeout_in_ms = common::DEFAULT_NETWORK_CALL_TIMEOUT);
        int post_request(const uint64_t server, Message* packet, uint8_t& send_id);
        int async_post_request(const std::vector<uint64_t>& servers, Message* packet, callback_func func);
        inline RESPONSE_MSG_MAP* get_success_response() { return complete_ ? &success_response_ : NULL;}
        inline RESPONSE_MSG_MAP* get_fail_response() { return complete_ ? &fail_response_ : NULL;}

      private:
        NewClient();
        DISALLOW_COPY_AND_ASSIGN(NewClient);
        tbutil::Monitor<tbutil::Mutex> monitor_;
        RESPONSE_MSG_MAP success_response_;
        RESPONSE_MSG_MAP fail_response_;
        std::vector<SEND_SIGN_PAIR> send_id_sign_;
        callback_func callback_;
        const uint32_t seq_id_;
        uint8_t generate_send_id_;
        static const uint8_t MAX_SEND_ID = 0xFF - 1;
        bool complete_;// receive all response(data packet, timeout packet) complete or timeout
        bool post_packet_complete_;
      private:
        SEND_SIGN_PAIR* find_send_id(const WaitId& id);
        bool push_fail_response(const uint8_t send_id, const uint64_t server);
        bool push_success_response(const uint8_t send_id, const uint64_t server, Message* packet);
        bool handlePacket(const WaitId& id, tbnet::Packet* packet);

        uint8_t create_send_id(const uint64_t server);
        bool destroy_send_id(const WaitId& id);

        inline const uint32_t& get_seq_id() const { return seq_id_;}

        bool async_wait();
    };
  } /* message */
} /* tfs */

#endif /* TFS_NEW_CLINET_H_*/
