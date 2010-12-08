#include "client_manager.h"
#include "error_msg.h"
#include "interval.h"
#include <Memory.hpp>

namespace tfs
{
  namespace common
  {

    ClientManager::ClientManager()
      : inited_(false), connmgr_(NULL), waitmgr_(NULL)
    {
    }

    ClientManager::~ClientManager()
    {
      destroy();
    }

    void ClientManager::destroy()
    {
      tbsys::gDelete(connmgr_);
      tbsys::gDelete(waitmgr_);
    }

    int ClientManager::initialize(tbnet::Transport *transport, tbnet::IPacketStreamer* streamer)
    {
      int rc = TFS_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(ERROR, "ClientManager already initialized.");
        rc = TFS_ERROR;
      }

      if (TFS_SUCCESS == rc)
      {
        connmgr_ = new (std::nothrow) tbnet::ConnectionManager(transport, streamer, this);
        if (NULL == connmgr_)
        {
          TBSYS_LOG(ERROR, "cannot allocate ClientManager object.");
          rc = TFS_ERROR;
        }
        waitmgr_ = new (std::nothrow) common::WaitObjectManager();
        if (NULL == waitmgr_)
        {
          TBSYS_LOG(ERROR, "cannot allocate WaitObjectManager object.");
          rc = TFS_ERROR;
        }
      }

      inited_ = (TFS_SUCCESS == rc);
      if (!inited_)
      {
        destroy();
      }

      return rc;
    }

    tbnet::IPacketHandler::HPRetCode ClientManager::handlePacket(
        tbnet::Packet* packet, void* args)
    {
      if (NULL != args && NULL != packet && packet->isRegularPacket())
      {
        WaitId* id = reinterpret_cast<WaitId*>(args);
        waitmgr_->wakeup_wait_object(*id, packet);
      }
      else
      {
        // post_packet set args to NULL, means donot handle response.
        // there is no client waiting for this response packet, free it.
        if (NULL != packet)
        {
          if (packet->isRegularPacket())
          {
            TBSYS_LOG(INFO, "no client waiting this packet:code=%d", packet->getPCode());
            packet->free();
          }
          else if (NULL != args)
          {
            TBSYS_LOG(WARN, "packet (pcode=%d) is not regular packet, discard anyway. args:%ld",
                packet->getPCode(), reinterpret_cast<int64_t>(args));
          }
          else
          {
            TBSYS_LOG(DEBUG, "packet (pcode=%d) is not regular packet, discard anyway. "
                "args is NULL, maybe post channel timeout packet ", packet->getPCode());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "packet is NULL, unknown error. args:%ld", reinterpret_cast<int64_t>(args));
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    int ClientManager::get_wait_id(int64_t& wait_id) const
    {
      int rc = TFS_SUCCESS;
      common::WaitObject* wait_object = waitmgr_->create_wait_object();
      if (NULL == wait_object)
      {
        TBSYS_LOG(ERROR, "cannot send packet, cannot create wait object");
        rc = TFS_ERROR;
      }
      else
      {
        //set wait_id
        wait_id = wait_object->get_id();
      }
      return rc;
    }
    /**
     * post_packet is async version of send_packet. donot wait for response packet.
     */
    int ClientManager::post_request(const int64_t server_id, tbnet::Packet* packet, const int64_t wait_id) const
    {
      int rc = TFS_SUCCESS;
      if (NULL == packet || wait_id <= 0)
      {
        rc = EXIT_INVALID_ARGU;
      }
      else if (!inited_)
      {
        rc = EXIT_NOT_INIT;
        TBSYS_LOG(ERROR, "cannot post packet, ClientManager not initialized.");
        packet->free();
      }
      else
      {
        common::WaitObject* wait_object = waitmgr_->get_wait_object(wait_id);
        if (NULL == wait_object)
        {
          TBSYS_LOG(ERROR, "cannot send packet, cannot get wait object");
          rc = TFS_ERROR;
        }

        wait_object->add_send_id();

        if (TFS_SUCCESS == rc)
        {
          bool send_ok = connmgr_->sendPacket(server_id, packet, NULL, reinterpret_cast<void*>(wait_object->get_wait_key()));
          if (!send_ok)
          {
            rc = EXIT_SENDMSG_ERROR;
            TBSYS_LOG(INFO, "cannot post packet, maybe send queue is full or disconnect.");
            packet->free();
          }
        }
      }

      return rc;
    }

    int ClientManager::get_response(const int64_t wait_id, const int64_t wait_count,
        const int64_t wait_timeout, std::map<int64_t, tbnet::Packet*>& packets)
    {
      int rc = TFS_SUCCESS;
      common::WaitObject* wait_object = NULL;
      if (0 == wait_id || wait_count < 0)
      {
        rc = EXIT_INVALID_ARGU;
      }
      else
      {
        wait_object = waitmgr_->get_wait_object(wait_id);
        if (NULL == wait_object)
        {
          TBSYS_LOG(ERROR, "cannot send packet, cannot create wait object");
          rc = TFS_ERROR;
        }
      }

      packets.clear();
      if (TFS_SUCCESS == rc)
      {
        wait_object->wait(wait_count, wait_timeout);
        packets = wait_object->get_response();
      }

      return rc;
    }
  }
}
