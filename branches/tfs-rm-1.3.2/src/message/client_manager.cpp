#include "client_manager.h"
#include "common/error_msg.h"
#include <Memory.hpp>

namespace tfs
{
  namespace message 
  {
    using namespace common;

    NewClientManager::NewClientManager()
      : inited_(false), own_transport_(false), connmgr_(NULL), waitmgr_(NULL)
    {
    }

    NewClientManager::~NewClientManager()
    {
      destroy();
    }

    void NewClientManager::destroy()
    {
      if (own_transport_ && transport_)
      {
        transport_->stop();
        transport_->wait();
        tbsys::gDelete(transport_);
      }

      tbsys::gDelete(connmgr_);
      tbsys::gDelete(waitmgr_);

      inited_ = false;
    }

    void NewClientManager::initialize()
    {
      if (!inited_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        if (!inited_)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = new tbnet::Transport();
          transport_->start();
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          waitmgr_ = new WaitObjectManager();
          own_transport_ = true;
          inited_ = true;
        }
      }
    }

    void NewClientManager::initialize_with_transport(tbnet::Transport* transport)
    {
      if (!inited_)
      {
        tbutil::Mutex::Lock lock(mutex_);
        if (!inited_)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = transport;
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          waitmgr_ = new WaitObjectManager();
          own_transport_ = false;
          inited_ = true;
        }
      }
    }

    tbnet::IPacketHandler::HPRetCode NewClientManager::handlePacket(
        tbnet::Packet* packet, void* args)
    {
      if (NULL != args && NULL != packet && packet->isRegularPacket())
      {
        WaitId id = *(reinterpret_cast<WaitId*>(&args));
        waitmgr_->wakeup_wait_object(id, packet);
      }
      else
      {
        // post_packet set args to NULL, means donot handle response.
        // there is no client waiting for this response packet, free it.
        if (NULL != packet)
        {
          if (packet->isRegularPacket())
          {
            TBSYS_LOG(INFO, "no client waiting this packet.code: %d", packet->getPCode());
            packet->free();
          }
          else if (NULL != args)
          {
            WaitId id = *(reinterpret_cast<WaitId*>(&args));
            TBSYS_LOG(WARN, "packet pcode: %d is not regular packet, command: %d, discard anyway. args: %x, waitid: %hu, sendid: %hu",
                packet->getPCode(), dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand(), args, id.seq_id_, id.send_id_);
          }
          else
          {
            TBSYS_LOG(DEBUG, "packet pcode: %d is not regular packet, command: %d, discard anyway. "
                "args is NULL, maybe post channel timeout packet ",
                packet->getPCode(), dynamic_cast<tbnet::ControlPacket*>(packet)->getCommand());
          }
        }
        else
        {
          TBSYS_LOG(WARN, "packet is NULL, unknown error. args: %x", args);
        }
      }
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }

    int NewClientManager::get_wait_id(uint16_t& wait_id) const
    {
      int rc = TFS_SUCCESS;
      WaitObject* wait_object = waitmgr_->create_wait_object();
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
    int NewClientManager::post_request(const int64_t server_id, Message* packet, const uint16_t wait_id, const uint16_t index_id)
    {
      initialize();
      int rc = TFS_SUCCESS;
      if (NULL == packet || wait_id <= 0)
      {
        rc = EXIT_INVALID_ARGU;
      }
      else
      {
        WaitObject* wait_object = waitmgr_->get_wait_object(wait_id);
        if (NULL == wait_object)
        {
          TBSYS_LOG(ERROR, "cannot send packet, cannot get wait object, wait id: %hu", wait_id);
          rc = TFS_ERROR;
        }

        wait_object->set_send_id(index_id);

        if (TFS_SUCCESS == rc)
        {
          Message* send_msg = factory_.clone_message(packet, 2, false);
          if (NULL == send_msg)
          {
            TBSYS_LOG(ERROR, "clone message failure, pcode:%d", packet->getPCode());
            rc = TFS_ERROR;
          }
          else
          {
            send_msg->set_auto_free(true);
            WaitId send_args = wait_object->get_wait_key();
            bool send_ok = connmgr_->sendPacket(server_id, send_msg,
               NULL, reinterpret_cast<void*>(*(reinterpret_cast<long*>(&send_args))));
            if (!send_ok)
            {
              rc = EXIT_SENDMSG_ERROR;
              TBSYS_LOG(INFO, "cannot post packet, maybe send queue is full or disconnect.");
              tbsys::gDelete(send_msg);
            }
          }
        }
      }

      return rc;
    }

    int NewClientManager::get_response(const uint16_t wait_id, const int64_t wait_count,
        const int64_t wait_timeout, std::map<uint16_t, Message*>& packets)
    {
      int rc = TFS_SUCCESS;
      WaitObject* wait_object = NULL;
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
      
      // destory wait_obj
      if (0 != wait_id)
      {
        waitmgr_->destroy_wait_object(wait_id);
      }

      return rc;
    }

    int NewClientManager::call(const int64_t server, Message* packet, 
        const int64_t timeout, Message* &response)
    {
      initialize();
      response = NULL;
      int rc = TFS_SUCCESS;
      if (NULL == packet) 
      {
        rc = EXIT_INVALID_ARGU;
      }

      WaitObject* wait_object = NULL;
      if (TFS_SUCCESS == rc)
      {
        wait_object = waitmgr_->create_wait_object();
        if (NULL == wait_object)
        {
          TBSYS_LOG(ERROR, "cannot send packet, cannot create wait object");
          rc = TFS_ERROR;
        }
      }

      if (TFS_SUCCESS == rc) 
      {
        // caution! wait_object set no free, it means response packet
        // not be free by wait_object, must be handled by user who call send_packet.
        //wait_object->set_no_free();
        wait_object->add_send_id();

        Message* send_msg = factory_.clone_message(packet, 2, false);
        if (NULL == send_msg)
        {
          TBSYS_LOG(ERROR, "clone message failure, pcode:%d", packet->getPCode());
          rc = TFS_ERROR;
        }
        else
        {
          send_msg->set_auto_free(true);
          WaitId send_args = wait_object->get_wait_key();
          bool send_ok = connmgr_->sendPacket(server, send_msg,
              NULL, reinterpret_cast<void*>(*(reinterpret_cast<long*>(&send_args))));
          if (send_ok)
          {
            send_ok = wait_object->wait(timeout);
            if (!send_ok)
            {
              TBSYS_LOG(ERROR, "wait response timeout, wait id: %hu, timeout: %d",
                  wait_object->get_id(), timeout);
              rc = EXIT_TIMEOUT_ERROR;
            }
            else
            {
              response = wait_object->get_single_response();
              rc = (NULL != response) ? TFS_SUCCESS : TFS_ERROR;
            }
          }
          else
          {
            rc = EXIT_SENDMSG_ERROR;
            TBSYS_LOG(ERROR, "cannot send packet, maybe send queue is full or disconnect.");
            tbsys::gDelete(send_msg);
          }
        }

        // do not free the response packet.
        waitmgr_->destroy_wait_object(wait_object);
        wait_object = NULL;
      } 
      return rc;
    }
  }
}
