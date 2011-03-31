/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "client_pool.h"

using namespace tfs::common;

namespace tfs
{
  namespace message
  {
    ClientManager ClientManager::gClientManager;

    ClientManager::ClientManager() :
      transport_(NULL), connmgr_(NULL), max_timeout_(3000), init_(false), own_transport_(false)
    {
    }

    ClientManager::~ClientManager()
    {
      if (connmgr_)
      {
        delete connmgr_;
      }
      connmgr_ = NULL;

      if (own_transport_ && transport_)
      {
        transport_->stop();
        transport_->wait();
        delete transport_;
        transport_ = NULL;
      }

      CLIENTS_MAP::iterator it;
      for (it = clients_.begin(); it != clients_.end(); ++it)
      {
        if (it->second)
        {
          delete it->second;
        }
      }
      clients_.clear();
      init_ = false;
    }

    // if server already has a transport, set to ClientManager directly.
    void ClientManager::init_with_transport(tbnet::Transport* transport)
    {
      if (init_ == false)
      {
        mutex_.lock();
        if (init_ == false)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = transport;
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          own_transport_ = false;
          init_ = true;
        }
        mutex_.unlock();
      }
    }

    void ClientManager::init()
    {
      if (init_ == false)
      {
        mutex_.lock();
        if (init_ == false)
        {
          streamer_.set_packet_factory(&factory_);
          transport_ = new tbnet::Transport();
          connmgr_ = new tbnet::ConnectionManager(transport_, &streamer_, this);
          transport_->start();
          own_transport_ = true;
          init_ = true;
        }
        mutex_.unlock();
      }
    }

    void ClientManager::store_client(Callee* callee)
    {
      mutex_.lock();
      clients_[callee->get_call_id()] = callee;
      mutex_.unlock();
    }

    AsyncClient* ClientManager::get_async_client(const VUINT64& ds, AsyncCallback* call_back)
    {
      init();
      AsyncClient* callee = new AsyncClient(ds, call_back);
      store_client(callee);
      return callee;
    }

    SimpleAsyncClient* ClientManager::get_simple_async_client(const uint64_t server_id, SimpleAsyncCallback* call_back)
    {
      init();
      VUINT64 ds_list;
      ds_list.push_back(server_id);
      SimpleAsyncClient* callee = new SimpleAsyncClient(ds_list, call_back);
      store_client(callee);
      return callee;
    }

    Client* ClientManager::get_client(const uint64_t server_id)
    {
      init();
      Client *callee = new Client(server_id);
      store_client(callee);
      return callee;
    }

    // AsyncClient no need to release
    void ClientManager::release_client(Callee* callee)
    {
      if (callee)
      {
        mutex_.lock();
        clients_.erase(callee->get_call_id());
        delete callee;
        mutex_.unlock();
      }
    }

    tbnet::IPacketHandler::HPRetCode ClientManager::handlePacket(tbnet::Packet *packet, void *args)
    {
      int id = static_cast<int> ((reinterpret_cast<long>(args)));
      int complete = Callee::CALL_UNFINISH;
      mutex_.lock();
      CLIENTS_MAP::iterator it = clients_.find(id);
      if (it != clients_.end())
      {
        if (it->second)
        {
          complete = it->second->handlePacket(packet, args);
        }
        if (complete == Callee::CALL_FINISHED)
        {
          delete it->second;
          it->second = NULL;
          clients_.erase(it);
        }
      }
      else if (packet->isRegularPacket())
      {
        delete packet;
      }
      mutex_.unlock();
      return tbnet::IPacketHandler::FREE_CHANNEL;
    }
  }
}
