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
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "tfs_session_pool.h"

using namespace tfs::client;
using namespace tfs::common;

TfsSessionPool TfsSessionPool::gSessionPool;

TfsSessionPool::TfsSessionPool()
{

}

TfsSessionPool::~TfsSessionPool()
{
  tbutil::Mutex::Lock lock(mutex_);
  SESSION_MAP::iterator it = pool_.begin();
  for (; it != pool_.end(); it++)
  {
    tbsys::gDelete(it->second);
  }
  pool_.clear();
}

TfsSession* TfsSessionPool::get(const std::string& ns_ip_port, const int32_t cache_time, const int32_t cache_items)
{
  tbutil::Mutex::Lock lock(mutex_);
  SESSION_MAP::iterator it = pool_.find(ns_ip_port);
  if (it != pool_.end())
    return it->second;
  TfsSession* session = new TfsSession(ns_ip_port, cache_time, cache_items);
  int32_t iret = session->initialize();
  if (iret != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "initialize tfs session failed");
    return NULL;
  }
  pool_[ns_ip_port] = session;
  return session;
}

void TfsSessionPool::release(TfsSession* session)
{
  tbutil::Mutex::Lock lock(mutex_);
  if (session != NULL)
  {
    pool_.erase(session->get_ns_ip_port_str());
    tbsys::gDelete(session);
  }
}
