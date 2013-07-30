/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-07-12
 *
 */
#include <Memory.hpp>
#include "tfs_session_pool.h"

using namespace tfs::common;

namespace tfs
{
  namespace clientv2
  {
    TfsSessionPool::TfsSessionPool()
    {
    }

    TfsSessionPool::~TfsSessionPool()
    {
      tbutil::Mutex::Lock lock(mutex_);
      SESSION_MAP::iterator it = pool_.begin();
      for (; it != pool_.end(); it++)
      {
        it->second->destroy();
        tbsys::gDelete(it->second);
      }
      pool_.clear();
    }

    TfsSession* TfsSessionPool::get(const char* ns_addr, const int64_t cache_time, const int64_t cache_items)
    {
      tbutil::Mutex::Lock lock(mutex_);
      TfsSession* session = NULL;
      SESSION_MAP::iterator it = pool_.find(ns_addr);
      if (it != pool_.end())
      {
        session = it->second;
      }
      else
      {
        session = new TfsSession(std::string(ns_addr), cache_time, cache_items);
        int32_t ret = session->initialize();
        if (TFS_SUCCESS == ret)
        {
          pool_[ns_addr] = session;
        }
        else
        {
          TBSYS_LOG(ERROR, "initialize tfs session failed, ret: %d", ret);
          tbsys::gDelete(session);
        }
      }
      return session;
    }

    void TfsSessionPool::put(TfsSession* session)
    {
      tbutil::Mutex::Lock lock(mutex_);
      if (session)
      {
        pool_.erase(session->get_ns_addr());
        tbsys::gDelete(session);
      }
    }

  }
}
