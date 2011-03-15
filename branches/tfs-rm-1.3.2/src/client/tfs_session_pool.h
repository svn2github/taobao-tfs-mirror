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
#ifndef TFS_CLIENT_TFSSESSION_POOL_H_
#define TFS_CLIENT_TFSSESSION_POOL_H_

#include <map>
#include <tbsys.h>
#include <Mutex.h>
#include "tfs_session.h"
namespace tfs
{
  namespace client
  {
    class TfsSessionPool
    {
      typedef std::map<std::string, TfsSession*> SESSION_MAP;
    public:
      TfsSessionPool();
      virtual ~TfsSessionPool();

      inline static TfsSessionPool& get_instance()
      {
        return gSessionPool;
      }

      TfsSession* get(const std::string& ns_ip_port, const int32_t cache_time, const int32_t cache_items);
      void release(TfsSession* session);

    private:
      DISALLOW_COPY_AND_ASSIGN( TfsSessionPool);
      tbutil::Mutex mutex_;
      SESSION_MAP pool_;
      static TfsSessionPool gSessionPool;
    };
  }
}

#endif /* TFSSESSION_POOL_H_ */
