/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *      qixiao.zs(qixiao.zs@alibaba-inc.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_LIFECYCLE_CLIENT_API_H_
#define TFS_CLIENT_LIFECYCLE_CLIENT_API_H_

#include <vector>
#include <set>
#include "common/expire_define.h"

namespace tfs
{
  namespace client
  {
    typedef int TfsRetType;
    class LifeCycleClientImpl;
    class LifeCycleClient
    {
      public:
        LifeCycleClient();
        ~LifeCycleClient();

        int initialize(const char *rs_addr);
        int initialize(const uint64_t rs_addr);

        TfsRetType query_task(const uint64_t es_id,
            std::vector<common::ServerExpireTask>* p_res_task);

      private:
        DISALLOW_COPY_AND_ASSIGN(LifeCycleClient);
        LifeCycleClientImpl* impl_;
    };
  }
}

#endif
