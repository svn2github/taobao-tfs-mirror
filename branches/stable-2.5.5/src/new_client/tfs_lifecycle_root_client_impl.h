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
#ifndef TFS_CLIENT_LIFECYCLE_CLIENT_IMPL_H_
#define TFS_CLIENT_LIFECYCLE_CLIENT_IMPL_H_

#include <stdio.h>
#include <tbsys.h>
#include "tfs_client_impl.h"
#include "common/expire_define.h"
#include "tfs_lifecycle_root_client_api.h"

namespace tfs
{
  namespace client
  {
    class LifeCycleClientImpl
    {
      public:
        LifeCycleClientImpl();
        ~LifeCycleClientImpl();

        int initialize(const char *lifecycle_rs_addr);
        int initialize(const uint64_t lifecycle_rs_addr);

        TfsRetType query_task(const uint64_t es_id,
                               std::vector<common::ServerExpireTask>* p_res_task);

        int do_query_task(const uint64_t es_id,
                           std::vector<common::ServerExpireTask>* p_res_task);


      private:
        DISALLOW_COPY_AND_ASSIGN(LifeCycleClientImpl);
        uint64_t rs_id_;
        common::BasePacketFactory* packet_factory_;
        common::BasePacketStreamer* packet_streamer_;
    };
  }
}

#endif
