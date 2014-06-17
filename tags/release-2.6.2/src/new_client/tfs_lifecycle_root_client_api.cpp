/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *    qixiao.zs<qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */

#include "common/define.h"
#include "tfs_lifecycle_root_client_api.h"
#include "tfs_lifecycle_root_client_impl.h"

namespace tfs
{
  namespace client
  {
    using namespace std;

    LifeCycleClient::LifeCycleClient():impl_(NULL)
    {
      impl_ = new LifeCycleClientImpl();
    }

    LifeCycleClient::~LifeCycleClient()
    {
      delete impl_;
      impl_ = NULL;
    }

    int LifeCycleClient::initialize(const char *rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    int LifeCycleClient::initialize(const uint64_t rs_addr)
    {
      return impl_->initialize(rs_addr);
    }

    TfsRetType LifeCycleClient::query_task(const uint64_t es_id,
        std::vector<common::ServerExpireTask>* p_res_task)
    {
      return impl_->query_task(es_id, p_res_task);
    }

  }
}

