/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *      qixiao.zs(qixiao.zs@alibaba-inc.com)
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_LIFECYCLE_HELPER_H_
#define TFS_CLIENT_LIFECYCLE_HELPER_H_

#include <vector>
#include <set>
#include "common/expire_define.h"

namespace tfs
{
  namespace client
  {
    class LifeCycleHelper
    {
    public:

      static int do_query_task(const uint64_t server_id, const uint64_t es_id,
                               std::vector<common::ServerExpireTask>* p_res_task);

    };
  }
}

#endif
