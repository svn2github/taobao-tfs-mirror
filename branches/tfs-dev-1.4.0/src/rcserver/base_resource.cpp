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
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "base_resource.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;

    int BaseResource::load()
    {
      //Todo
      //set base_last_update_time_
      return TFS_SUCCESS;
    }

    bool BaseResource::need_reload(const int64_t update_time_in_db) const
    {
      return update_time_in_db > base_last_update_time_;
    }

  }
}
