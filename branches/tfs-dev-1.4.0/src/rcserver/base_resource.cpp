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
      return TFS_SUCCESS;
    }

    int BaseResource::load_rc_server()
    {
      //Todo
      return TFS_SUCCESS;
    }

    int BaseResource::load_cluster_rack()
    {
      //Todo
      return TFS_SUCCESS;
    }

    int BaseResource::load_app_group()
    {
      //Todo
      return TFS_SUCCESS;
    }

    int BaseResource::load_duplicate_server()
    {
      //Todo
      return TFS_SUCCESS;
    }
  }
}
