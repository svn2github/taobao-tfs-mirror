/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#ifndef TFS_NAMEMETASERVER_MEM_HELPER_H_
#define TFS_NAMEMETASERVER_MEM_HELPER_H_
#include <stdint.h>

#include "common/define.h"
namespace tfs
{
  namespace namemetaserver
  {
    class MemHelper
    {
      public:
        enum
        {
          CACHE_ROOT_NODE = 1,
          CACHE_DIR_META_NODE = 2,
          CACHE_FILE_META_NODE = 3,
        };
        static void* malloc(const int64_t size, const int32_t type = 0);
        static void free(void* p, const int32_t type = 0);
    };
  }
}
#endif
