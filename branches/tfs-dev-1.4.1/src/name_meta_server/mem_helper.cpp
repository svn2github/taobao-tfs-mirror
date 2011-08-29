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
#include "mem_helper.h"

#include <stdlib.h>
#include "meta_cache_helper.h"
namespace tfs
{
  namespace namemetaserver
  {
    void* MemHelper::malloc(const int64_t size, const int32_t type)
    {
      switch (type)
      {
        //TODO get from free_list
        case CACHE_ROOT_NODE:
          break;
        case CACHE_DIR_META_NODE:
          break;
        case CACHE_FILE_META_NODE:
          break;
        default:
          break;
      }
      return ::malloc(size);
    }
    void MemHelper::free(void* p, const int32_t type)
    {
      switch (type)
      {
        //TODO insert into free_list
        case CACHE_ROOT_NODE:
          break;
        case CACHE_DIR_META_NODE:
          break;
        case CACHE_FILE_META_NODE:
          break;
        default:
          break;
      }
      return ::free(p);
    }
  }
}

