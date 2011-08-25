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
namespace tfs
{
  namespace namemetaserver
  {
    void* MemHelper::malloc(const int64_t size)
    {
      return ::malloc(size);
    }
    void MemHelper::free(void* p)
    {
      return ::free(p);
    }
  }
}

