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
    class CacheRootNode;
    class CacheDirMetaNode;
    class CacheFileMetaNode;

    enum
    {
      CACHE_NONE_NODE = 0,
      CACHE_ROOT_NODE = 1,
      CACHE_DIR_META_NODE = 2,
      CACHE_FILE_META_NODE = 3,
    };

    // use array not list
    class MemNodeList
    {
    public:
      MemNodeList();
      ~MemNodeList();

      inline bool empty() const
      {
        return (0 == size_);
      }
      inline bool full() const
      {
        return (capacity_ == size_);
      }
      inline int32_t get_size() const
      {
        return size_;
      }
      inline int32_t get_capacity() const
      {
        return capacity_;
      }

      void* get();
      bool put(void* p);
      void free();
    private:
      static const int32_t MAX_MEM_NODE_CAPACITY = 1 << 30;
      int32_t size_;
      int32_t capacity_;
      void** p_list_;
    };

    class MemHelper
    {
    public:
      MemHelper();
      ~MemHelper();

      static void* malloc(const int64_t size, const int32_t type = CACHE_NONE_NODE);
      static void free(void* p, const int32_t type = CACHE_NONE_NODE);
      static void destroy();
    private:
      static MemNodeList root_node_free_list_;
      static MemNodeList dir_node_free_list_;
      static MemNodeList file_node_free_list_;
    };
  }
}
#endif
