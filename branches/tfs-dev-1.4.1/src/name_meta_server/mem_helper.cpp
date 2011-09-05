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
    MemNodeList::MemNodeList() : size_(0), capacity_(10)
    {
      p_list_ = (void**)::malloc(sizeof(void*) * capacity_);
      assert(NULL != p_list_);
    }

    MemNodeList::~MemNodeList()
    {
      free();
      ::free(p_list_);
    }

    void* MemNodeList::get()
    {
      void* p_ret = NULL;
      if (!empty())
      {
        p_ret = p_list_[size_-1];
        --size_;
      }
      return p_ret;
    }

    bool MemNodeList::put(void* p)
    {
      bool ret = true;
      if (p != NULL)
      {
        if (full())
        {
          if (capacity_ >= MAX_MEM_NODE_CAPACITY)
          {
            ret = false;
          }
          else
          {
            int32_t new_capacity = capacity_ << 1;
            void** new_p_list = (void**)::malloc(sizeof(void*) * new_capacity);
            memcpy(new_p_list, p_list_, capacity_);
            ::free(p_list_);
            capacity_ = new_capacity;
            p_list_ = new_p_list;
            p_list_[size_++] = p;
          }
        }
        else
        {
          p_list_[size_++] = p;
        }
      }
      return ret;
    }

    void MemNodeList::free()
    {
      for (int32_t i = 0; i < size_; i++)
      {
        ::free(p_list_[i]);
      }
      size_ = 0;
    }

    MemNodeList MemHelper::root_node_free_list_;
    MemNodeList MemHelper::dir_node_free_list_;
    MemNodeList MemHelper::file_node_free_list_;

    MemHelper::MemHelper()
    {
    }
    MemHelper::~MemHelper()
    {
      destroy();
    }

    void* MemHelper::malloc(const int64_t size, const int32_t type)
    {
      void* ret_p = NULL;
      switch (type)
      {
      case CACHE_ROOT_NODE:
        ret_p = root_node_free_list_.get();
        break;
      case CACHE_DIR_META_NODE:
        ret_p = dir_node_free_list_.get();
        break;
      case CACHE_FILE_META_NODE:
        ret_p = file_node_free_list_.get();
        break;
      default:
        break;
      }

      if (NULL == ret_p)
      {
        ret_p = ::malloc(size);
      }
      return ret_p;
    }
    void MemHelper::free(void* p, const int32_t type)
    {
      if (p != NULL)
      {
        bool ret = false;
        switch (type)
        {
        case CACHE_ROOT_NODE:
          reinterpret_cast<CacheRootNode*>(p)->reset();
          ret = root_node_free_list_.put(p);
          break;
        case CACHE_DIR_META_NODE:
          reinterpret_cast<CacheDirMetaNode*>(p)->reset();
          ret = dir_node_free_list_.put(p);
          break;
        case CACHE_FILE_META_NODE:
          reinterpret_cast<CacheFileMetaNode*>(p)->reset();
          ret = file_node_free_list_.put(p);
          break;
        default:
          break;
        }

        if (!ret)
        {
          ::free(p);
        }
      }
    }
    void MemHelper::destroy()
    {
      root_node_free_list_.free();
      dir_node_free_list_.free();
      file_node_free_list_.free();
    }
  }
}

