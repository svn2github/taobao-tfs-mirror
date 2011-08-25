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
#include "meta_cache_info.h"

#include "meta_cache_helper.h"
namespace tfs
{
  namespace namemetaserver
  {
    bool CacheDirMetaNode::operator < (const CacheDirMetaNode& right) const
    {
      return MetaCacheHelper::less_compare(*this, right);
    }
    bool CacheDirMetaNode::operator == (const CacheDirMetaNode& right) const
    {
      return MetaCacheHelper::equal_compare(*this, right);
    }
    bool CacheDirMetaNode::operator != (const CacheDirMetaNode& right) const
    {
      return !(*this == right);
    }
    bool CacheFileMetaNode::operator < (const CacheFileMetaNode& right) const
    {
      return MetaCacheHelper::less_compare(*this, right);
    }
    bool CacheFileMetaNode::operator == (const CacheFileMetaNode& right) const
    {
      return MetaCacheHelper::equal_compare(*this, right);
    }
    bool CacheFileMetaNode::operator != (const CacheFileMetaNode& right) const
    {
      return !(*this == right);
    }
  }
}
