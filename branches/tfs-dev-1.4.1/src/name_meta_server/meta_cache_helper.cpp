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
#include "meta_cache_helper.h"

#include "common/define.h"
namespace tfs
{
  namespace namemetaserver
  {
    using namespace common;

    int MetaCacheHelper::find_dir(const CacheDirMetaNode* p_dir_node,
        const char* name, CacheDirMetaNode*& ret_node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == name)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret_node = NULL;
        CacheDirMetaNode** iterator;
        if (NULL != p_dir_node->child_dir_infos_)
        {
          ret = find((InfoArray<CacheDirMetaNode>*)p_dir_node->child_dir_infos_,
              name, iterator);
          if (NULL != iterator)
          {
            ret_node = *iterator;
          }
        }
      }
      return ret;
    }
    int MetaCacheHelper::find_file(const CacheDirMetaNode* p_dir_node,
        const char* name, CacheFileMetaNode*& ret_node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == name)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret_node = NULL;
        CacheFileMetaNode** iterator;
        if (NULL != p_dir_node->child_file_infos_)
        {
          ret = find((InfoArray<CacheFileMetaNode>*)p_dir_node->child_file_infos_,
              name, iterator);
          if (NULL != iterator)
          {
            ret_node = *iterator;
          }
        }
      }
      return ret;
    }
    template<class T>
      int MetaCacheHelper::find(InfoArray<T>* info_arrfy, const char* name, T**& ret_value)
      {
        int ret = TFS_SUCCESS;
        assert(NULL != info_arrfy);
        ret_value = info_arrfy->find(name);
        return ret;
      }
  }
}
