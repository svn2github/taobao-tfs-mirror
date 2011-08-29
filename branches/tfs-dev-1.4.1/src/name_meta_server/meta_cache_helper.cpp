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
    InfoArray<CacheDirMetaNode>* MetaCacheHelper::get_sub_dirs_array_info(const CacheDirMetaNode* p_dir_node)
    {
      InfoArray<CacheDirMetaNode>* ret = NULL;
      if (NULL != p_dir_node)
      {
        ret = (InfoArray<CacheDirMetaNode>*)(p_dir_node->child_dir_infos_);
      }
      return ret;
    }
    InfoArray<CacheFileMetaNode>* MetaCacheHelper::get_sub_files_array_info(const CacheDirMetaNode* p_dir_node)
    {
      InfoArray<CacheFileMetaNode>* ret = NULL;
      if (NULL != p_dir_node)
      {
        ret = (InfoArray<CacheFileMetaNode>*)(p_dir_node->child_file_infos_);
      }
      return ret;
    }
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
        InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(p_dir_node);
        if (NULL != child_dir_infos)
        {
          ret = find(child_dir_infos, name, iterator);
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
        InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(p_dir_node);
        if (NULL != child_file_infos)
        {
          ret = find(child_file_infos, name, iterator);
          if (NULL != iterator)
          {
            ret_node = *iterator;
          }
        }
      }
      return ret;
    }
    int MetaCacheHelper::rm_dir(const CacheDirMetaNode* p_dir_node, const char* name)
    {
      int ret = TFS_ERROR;
      if (NULL != p_dir_node && NULL != name)
      {
        InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(p_dir_node);
        if (NULL != child_dir_infos)
        {
          child_dir_infos->remove(name);
          ret = TFS_SUCCESS;
        }
      }
      return ret;
    }
    int MetaCacheHelper::rm_file(const CacheDirMetaNode* p_dir_node, const char* name)
    {
      int ret = TFS_ERROR;
      if (NULL != p_dir_node && NULL != name)
      {
        InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(p_dir_node);
        if (NULL != child_file_infos)
        {
          child_file_infos->remove(name);
          ret = TFS_SUCCESS;
        }
      }
      return ret;
    }
    int MetaCacheHelper::insert_dir(const CacheDirMetaNode* p_dir_node, CacheDirMetaNode* node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == node)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = TFS_ERROR;
        InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(p_dir_node);
        if (NULL != child_dir_infos)
        {
          if(child_dir_infos->insert(node))
          {
            ret = TFS_SUCCESS;
          }
        }
      }
      return ret;
    }
    int MetaCacheHelper::insert_file(const CacheDirMetaNode* p_dir_node, CacheFileMetaNode* node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == node)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = TFS_ERROR;
        InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(p_dir_node);
        if (NULL != child_file_infos)
        {
          if(child_file_infos->insert(node))
          {
            ret = TFS_SUCCESS;
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
