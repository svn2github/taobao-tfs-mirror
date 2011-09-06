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

    //MetaCacheHelper::ROOT_NODE_MAP MetaCacheHelper::root_node_map_;
    //CacheRootNode* MetaCacheHelper::lru_head_;
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
    InfoArray<CacheDirMetaNode>* MetaCacheHelper::add_sub_dirs_array_info(CacheDirMetaNode* p_dir_node)
    {
      InfoArray<CacheDirMetaNode>* ret = NULL;
      if (NULL != p_dir_node)
      {
        void* addr = malloc(sizeof(InfoArray<CacheDirMetaNode>));
        assert(NULL != addr);
        ret = new(addr)InfoArray<CacheDirMetaNode>();
        p_dir_node->child_dir_infos_ = ret;
      }
      return ret;
    }
    InfoArray<CacheFileMetaNode>* MetaCacheHelper::add_sub_file_array_info(CacheDirMetaNode* p_dir_node)
    {
      InfoArray<CacheFileMetaNode>* ret = NULL;
      if (NULL != p_dir_node)
      {
        void* addr = malloc(sizeof(InfoArray<CacheFileMetaNode>));
        assert(NULL != addr);
        ret = new(addr)InfoArray<CacheFileMetaNode>();
        p_dir_node->child_file_infos_ = ret;
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
    int MetaCacheHelper::rm_dir(const CacheDirMetaNode* p_dir_node, CacheDirMetaNode* dir_node)
    {
      int ret = TFS_ERROR;
      if (NULL != p_dir_node && NULL != dir_node && NULL != dir_node->name_)
      {
        InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(p_dir_node);
        if (NULL != child_dir_infos)
        {
          if(child_dir_infos->remove(dir_node->name_))
          {
            ret = TFS_SUCCESS;
          }
        }
      }
      return ret;
    }
    int MetaCacheHelper::rm_file(const CacheDirMetaNode* p_dir_node, CacheFileMetaNode* file_node)
    {
      int ret = TFS_ERROR;
      if (NULL != p_dir_node && NULL != file_node && NULL != file_node->name_)
      {
        InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(p_dir_node);
        if (NULL != child_file_infos)
        {
          if(child_file_infos->remove(file_node->name_))
          {
            ret = TFS_SUCCESS;
          }
        }
      }
      return ret;
    }
    int MetaCacheHelper::insert_dir(CacheDirMetaNode* p_dir_node, CacheDirMetaNode* node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == node)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "insert dir p node: %s, node: %s", p_dir_node->name_, node->name_);
        ret = TFS_ERROR;
        InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(p_dir_node);
        // add new child dir info array
        if (NULL == child_dir_infos)
        {
          child_dir_infos = add_sub_dirs_array_info(p_dir_node);
        }

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
    int MetaCacheHelper::insert_file(CacheDirMetaNode* p_dir_node, CacheFileMetaNode* node)
    {
      int ret = TFS_SUCCESS;
      if (NULL == p_dir_node || NULL == node)
      {
        TBSYS_LOG(ERROR,"parameters err");
        ret = TFS_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "insert file p node: %s, node: %s", p_dir_node->name_, node->name_);
        ret = TFS_ERROR;
        InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(p_dir_node);
        if (NULL == child_file_infos)
        {
          child_file_infos = add_sub_file_array_info(p_dir_node);
        }
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

    int MetaCacheHelper::free(CacheDirMetaNode* dir_meta_node)
    {
      int ret = TFS_SUCCESS;
      if (NULL != dir_meta_node)
      {
        if (NULL != dir_meta_node->name_)
        {
          ::free(dir_meta_node->name_);
          dir_meta_node->name_ = NULL;
        }
        if (NULL != dir_meta_node->child_dir_infos_)
        {
          InfoArray<CacheDirMetaNode>* child_dir_infos = get_sub_dirs_array_info(dir_meta_node);
          CacheDirMetaNode** begin = child_dir_infos->get_begin();
          for (int i = 0; i < child_dir_infos->get_size(); i++)
          {
            free(*(begin + i));
          }
          ((InfoArray<CacheDirMetaNode>*)(dir_meta_node->child_dir_infos_))->~InfoArray();
          ::free((InfoArray<CacheDirMetaNode>*)(dir_meta_node->child_dir_infos_));
        }
        if (NULL != dir_meta_node->child_dir_infos_)
        {
          InfoArray<CacheFileMetaNode>* child_file_infos = get_sub_files_array_info(dir_meta_node);
          CacheFileMetaNode** begin = child_file_infos->get_begin();
          for (int i = 0; i < child_file_infos->get_size(); i++)
          {
            free(*(begin + i));
          }
          ((InfoArray<CacheFileMetaNode>*)(dir_meta_node->child_file_infos_))->~InfoArray();
          ::free((InfoArray<CacheFileMetaNode>*)(dir_meta_node->child_file_infos_));
        }
        ::free(dir_meta_node);
      }
      return ret;
    }
    int MetaCacheHelper::free(CacheFileMetaNode* file_meta_node)
    {
      int ret = TFS_SUCCESS;
      if (NULL != file_meta_node)
      {
        if (NULL != file_meta_node->name_)
        {
          ::free(file_meta_node->name_);
          file_meta_node->name_ = NULL;
        }
        if (NULL != file_meta_node->meta_info_)
        {
          ::free(file_meta_node->meta_info_);
        }
        ::free(file_meta_node);
      }
      return ret;
    }

    //CacheDirMetaNode* MetaCacheHelper::add_top_dir(const int64_t app_id, const int64_t uid)
    //{
    //  ROOT_NODE_MAP_ITER iter = root_node_map_.find(app_id);
    //  TBSYS_LOG(DEBUG, "add top dir %"PRI64_PREFIX"d, %"PRI64_PREFIX"d", app_id, uid);
    //  if (iter == root_node_map_.end())
    //  {
    //    iter = root_node_map_.insert(std::make_pair(app_id, __gnu_cxx::hash_map<int64_t, CacheRootNode*>())).first;
    //  }

    //  __gnu_cxx::hash_map<int64_t, CacheRootNode*>::const_iterator inner_iter = iter->second.find(uid);
    //  if (inner_iter == iter->second.end())
    //  {
    //    inner_iter =
    //      iter->second.insert(std::make_pair(uid, (CacheRootNode*)MemHelper::malloc(sizeof(CacheRootNode), CACHE_ROOT_NODE))).first;
    //  }

    //  if (NULL == inner_iter->second->dir_meta_)
    //  {
    //    inner_iter->second->dir_meta_ = reinterpret_cast<CacheDirMetaNode*>(MemHelper::malloc(sizeof(CacheDirMetaNode), CACHE_DIR_META_NODE));
    //  }
    //  return inner_iter->second->dir_meta_;
    //}

    //CacheDirMetaNode* MetaCacheHelper::get_top_dir(const int64_t app_id, const int64_t uid)
    //{
    //  CacheDirMetaNode* top_dir = NULL;
    //  CacheRootNode* root_node = get_root_node(app_id, uid);
    //  if (root_node != NULL)
    //  {
    //    TBSYS_LOG(DEBUG, "get top dir ");
    //    top_dir = root_node->dir_meta_;
    //    entrance(root_node);
    //  }
    //  return top_dir;
    //}

    //CacheRootNode* MetaCacheHelper::get_root_node(const int64_t app_id, const int64_t uid)
    //{
    //  CacheRootNode* root_node = NULL;
    //  ROOT_NODE_MAP_CONST_ITER iter = root_node_map_.find(app_id);

    //  if (iter != root_node_map_.end())
    //  {
    //    __gnu_cxx::hash_map<int64_t, CacheRootNode*>::const_iterator uid_iter = iter->second.find(uid);
    //    if (uid_iter != iter->second.end())
    //    {
    //      root_node = (CacheRootNode*)&(uid_iter->second);
    //    }
    //  }
    //  return root_node;
    //}

    //void MetaCacheHelper::entrance(CacheRootNode* root_node)
    //{
    //  // TODO: lock
    //  if (root_node->previous_ != root_node->next_)
    //  {
    //    root_node->previous_->next_ = root_node->next_;
    //    root_node->previous_ = lru_head_->previous_;
    //    root_node->next_ = lru_head_;
    //    lru_head_->previous_ = root_node;
    //    lru_head_ = root_node;
    //  }
    //  ++root_node->visit_count_;
    //}

  }
}
