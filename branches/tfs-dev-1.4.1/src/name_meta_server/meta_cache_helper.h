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
#ifndef TFS_NAMEMETASERVER_META_CACHE_HELPER_H_
#define TFS_NAMEMETASERVER_META_CACHE_HELPER_H_
#include "meta_cache_info.h"
namespace tfs
{
  namespace namemetaserver
  {
    class MetaCacheHelper
    {
      public:
        template<class T>
          static bool less_compare(const T& left, const T& right)
          {
            if (NULL == left.name_)
            {
              if (NULL == right.name_)
              {
                return false;
              }
              return true;
            }
            else
            {
              if (NULL == right.name_)
              {
                return false;
              }
              else
              {
                //the first char is the size of name
                int l_len = *((unsigned char*)left.name_);
                int r_len = *((unsigned char*)right.name_);
                int ret_comp = ::memcmp(left.name_ + 1, right.name_ + 1, std::min(l_len, r_len));
                if (0 == ret_comp)
                {
                  ret_comp = l_len - r_len;
                }
                return ret_comp < 0;
              }
            }
          }
        template<class T>
          static bool equal_compare(const T& left, const T& right)
          {
            if (NULL == left.name_)
            {
              if (NULL == right.name_)
              {
                return true;
              }
              return false;
            }
            else
            {
              if (NULL == right.name_)
              {
                return false;
              }
              else
              {
                //the first char is the size of name
                int l_len = *((unsigned char*)left.name_);
                int r_len = *((unsigned char*)right.name_);
                if (l_len != r_len)
                {
                  return false;
                }
                int ret_comp = ::memcmp(left.name_ + 1, right.name_ + 1, std::min(l_len, r_len));
                return ret_comp == 0;
              }
            }
          }

        static InfoArray<CacheDirMetaNode>* get_sub_dirs_array_info(const CacheDirMetaNode* p_dir_node);
        static InfoArray<CacheFileMetaNode>* get_sub_files_array_info(const CacheDirMetaNode* p_dir_node);

        static int find_dir(const CacheDirMetaNode* p_dir_node,
            const char* name, CacheDirMetaNode*& ret_node);
        static int find_file(const CacheDirMetaNode* p_dir_node,
            const char* name, CacheFileMetaNode*& ret_node);

        static int rm_dir(const CacheDirMetaNode* p_dir_node, const char* name);
        static int rm_file(const CacheDirMetaNode* p_dir_node, const char* name);

        static int insert_dir(const CacheDirMetaNode* p_dir_node, CacheDirMetaNode* node);
        static int insert_file(const CacheDirMetaNode* p_dir_node, CacheFileMetaNode* node);
      private:
        template<class T>
          static int find(InfoArray<T>* info_arrfy, const char* name, T**& ret_value);
    };
  }
}
#endif
