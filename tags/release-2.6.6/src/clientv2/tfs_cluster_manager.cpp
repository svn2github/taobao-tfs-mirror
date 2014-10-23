/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.cpp 49 2010-11-16 09:58:57Z zongdai@taobao.com $
 *
 * Authors:
 *    mingyan<mingyan.zc@taobao.com>
 *      - initial release
 *
 */

#include <Memory.hpp>
#include "tfs_cluster_manager.h"

#include "tfs_client_impl_v2.h"
#include "fsname.h"

namespace tfs
{
  namespace clientv2
  {
    using namespace tfs::common;
    using namespace std;

    TfsClusterManager::TfsClusterManager()
    {
    }

    TfsClusterManager::~TfsClusterManager()
    {
      std::map<int32_t, ClusterGroupInfo*>::iterator uit = unlink_ns_.begin();
      for (; unlink_ns_.end() != uit; uit++)
      {
        tbsys::gDelete(uit->second);
      }
      unlink_ns_.clear();
    }

    void TfsClusterManager::clear()
    {
      tbsys::CThreadGuard mutex_guard(&mutex_);
      for (int8_t i = 0; i < CHOICE_CLUSTER_NS_TYPE_LENGTH; ++i)
      {
        write_ns_[i].clear();
        read_ns_[i].clear();
      }
      map<int32_t, ClusterGroupInfo*>::iterator uit = unlink_ns_.begin();
      for (; unlink_ns_.end() != uit; uit++)
      {
        tbsys::gDelete(uit->second);
      }
      unlink_ns_.clear();
    }

    void TfsClusterManager::dump()
    {
      tbsys::CThreadGuard mutex_guard(&mutex_);
      for (int i = 0; i < CHOICE_CLUSTER_NS_TYPE_LENGTH; i++)
      {
        TBSYS_LOG(INFO, "%d write_ns %s", i, write_ns_[i].c_str());
        TBSYS_LOG(INFO, "read_ns: ");
        ClusterNsType::const_iterator it = read_ns_[i].begin();
        for (; it != read_ns_[i].end(); it++)
        {
          TBSYS_LOG(INFO, "cluster_id :%d ns :%s", it->first, it->second.c_str());
        }
      }

      std::map<int32_t, ClusterGroupInfo*>::iterator git = unlink_ns_.begin();
      TBSYS_LOG(INFO, "unlink_ns: ");
      for (; unlink_ns_.end() != git; git++)
      {
        TBSYS_LOG(INFO, "cluster_id: %d", git->first);
        ClusterGroupInfo* cluster_group_info = git->second;
        std::vector<GroupInfo*>::iterator iter = cluster_group_info->group_info_list_.begin();
        for (; cluster_group_info->group_info_list_.end() != iter; iter++)
        {
          TBSYS_LOG(INFO, "group_seq: %d ns_addr :%s, is_master: %d",
              (*iter)->group_seq_, (*iter)->ns_addr_.c_str(), (*iter)->is_master_);
        }
      }
    }

    int32_t TfsClusterManager::get_cluster_id(const char* file_name)
    {
      FSName fs(file_name);
      return fs.get_cluster_id();
    }

    uint32_t TfsClusterManager::get_block_id(const char* file_name)
    {
      FSName fs(file_name);
      return fs.get_block_id();
    }

    int TfsClusterManager::add_ns_into_write_ns(const string& ip_str)
    {
      int32_t iret = !ip_str.empty() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        int8_t index = 0;
        for (; index < CHOICE_CLUSTER_NS_TYPE_LENGTH; index++)
        {
          if (write_ns_[index].empty())
          {
            break;
          }
        }
        if (index < CHOICE_CLUSTER_NS_TYPE_LENGTH)
        {
          write_ns_[index] = ip_str;
        }
      }
      return iret;
    }

    int TfsClusterManager::add_ns_into_read_ns(const string& ip_str, const int32_t cluster_id)
    {
      int32_t iret = !ip_str.empty() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        int8_t index = 0;
        for (; index < CHOICE_CLUSTER_NS_TYPE_LENGTH; index++)
        {
          if (read_ns_[index].empty())
          {
            break;
          }
          else
          {
            ClusterNsType::const_iterator it = read_ns_[index].find(cluster_id);
            if (it == read_ns_[index].end())
            {
              break;
            }
          }
        }

        if (index < CHOICE_CLUSTER_NS_TYPE_LENGTH)
        {
          read_ns_[index][cluster_id] = ip_str;
        }
      }
      return iret;
    }

    int TfsClusterManager::add_ns_into_unlink_ns(const string& ip_str, const int32_t cluster_id, bool is_master)
    {
      int32_t iret = !ip_str.empty() ? TFS_SUCCESS : TFS_ERROR;
      if (TFS_SUCCESS == iret)
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        map<int32_t, ClusterGroupInfo*>::iterator it = unlink_ns_.find(cluster_id);
        ClusterGroupInfo* cluster_group_info = NULL;
        if (unlink_ns_.end() == it)
        {
          cluster_group_info = new ClusterGroupInfo();
          unlink_ns_.insert(map<uint32_t, ClusterGroupInfo*>::value_type(cluster_id, cluster_group_info));
        }
        else
        {
          cluster_group_info = it->second;
        }
        cluster_group_info->insert_group_info(-1, ip_str, is_master);
        update_cluster_group_info(cluster_group_info);
      }
      return iret;
    }

    string TfsClusterManager::get_read_ns_addr(const char* file_name,
        const int index)
    {
      int32_t cluster_id = get_cluster_id(file_name);
      return get_read_ns_addr_ex(cluster_id, index);
    }

    string TfsClusterManager::get_unlink_ns_addr(const char* file_name,
        const int index)
    {
      int32_t cluster_id = get_cluster_id(file_name);
      int32_t block_id = get_block_id(file_name);
      return get_unlink_ns_addr_ex(cluster_id, block_id, index);
    }

    string TfsClusterManager::get_write_ns_addr(const int index)
    {
      string ns_addr;
      if (index >= CHOICE_CLUSTER_NS_TYPE_LENGTH)
      {
        TBSYS_LOG(DEBUG, "wrong index: %d", index);
      }
      else
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        ns_addr = write_ns_[index];
      }

      return ns_addr;
    }

    string TfsClusterManager::get_read_ns_addr_ex(int32_t cluster_id,
        const int index)
    {
      string ns_addr;
      if (index >= CHOICE_CLUSTER_NS_TYPE_LENGTH
          || -1 == cluster_id)
      {
        TBSYS_LOG(DEBUG, "wrong index or cluster id invalid, index: %d, cluster_id: %d",
            index, cluster_id);
      }
      else
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        if (cluster_id == 0)
        {
          if (!read_ns_[0].empty())
          {
            ns_addr = read_ns_[0].begin()->second;
          }
        }
        else
        {
          ClusterNsType::const_iterator it = read_ns_[index].find(cluster_id);
          if (it != read_ns_[index].end())
          {
            ns_addr = it->second;
          }
        }
      }
      return ns_addr;
    }

    string TfsClusterManager::get_unlink_ns_addr_ex(const int32_t cluster_id,
        const uint32_t block_id, const int index)
    {
      string ns_addr;
      if (index >= CHOICE_CLUSTER_NS_TYPE_LENGTH
          || -1 == cluster_id || 0 == block_id)
      {
        TBSYS_LOG(DEBUG, "wrong index or cluster id or block id invalid, index: %d, "
            "cluster_id: %d, block_id: %d",
            index, cluster_id, block_id);
      }
      else
      {
        tbsys::CThreadGuard mutex_guard(&mutex_);
        map<int32_t, ClusterGroupInfo*>::iterator it = unlink_ns_.find(cluster_id);
        if (it != unlink_ns_.end())
        {
          ClusterGroupInfo* cluster_group_info = it->second;
          bool b_success = cluster_group_info->get_ns_addr(block_id, ns_addr);
          if (!b_success)
          {
            b_success = update_cluster_group_info(cluster_group_info);
            if (b_success)
            {
              cluster_group_info->get_ns_addr(block_id, ns_addr);
            }
          }
        }
      }

      return ns_addr;
    }


    bool TfsClusterManager::update_cluster_group_info(ClusterGroupInfo* cluster_group_info)
    {
      bool bRet = false;
      std::vector<GroupInfo*> need_update_group_info_list;
      cluster_group_info->get_need_update_group_info_list(need_update_group_info_list);
      if (need_update_group_info_list.size() > 0)
      {
        std::vector<GroupInfo*>::iterator iter = need_update_group_info_list.begin();
        for (; need_update_group_info_list.end() != iter; iter++)
        {
          int cluster_group_count = TfsClientImplV2::Instance()->get_cluster_group_count((*iter)->ns_addr_.c_str());
          if (cluster_group_count > 0)
          {
            bRet = true;
            if (cluster_group_info->group_count_ > 0)
            {
              if (cluster_group_count != cluster_group_info->group_count_)
              {
                bRet = false;
                TBSYS_LOG(ERROR, "cluster group count conflict. %d <==> %d",
                    cluster_group_count, cluster_group_info->group_count_);
                break;
              }
            }
            else
            {
              cluster_group_info->group_count_ = cluster_group_count;
              TBSYS_LOG(INFO, "set cluster group count %d for ns: %s", cluster_group_count, (*iter)->ns_addr_.c_str());
            }
            if (1 == cluster_group_count)
            {
              cluster_group_info->insert_group_info(0, (*iter)->ns_addr_, (*iter)->is_master_);
              TBSYS_LOG(INFO, "set cluster group seq 0 for ns: %s, is_master: %d", (*iter)->ns_addr_.c_str(), (*iter)->is_master_);
            }
            else
            {
              int cluster_group_seq = TfsClientImplV2::Instance()->get_cluster_group_seq((*iter)->ns_addr_.c_str());
              if (cluster_group_seq >= 0)
              {
                cluster_group_info->insert_group_info(cluster_group_seq, (*iter)->ns_addr_, (*iter)->is_master_);
                TBSYS_LOG(INFO, "set cluster group seq %d for ns: %s, is_master: %d", cluster_group_seq, (*iter)->ns_addr_.c_str(), (*iter)->is_master_);
              }
              else
              {
                bRet = false;
                break;
              }
            }
          }
        }
      }
      return bRet;
    }
  }
}


