/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_client_api.h 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *    mingyan<mingyan.zc@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLUSTER_MANAGER_H_
#define TFS_CLUSTER_MANAGER_H_

#include <stdio.h>
#include <map>
#include <tbsys.h>
#include <common/define.h>

namespace tfs
{
  namespace clientv2
  {
    class TfsClusterManager
    {
      struct GroupInfo
      {
        GroupInfo():group_seq_(-1), is_master_(false)
        {
        }
        GroupInfo(const int group_seq, const std::string& ns_addr, const bool is_master):
          group_seq_(group_seq), ns_addr_(ns_addr), is_master_(is_master)
        {
        }
        int group_seq_;
        std::string ns_addr_;
        bool is_master_;
      };
      struct ClusterGroupInfo
      {
        ClusterGroupInfo():group_count_(-1)
        {
        }
        ~ClusterGroupInfo()
        {
          std::vector<GroupInfo*>::iterator iter = group_info_list_.begin();
          for (; group_info_list_.end() != iter; iter++)
          {
            delete (*iter);
            //tbsys::gDelete(*iter);
          }
        }
        void insert_group_info(const int group_seq, const std::string& ns_addr, const bool is_master)
        {
          std::vector<GroupInfo*>::iterator iter = group_info_list_.begin();
          for (; group_info_list_.end() != iter; iter++)
          {
            // exist
            if (!(*iter)->ns_addr_.compare(ns_addr))
            {
              (*iter)->group_seq_ = group_seq;
              break;
            }
          }
          // new insert
          if (group_info_list_.end() == iter)
          {
            GroupInfo* group_info = new GroupInfo(group_seq, ns_addr, is_master);
            if (is_master)
            {
              group_info_list_.insert(group_info_list_.begin(), group_info);
            }
            else
            {
              group_info_list_.push_back(group_info);
            }
          }
        }
        void get_need_update_group_info_list(std::vector<GroupInfo*>& need_group_info_list)
        {
          std::vector<GroupInfo*>::iterator iter = group_info_list_.begin();
          for (; group_info_list_.end() != iter; iter++)
          {
            if (-1 == (*iter)->group_seq_)
            {
              need_group_info_list.push_back(*iter);
            }
          }
        }
        bool get_ns_addr(const uint32_t block_id, std::string& ns_addr)
        {
          bool bRet = false;
          if (group_count_ > 0)
          {
            int group_seq = block_id % group_count_;
            std::vector<GroupInfo*>::iterator iter = group_info_list_.begin();
            for (; group_info_list_.end() != iter; iter++)
            {
              if (group_seq == (*iter)->group_seq_)
              {
                ns_addr = (*iter)->ns_addr_;
                bRet = true;
                break;
              }
            }
          }
          return bRet;
        }
        int group_count_;
        std::vector<GroupInfo*> group_info_list_;
      };

      public:
        TfsClusterManager();
        ~TfsClusterManager();
        void clear();
        void dump();

        int add_ns_into_write_ns(const std::string& ip_str);

        int add_ns_into_read_ns(const std::string& ip_str, const int32_t cluster_id);

        int add_ns_into_unlink_ns(const std::string& ip_str, const int32_t cluster_id, bool is_master);

        bool update_cluster_group_info(ClusterGroupInfo* cluster_group_info);

        std::string get_write_ns_addr(const int index);
        std::string get_read_ns_addr(const char* file_name, const int index);
        std::string get_read_ns_addr_ex(const int32_t cluster_id, const int index);
        std::string get_unlink_ns_addr(const char* file_name, const int index);
        std::string get_unlink_ns_addr_ex(const int32_t cluster_id, const uint32_t block_id, const int index);

      private:
        DISALLOW_COPY_AND_ASSIGN(TfsClusterManager);
        int32_t get_cluster_id(const char* file_name);
        uint32_t get_block_id(const char* file_name);

        tbsys::CThreadMutex mutex_;
        static const int8_t CHOICE_CLUSTER_NS_TYPE_LENGTH = 3;
        typedef std::map<int32_t, std::string> ClusterNsType; //<cluster_id, ns>
        ClusterNsType read_ns_[CHOICE_CLUSTER_NS_TYPE_LENGTH];
        std::string write_ns_[CHOICE_CLUSTER_NS_TYPE_LENGTH];
        std::map<int32_t, ClusterGroupInfo*> unlink_ns_; //<cluster_id, cluster_group_info>
    };
  }
}

#endif
