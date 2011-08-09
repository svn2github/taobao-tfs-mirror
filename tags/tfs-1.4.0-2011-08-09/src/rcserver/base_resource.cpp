/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   zongdai <zongdai@taobao.com>
 *      - initial release
 *
 */
#include "base_resource.h"
#include <algorithm>
#include <tbsys.h>
#include "mysql_database_helper.h"
namespace 
{
  const int SERVER_STAT_AVALIABLE = 1;
  const int CLUSTER_ACCESS_TYPE_FORBIDEN = 0;
  const int CLUSTER_ACCESS_TYPE_READ_ONLY = 1;
  const int CLUSTER_ACCESS_TYPE_READ_AND_WRITE = 2;
}
namespace tfs
{
  namespace rcserver
  {
    using namespace common;

    int BaseResource::load()
    {
      v_resource_server_info_.clear();
      v_cluster_rack_info_.clear();
      v_cluster_rack_group_.clear();
      v_cluster_rack_duplicate_server_.clear();
      int ret = TFS_SUCCESS;
      ret = database_helper_.scan(v_resource_server_info_);
      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "load resource_server_info error ret is %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_info_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_info error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_group_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_group error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        ret = database_helper_.scan(v_cluster_rack_duplicate_server_);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load cluster_rack_duplicate_server error ret is %d", ret);
        }
      }
      if (TFS_SUCCESS == ret)
      {
        BaseInfoUpdateTime outparam;
        ret = database_helper_.select(outparam);
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "load base_info_update_time error ret is %d", ret);
        }
        else
        {
          base_last_update_time_ = outparam.base_last_update_time_;
        }
      }
      return ret;
    }

    bool BaseResource::need_reload(const int64_t update_time_in_db) const
    {
      return update_time_in_db > base_last_update_time_;
    }
    int BaseResource::get_resource_servers(std::vector<uint64_t>& resource_servers) const
    {
      int ret = TFS_SUCCESS;
      resource_servers.clear();
      VResourceServerInfo::const_iterator it = v_resource_server_info_.begin();
      for (; it != v_resource_server_info_.end(); it++)
      {
        if (SERVER_STAT_AVALIABLE == it->stat_)
        {
          uint64_t server = tbsys::CNetUtil::strToAddr(it->addr_info_, 0);
          if (0 == server)
          {
            TBSYS_LOG(WARN, "unknow server %s", it->addr_info_);
          }
          else
          {
            std::vector<uint64_t>::const_iterator vit = find(resource_servers.begin(), resource_servers.end(), server);
            if (vit != resource_servers.end())
            {
              TBSYS_LOG(WARN, "server repeated %s", it->addr_info_);
            }
            else
            {
              resource_servers.push_back(server);
            }
          }
        }
      }
      return ret;
    }

    int BaseResource::get_cluster_infos(const int32_t cluster_group_id, 
        std::vector<ClusterRackData>& cluster_rack_datas) const
    {
      cluster_rack_datas.clear();
      int ret = TFS_SUCCESS;
      VClusterRackDuplicateServer::const_iterator rack_duplicate_it;
      VClusterRackInfo::const_iterator rack_info_it;
      VClusterRackGroup::const_iterator rack_group_it = v_cluster_rack_group_.begin();

      for (; rack_group_it != v_cluster_rack_group_.end(); rack_group_it++)
      {
        //find right group
        if (cluster_group_id == rack_group_it->cluster_group_id_)
        {
          int32_t current_rack_id = rack_group_it->cluster_rack_id_;
          ClusterRackData tmp_rack;
          bool read_only = true;
          for (rack_info_it = v_cluster_rack_info_.begin();
              rack_info_it != v_cluster_rack_info_.end(); rack_info_it++)
          {
            //find right cluster rack
            if (rack_info_it->cluster_rack_id_ == current_rack_id
                && CLUSTER_ACCESS_TYPE_FORBIDEN != rack_info_it->cluster_data_.cluster_stat_)
            {
              //found one available cluster;
              ClusterData tmp_cluster;
              tmp_cluster = rack_info_it->cluster_data_;
              if (tmp_cluster.cluster_stat_ > rack_group_it->cluster_rack_access_type_)
              {
                tmp_cluster.access_type_ = rack_group_it->cluster_rack_access_type_;
              }
              else
              {
                tmp_cluster.access_type_ = tmp_cluster.cluster_stat_;
              }
              if (CLUSTER_ACCESS_TYPE_READ_AND_WRITE == tmp_cluster.access_type_)
              {
                read_only = false;
              }
              tmp_rack.cluster_data_.push_back(tmp_cluster);
            }
          }
          if (!tmp_rack.cluster_data_.empty())
          {
            //find duplicate server info
            rack_duplicate_it = v_cluster_rack_duplicate_server_.begin();
            for (; rack_duplicate_it != v_cluster_rack_duplicate_server_.end(); rack_duplicate_it++)
            {
              if (rack_duplicate_it->cluster_rack_id_ == current_rack_id && !read_only)
              {
                tmp_rack.dupliate_server_addr_ = rack_duplicate_it->dupliate_server_addr_;
                tmp_rack.need_duplicate_ = true;
              }
            }
            cluster_rack_datas.push_back(tmp_rack);
          }
        }
      }
      return ret;
    }

    int BaseResource::get_last_modify_time(int64_t& last_modify_time) const
    {
      last_modify_time = base_last_update_time_;
      return TFS_SUCCESS;
    }

  }
}
