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
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *
 */
#ifndef TFS_RCSERVER_RESOURCE_SERVER_DATA_H_
#define TFS_RCSERVER_RESOURCE_SERVER_DATA_H_
#include <stdint.h>
#include <vector>
#include <map>
#include <string>
#include <set>
namespace tfs
{
  namespace rcserver
  {

    const int ADDR_INFO_LEN = 64;
    const int REM_LEN = 256;
    const int CLUSTER_ID_LEN = 8;
    const int APP_KEY_LEN = 256;
    const int SESSION_ID_LEN = 256;
    const int CLIENT_VERSION_LEN = 64;

    struct ResourceServerInfo
    {
      int32_t stat_;
      char addr_info_[ADDR_INFO_LEN];
      char rem_[REM_LEN];
      ResourceServerInfo()
      {
        stat_ = -1;
        addr_info_[0] = '\0';
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ResourceServerInfo> VResourceServerInfo;

    struct ClusterData
    {
      int32_t cluster_stat_;
      int32_t access_type_;
      std::string cluster_id_;
      std::string ns_vip_;
      ClusterData()
      {
        cluster_stat_ = -1;
        access_type_ = -1;
      }
    };

    struct ClusterRackInfo
    {
      int32_t cluster_rack_id_;
      ClusterData cluster_data_;
      char rem_[REM_LEN];
      ClusterRackInfo()
      {
        cluster_rack_id_ = -1;
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackInfo> VClusterRackInfo;

    struct ClusterRackGroup
    {
      int32_t cluster_group_id_;
      int32_t cluster_rack_id_;
      int32_t cluster_rack_access_type_;
      char rem_[REM_LEN];
      ClusterRackGroup()
      {
        cluster_group_id_ = -1;
        cluster_rack_id_ = -1;
        cluster_rack_access_type_ = -1;
        rem_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackGroup> VClusterRackGroup;

    struct ClusterRackData
    {
      bool need_duplicate_;
      std::string dupliate_server_addr_;
      std::vector<ClusterData> cluster_data_;
      ClusterRackData()
      {
        need_duplicate_ = false;
      }
    };

    struct ClusterRackDuplicateServer
    {
      int32_t cluster_rack_id_;
      char dupliate_server_addr_[ADDR_INFO_LEN];
      ClusterRackDuplicateServer()
      {
        cluster_rack_id_ = -1;
        dupliate_server_addr_[0] = '\0';
      }
    };
    typedef std::vector<ClusterRackDuplicateServer> VClusterRackDuplicateServer;

    struct BaseInfoUpdateTime
    {
      int64_t base_last_update_time_;
      int64_t app_last_update_time_;
      BaseInfoUpdateTime()
      {
        base_last_update_time_ = -1;
        app_last_update_time_ = -1;
      }
    };

    struct AppInfo
    {
      int64_t quto_;
      int32_t id_;
      int32_t cluster_group_id_;
      int32_t report_interval_;
      int32_t need_duplicate_;
      int64_t modify_time_;
      char key_[APP_KEY_LEN];
      char app_name_[REM_LEN];
      char app_owner_[REM_LEN];
      char rem_[REM_LEN];
      AppInfo()
      {
        quto_ = -1;
        id_ = -1;
        cluster_group_id_ = -1;
        report_interval_ = -1;
        need_duplicate_ = -1;
        modify_time_ = -1;
        key_[0] = '\0';
        app_name_[0] = '\0';
        app_owner_[0] = '\0';
        rem_[0] = '\0';
      }
    };
    typedef std::map<int32_t, AppInfo> MIdAppInfo;  //map<id, appinfo>

    struct SessionBaseInfo
    {
      std::string session_id_;
      std::string client_version_;
      uint64_t cache_size_;
      uint64_t modify_time_;
      // need extend?
    };

    enum OperType
    {
      OPER_READ = 1,
      OPER_WRITE,
      OPER_UNIQUE_WRITE,
      OPER_UNLINK,
      OPER_UNIQUE_UNLINK
    };

    struct AppOperInfo
    {
      OperType oper_type_;
      uint64_t oper_times_;  //total
      uint64_t oper_size_;   //succ
      uint64_t oper_rt_;     //succ ¿€º”÷µ
      uint64_t oper_succ_;
    };

    struct SessionStat
    {
      std::set<AppOperInfo> app_oper_info_;
      uint64_t cache_hit_ratio;
    };

    //struct SessionInfo
    //{
    //  int64_t cache_size_;
    //  int64_t logout_time_;
    //  char session_id_[SESSION_ID_LEN];
    //  char client_version_[CLIENT_VERSION_LEN];
    //  SessionInfo()
    //  {
    //    cache_size_ = -1;
    //    logout_time_ = -1;
    //    session_id_[0] = '\0';
    //    client_version_[0] = '\0';
    //  }
    //};
    //typedef std::map<std::string, SessionInfo> MIdSessionInfo;

    //struct SessionStat
    //{
    //  int32_t oper_type_;
    //  int32_t response_time_;
    //  int64_t oper_times_;
    //  int64_t file_size_;
    //  char session_id_[SESSION_ID_LEN];
    //  SessionStat()
    //  {
    //    oper_type_ = -1;
    //    response_time_ = -1;
    //    oper_times_ = -1;
    //    file_size_ = -1;
    //    session_id_[0] ='\0';
    //  }
    //};
    //typedef std::map<std::string, map<int32_t, SessionStat> > MIdSessionStat;

    struct AppStat
    {
      int32_t id_;
      int32_t file_count_;
      int64_t used_capacity_;
      AppStat()
      {
        id_ = -1;
        file_count_ = -1;
        used_capacity_ = -1;
      }
    };
    typedef std::map<int32_t, AppStat> MIdAppStat;
  }
}
#endif
