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
#include <algorithm>
#include <assert.h>

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
      int64_t cache_size_;
      int64_t cache_time_;
      int64_t modify_time_;
      bool is_logout_;

      SessionBaseInfo(const std::string session_id) : 
        session_id_(session_id), cache_size_(0), cache_time_(0), modify_time_(0), is_logout_(false)
      {
      }
      SessionBaseInfo() : cache_size_(0), cache_time_(0), modify_time_(0), is_logout_(false)
      {
      }

      SessionBaseInfo& operator= (const SessionBaseInfo& right)
      {
        client_version_ = right.client_version_;
        cache_size_ = right.cache_size_;
        cache_time_ = right.cache_time_;
        modify_time_ = right.modify_time_;
        is_logout_ = right.is_logout_;
        return *this;
      }
    };

    enum OperType
    {
      OPER_INVALID = 0,
      OPER_READ,
      OPER_WRITE,
      OPER_UNIQUE_WRITE,
      OPER_UNLINK,
      OPER_UNIQUE_UNLINK
    };

    struct AppOperInfo
    {
      OperType oper_type_;
      int64_t oper_times_;  //total
      int64_t oper_size_;   //succ
      int64_t oper_rt_;     //succ ¿€º”÷µ
      int64_t oper_succ_;

      AppOperInfo() : oper_type_(OPER_INVALID), oper_times_(0),
                      oper_size_(0), oper_rt_(0), oper_succ_(0)
      {
      }

      AppOperInfo& operator +=(const AppOperInfo& right)
      {
        assert(oper_type_ == right.oper_type_);
        oper_times_ += right.oper_times_;
        oper_size_ += right.oper_size_;
        oper_rt_ += right.oper_rt_;
        oper_succ_+= right.oper_succ_;
        return *this;
      }
    };

    struct SessionStat
    {
      std::map<OperType, AppOperInfo> app_oper_info_;
      int64_t cache_hit_ratio_;

      SessionStat() : cache_hit_ratio_(100)
      {
        app_oper_info_.clear();
      }

      SessionStat& operator +=(const SessionStat& right)
      {
        std::map<OperType, AppOperInfo>::iterator lit;
        std::map<OperType, AppOperInfo>::const_iterator rit = right.app_oper_info_.begin();
        for ( ; rit != right.app_oper_info_.end(); ++rit)
        {
          lit = app_oper_info_.find(rit->first);
          if (lit == app_oper_info_.end()) //not found
          {
            app_oper_info_.insert(std::pair<OperType, AppOperInfo>(rit->first, rit->second));
          }
          else //found
          {
            lit->second += rit->second;
          }
        }
        return *this;
      }

    };

    struct AppStat
    {
      int32_t id_;
      int64_t file_count_;
      int64_t used_capacity_;
      AppStat()
      {
        id_ = -1;
        file_count_ = 0;
        used_capacity_ = 0;
      }

      explicit AppStat(const int32_t id)
      {
        id_ = id;
        file_count_ = 0;
        used_capacity_ = 0;
      }

      void add(const SessionStat& s_stat)
      {
        const std::map<OperType, AppOperInfo>& app_oper_info = s_stat.app_oper_info_;
        std::map<OperType, AppOperInfo>::const_iterator sit = app_oper_info.begin();
        for ( ; sit != app_oper_info.end(); ++sit)
        {
          if (OPER_WRITE == sit->first || OPER_UNIQUE_WRITE == sit->first)
          {
            file_count_ += sit->second.oper_times_;
            used_capacity_ += sit->second.oper_size_;
          }
          else if (OPER_UNLINK == sit->first || OPER_UNIQUE_UNLINK == sit->first)
          {
            file_count_ -= sit->second.oper_times_;
            used_capacity_ -= sit->second.oper_size_;
          }
        }
      }
    };
    typedef std::map<int32_t, AppStat> MIdAppStat;

    struct KeepAliveInfo
    {
      KeepAliveInfo(const std::string& session_id) : s_base_info_(session_id), last_report_time_(time(NULL))
      {
      }
      KeepAliveInfo() : last_report_time_(time(NULL))
      {
      }
      SessionBaseInfo s_base_info_;
      SessionStat s_stat_;
      int64_t last_report_time_;

      KeepAliveInfo& operator +=(const KeepAliveInfo& right)
      {
        s_stat_ += right.s_stat_;
        if (last_report_time_ < right.last_report_time_)
        {
          s_base_info_ = right.s_base_info_;
          s_stat_.cache_hit_ratio_ = right.s_stat_.cache_hit_ratio_;
          last_report_time_ = right.last_report_time_;
        }
        return *this;
      }
    };

    typedef std::map<std::string, KeepAliveInfo> SessionCollectMap;
    typedef SessionCollectMap::const_iterator SessionCollectMapConstIter;
    typedef SessionCollectMap::iterator SessionCollectMapIter;

    typedef std::map<int32_t, SessionCollectMap> AppSessionMap;
    typedef AppSessionMap::const_iterator AppSessionMapConstIter;
    typedef AppSessionMap::iterator AppSessionMapIter;
  }
}
#endif
