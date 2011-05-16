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
#ifndef TFS_RCSERVER_IRESOURCEMANAGER_H_
#define TFS_RCSERVER_IRESOURCEMANAGER_H_

#include <set>
#include <vector>
#include <string>
#include "common/define.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    struct BaseInfo
    {
      std::set<uint64_t> rc_server_infos_;
      std::vector<ClusterRackData> cluster_infos_;
      int32_t report_interval_;
    };

    class IResourceManager
    {
      public:
        IResourceManager(){}
        virtual ~IResourceManager() {}

      public:
        virtual int initialize() = 0;
        virtual int load() = 0;

        virtual int login(const std::string& app_key, int32_t& app_id, BaseInfo& base_info) = 0;
        virtual int check_update_info(const int32_t app_id, const int64_t modify_time, bool& update_flag, BaseInfo& base_info) = 0;
        virtual int get_app_name(const int32_t app_id, std::string& app_name) const
        {
          return common::TFS_SUCCESS;
        }

        virtual int update_session_info(const std::vector<SessionBaseInfo>& session_infos) = 0;
        virtual int update_session_stat(const std::map<std::string, SessionStat>& session_stats) = 0;
        virtual int update_app_stat(const MIdAppStat& app_stats) = 0;

    };
  }
}
#endif //TFS_RCSERVER_IRESOURCEMANAGER_H_
