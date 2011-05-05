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
#include <string>
#include "common/define.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    struct BaseInfo
    {
      std::set<int64_t> rc_server_infos_;
      std::vector<ClusterRackData> cluster_infos_;
      int32_t report_interval_;
    };

    class IResourceManager
    {
      public:
        virtual ~IResourceManager() {}

      public:
        virtual int initialize() = 0;
        virtual int load() = 0;

        virtual int login(const std::string& app_key, int32_t& app_id, BaseInfo& base_info) = 0;
        virtual int check_update_info(const int32_t app_id, const int64_t modify_time, bool& update_flag, BaseInfo& base_info) = 0;
        virtual int logout(const std::string& session_id) = 0;
    };
  }
}
#endif //TFS_RCSERVER_IRESOURCEMANAGER_H_
