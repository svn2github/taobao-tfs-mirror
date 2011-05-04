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
#ifndef TFS_RCSERVER_RESOURCEMANAGER_H_
#define TFS_RCSERVER_RESOURCEMANAGER_H_

#include <map>
#include <set>
#include <string>
#include "mysql_conn.h"
#include "base_resource.h"
#include "app_resource.h"
#include "update_resource.h"
#include "common/define.h"

namespace tfs
{
  namespace rcserver
  {
    struct BaseInfo
    {
      std::set<uint64_t> rc_server_info_;
      std::set<ClusterInfo> cluster_info_;
      bool duplicate_flag_;
      std::set<uint64_t> duplicate_config_server_id_;
      uint64_t report_interval_;
    };

    //enum LoadFlag
    //{
    //  LOAD_NONE = 0,
    //  LOAD_ALL,
    //  LOAD_BASE,
    //  LOAD_APP
    //};

    class ResourceManager
    {
      public:
        ResourceManager() :
          mysql_conn_(NULL), update_resource_(NULL), base_resource_(NULL), app_resource_(NULL)
        {
        }
        ~ResourceManager()
        {
        }

      public:
        int initialize();
        int load();

        int login(const std::string& app_key, const uint64_t session_ip,
            /*std::string& session_id, */BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const uint64_t modify_time, BaseInfo& base_info);
        int logout(const std::string& session_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(ResourceManager);

        MysqlConn* mysql_conn_;
        IResource* update_resource_;
        IResource* base_resource_;
        IResource* app_resource_;
    };
  }
}
#endif //TFS_RCSERVER_RESOURCEMANAGER_H_
