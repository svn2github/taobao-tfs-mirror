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
#ifndef TFS_RCSERVER_BASERESOURCE_H_
#define TFS_RCSERVER_BASERESOURCE_H_

#include <map>
#include <set>
#include <string>
#include "common/define.h"
#include "resource.h"

namespace tfs
{
  namespace rcserver
  {
    struct ClusterInfo
    {
      std::string cluster_id_;
      uint64_t ns_vip_;
      uint64_t cluster_stat_;
    };

    struct AppRackInfo
    {
      uint64_t cluster_rack_id_;
      uint64_t rack_access_type_;
    };

    class BaseResource : public IResource
    {
      public:
        BaseResource(MysqlConn* mysql_conn) : IResource(mysql_conn)
        {
        }
        virtual ~BaseResource()
        {
        }

      public:
        virtual int load();

      private:
        int load_rc_server();
        int load_cluster_rack();
        int load_app_group();
        int load_duplicate_server();

      private:
        BaseResource();
        DISALLOW_COPY_AND_ASSIGN(BaseResource);

        // server_id <-> status
        std::map<uint64_t, uint64_t> rc_server_info_;
        // cluster_rack_id <-> set<ClusterInfo>
        std::map<uint64_t, std::set<ClusterInfo> > cluster_rack_info_;
        // app_group_id <-> set<AppRackInfo>
        std::map<uint64_t, std::set<AppRackInfo> > app_group_info_;
        // cluster_rack_id <-> config_server_ids
        std::map<uint64_t, std::set<uint64_t> > rack_dup_info_;
    };
  }
}
#endif //TFS_RCSERVER_BASERESOURCE_H_
