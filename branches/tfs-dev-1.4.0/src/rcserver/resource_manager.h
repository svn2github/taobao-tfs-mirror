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
#include "common/define.h"
#include "mysql_database_helper.h"

namespace tfs
{
  namespace rcserver
  {

    //enum LoadFlag
    //{
    //  LOAD_NONE = 0,
    //  LOAD_ALL,
    //  LOAD_BASE,
    //  LOAD_APP
    //};
    class AppResource;
    class BaseResource;

    class ResourceManager
    {
      public:
        ResourceManager() :
          app_resource_manager_(NULL)
        {
        }
        ~ResourceManager()
        {
        }

      public:
        int initialize();
        int load();

        int login(const std::string& app_key, const uint64_t session_ip, BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const uint64_t modify_time, BaseInfo& base_info);
        int logout(const std::string& session_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(ResourceManager);
        MysqlDatabaseHelper database_helper_;
        AppResource* app_resource_manager_;
        BaseResource* base_resource_manager_;

    };
  }
}
#endif //TFS_RCSERVER_RESOURCEMANAGER_H_
