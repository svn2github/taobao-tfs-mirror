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
#ifndef TFS_RCSERVER_APPRESOURCE_H_
#define TFS_RCSERVER_APPRESOURCE_H_

#include <map>
#include <string>
#include "common/define.h"
#include "resource.h"

namespace tfs
{
  namespace rcserver
  {
    struct AppInfo
    {
      AppInfo() : app_id_(0), app_group_id_(0),
                  report_interval_(0), duplicate_flag_(false)
      {}

      uint64_t app_id_;
      std::string app_name_;
      uint64_t app_group_id_;
      uint64_t report_interval_;
      bool duplicate_flag_;
    };

    class AppResource : public IResource
    {
      public:
        AppResource(MysqlConn* mysql_conn) : IResource(mysql_conn)
        {
        }
        virtual ~AppResource()
        {
        }

      public:
        virtual int load();
        const std::map<std::string, AppInfo>& get_app_collect() const
        {
          return app_collect_;
        }

        int get_app_info(const std::string& app_key, AppInfo& app_info) const
        {
          int ret = common::TFS_SUCCESS;
          AppCollectMapConstIter mit = app_collect_.find(app_key);
          if (mit != app_collect_.end())
          {
            app_info = mit->second; 
          }
          else
          {
            // ret = error
          }

          return ret;
        }

      private:
        int load_app_info();

      private:
        AppResource();
        DISALLOW_COPY_AND_ASSIGN(AppResource);

        // app_id <-> app_key
        std::map<uint64_t, std::string> app_ids_;
        // app_key <-> AppInfo
        typedef std::map<std::string, AppInfo> AppCollectMap;
        typedef AppCollectMap::const_iterator AppCollectMapConstIter;
        AppCollectMap app_collect_;
    };
  }
}
#endif //TFS_RCSERVER_APPRESOURCE_H_
