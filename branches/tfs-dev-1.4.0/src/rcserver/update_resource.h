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
#ifndef TFS_RCSERVER_UPDATERESOURCE_H_
#define TFS_RCSERVER_UPDATERESOURCE_H_

#include "common/define.h"
#include "resource.h"

namespace tfs
{
  namespace rcserver
  {
    class UpdateResource : public IResource
    {
      public:
        UpdateResource(MysqlConn* mysql_conn) : 
          IResource(mysql_conn), base_update_time_(0), app_update_time_(0),
          base_last_update_time_(0), app_last_update_time_(0)
        {
        }

        virtual ~UpdateResource()
        {
        }

      public:
        virtual int load();

        time_t get_base_update_time() const
        {
          return base_last_update_time_;
        }

        time_t get_app_update_time() const
        {
          return app_last_update_time_;
        }

        bool get_base_update_flag() const
        {
          return base_update_time_ > base_last_update_time_;
        }

        bool get_app_update_flag() const
        {
          return app_update_time_ > app_last_update_time_;
        }

      private:
        int load_update_info();

      private:
        UpdateResource();
        DISALLOW_COPY_AND_ASSIGN(UpdateResource);

        time_t base_update_time_;
        time_t app_update_time_;

        time_t base_last_update_time_;
        time_t app_last_update_time_;
    };
  }
}
#endif //TFS_RCSERVER_APPRESOURCE_H_
