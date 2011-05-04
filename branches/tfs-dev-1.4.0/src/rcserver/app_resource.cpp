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
#include "app_resource.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
   
    int AppResource::load()
    {
      //TODO 
      //set app_last_update_time_
      return TFS_SUCCESS;
    }

    int AppResource::get_app_info(const std::string& app_key, AppInfo& app_info) const
    {
      int ret = TFS_SUCCESS;
      std::map<std::string, int32_t>::const_iterator it = app_ids_.find(app_key);
      if (it == app_ids_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        ret = get_app_info(it->second, app_info);
      }

    }

    int AppResource::get_app_info(const int32_t app_id, AppInfo& app_info) const
    {
      int ret = TFS_SUCCESS;
      MIdAppInfo::const_iterator it = m_id_appinfo_.find(app_id);
      if (it == m_id_appinfo_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        app_info = it->second;
      }
      return ret;
    }

    int AppResource::get_last_modify_time(const int32_t app_id, int64_t& last_modify_time) const
    {
      int ret = TFS_SUCCESS;
      MIdAppInfo::const_iterator it = m_id_appinfo_.find(app_id);
      if (it == m_id_appinfo_.end())
      {
        ret = TFS_ERROR;
      }
      else
      {
        last_modify_time = it->second.modify_time_;
      }
      return ret;
    }

    bool need_reload(const int64_t update_time_in_db) const
    {
      return update_time_in_db > app_last_update_time_;
    }

  }
}
