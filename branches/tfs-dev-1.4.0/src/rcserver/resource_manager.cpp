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
#include "resource_manager.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;

    int ResourceManager::initialize()
    {
      //init mysql
    }

    int ResourceManager::load()
    {
      int ret = TFS_SUCCESS;
      // load update info 
      if ((ret = dynamic_cast<UpdateResource*>(update_resource_)->load()) == TFS_SUCCESS)
      {
        if (dynamic_cast<UpdateResource*>(update_resource_)->get_base_update_flag())
        {
          // load base info
          ret = base_resource_->load();
        }

        if (TFS_SUCCESS == ret && dynamic_cast<UpdateResource*>(update_resource_)->get_app_update_flag())
        {
          // load app info
          ret = app_resource_->load();
        }
      }

      if (TFS_SUCCESS != ret)
      {
        //log
      }

      return ret;
    }

    int ResourceManager::login(const std::string& app_key, /*std::string& session_id, */BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      AppInfo app_info;
      //ret = app_resource_.get_app_info(key, app_info);
      //todo fill base info
      return ret;

    }

    int ResourceManager::keep_alive(const std::string& session_id, const uint64_t modify_time, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      //todo 
      return ret;
    }

    int ResourceManager::logout(const std::string& session_id)
    {
      int ret = TFS_SUCCESS;
      //todo 
      return ret;
    }
  }
}
