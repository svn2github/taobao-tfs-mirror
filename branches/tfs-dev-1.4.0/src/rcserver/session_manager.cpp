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

    int SessionManager::login(const std::string& app_key, const uint64_t session_ip,
        std::string& session_id, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      if ((ret = resource_manager_.login(app_key, session_ip, base_info)) == TFS_SUCCESS)
      {
        //gene session id
        //session_id = 
        //
       
        SessionCollectMapIter mit = session_infos_.find(session_id_);
        if (mit == session_infos_.end())
        {
        }
        else
        {
          //error
        }
      }
      else
      {
      }

      return ret;
    }

    int SessionManager::keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      
      // accumulate keep alive stat info
      
      // get update info
      ret = resource_manager_.keep_alive(const uint64_t modify_time, const BaseInfo& base_info);
      
      return ret;
    }

    int SessionManager::initialize()
    {
      //init mysql
    }

    int SessionManager::load()
    {
      int ret = TFS_SUCCESS;
      // load update info 
      if ((ret = update_resource_->load()) == TFS_SUCCESS)
      {
        if (update_resource_->get_base_update_flag())
        {
          // load base info
          ret = base_resource_->load();
        }

        if (TFS_SUCCESS == ret && update_resource_->get_app_update_flag())
        {
          // load app info
          ret = app_resource->load();
        }
      }

      if (TFS_SUCCESS != ret)
      {
        //log
      }

      return ret;
    }

    int SessionManager::login(const std::string& app_key, const uint64_t session_ip,
        std::string& session_id, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      AppInfo app_info;
      ret = app_resource_.get_app_info(key, app_info);
      //todo fill base info
      //gene session_id 
      return ret;

    }

    int SessionManager::keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info)
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int SessionManager::logout(const std::string& session_id)
    {
      int ret = TFS_SUCCESS;
      return ret;
    }
  }
}
