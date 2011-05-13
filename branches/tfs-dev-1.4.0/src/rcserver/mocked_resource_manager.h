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
#ifndef TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
#define TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_

#include "i_resource_manager.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace rcserver
  {
    class MockedResourceManager : public IResourceManager
    {
      public:
        MockedResourceManager() {}
        ~MockedResourceManager() {}

      public:
        int initialize()
        {
          apps_["pic"] = 10001;
          apps_["ic"] = 10002;
          apps_["uc"] = 10003;
          apps_["dc"] = 10004;
          apps_["cc"] = 10005;
          apps_["fc"] = 10006;
          apps_["gc"] = 10007;

          old_info_.report_interval_ = 10;
          new_info_.report_interval_ = 20;

          id_2_mtimes_[10001] = 100000;
          id_2_mtimes_[10002] = 200000;
          id_2_mtimes_[10003] = 300000;
          id_2_mtimes_[10004] = 400000;
          id_2_mtimes_[10005] = 500000;
          id_2_mtimes_[10006] = 600000;
          id_2_mtimes_[10007] = 700000;

          return common::TFS_SUCCESS;
        }

        int load()
        {
          return common::TFS_SUCCESS;
        }

        int login(const std::string& app_key, int32_t& app_id, BaseInfo& base_info)
        {
          int ret = common::TFS_SUCCESS;
          std::map<std::string, int32_t>::iterator mit = apps_.find(app_key);
          if (mit == apps_.end())
          {
            ret = common::EXIT_APP_NOTEXIST_ERROR;
          }
          else
          {
            app_id = mit->second;
            base_info = old_info_;
          }
          return ret;
        }

        int check_update_info(const int32_t app_id, const int64_t modify_time, bool& update_flag, BaseInfo& base_info)
        {
          int ret = common::TFS_SUCCESS;
          std::map<int32_t, int64_t>::iterator mit = id_2_mtimes_.find(app_id);
          if (mit == id_2_mtimes_.end())
          {
            ret = common::EXIT_APP_NOTEXIST_ERROR;
          }
          else
          {
            if (modify_time < mit->second) //need update
            {
              update_flag = true;
              base_info = new_info_; //copy info
            }
            else // >=
            {
              update_flag = false;
            }
          }

          return ret;
        }

        int logout(const std::string& session_id)
        {
          int ret = common::TFS_SUCCESS;
          return ret;
        }

        int update_session_info(const std::vector<SessionBaseInfo>& session_infos)
        {
          return common::TFS_SUCCESS;
        }

        int update_session_stat(const std::map<std::string, SessionStat>& session_stats)
        {
          return common::TFS_SUCCESS;
        }

        int update_app_stat(const MIdAppStat& app_stats)
        {
          return common::TFS_SUCCESS;
        }
      private:
        DISALLOW_COPY_AND_ASSIGN(MockedResourceManager);
        std::map<std::string, int32_t> apps_;
        std::map<int32_t, int64_t> id_2_mtimes_;
        BaseInfo old_info_, new_info_;
    };
  }
}
#endif //TFS_RCSERVER_MOCKEDRESOURCEMANAGER_H_
