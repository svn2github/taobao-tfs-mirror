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
#include "session_manager.h"
#include "uuid.h"
#include "common/error_msg.h"

namespace tfs
{
  namespace rcserver
  {
    using namespace common;
    using namespace std;

    int SessionManager::initialize()
    {
      int ret = TFS_SUCCESS;
      if (NULL == resource_manager_ || 0 == timer_)
      {
        ret = EXIT_INVALID_ARGU;
      }
      else
      {
        stat_db_task_ = new SessionStatTask(*this, STAT_DB);
        stat_monitor_task_ = new SessionStatTask(*this, STAT_MONITOR);
        expire_task_ = new SessionExpireTask(*this);

        int64_t db_interval, monitor_interval, expire_interval; // read this from config file
        //scheduleRepeated return 0 if succ
        if (((ret = timer_->scheduleRepeated(stat_db_task_, tbutil::Time::seconds(db_interval))) != 0)
            || ((ret = timer_->scheduleRepeated(stat_monitor_task_, tbutil::Time::seconds(monitor_interval))) != 0)
            || ((ret = timer_->scheduleRepeated(expire_task_, tbutil::Time::seconds(expire_interval))) != 0)
            )
        {
          TBSYS_LOG(ERROR, "call scheduleRepeated failed, db_interval_: %"PRI64_PREFIX"d, monitor_interval_: %"PRI64_PREFIX"d,"
              " expire_interval_: %"PRI64_PREFIX"d, ret: %d",
              db_interval, monitor_interval, expire_interval, ret);
        }
      }

      return ret;
    }

    int SessionManager::wait_for_shut_down()
    {
      if (0 != timer_)
      {
        if (0 != stat_db_task_)
        {
          timer_->cancel(stat_db_task_);
        }
        if (0 != stat_monitor_task_)
        {
          timer_->cancel(stat_monitor_task_);
        }
        if (0 != expire_task_)
        {
          timer_->cancel(expire_task_);
        }
      }

      return TFS_SUCCESS;
    }

    void SessionManager::destroy()
    {
      destroy_ = true;
    }

    int SessionManager::login(const std::string& app_key, const int64_t session_ip,
        std::string& session_id, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      int32_t app_id = 0;
      if ((ret = resource_manager_->login(app_key, app_id, base_info)) == TFS_SUCCESS)
      {
        // gene session id
        gene_session_id(app_id, session_ip, session_id);

        KeepAliveInfo keep_alive_info;
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, LOGIN_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::update_session_info failed,"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call ResourceManager::login failed, app_key: %s, session_ip: %d, ret: %d",
            app_key.c_str(), session_ip, ret);
      }
      return ret;
    }

    int SessionManager::keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      int32_t app_id = 0;
      int64_t session_ip = 0;
      if ((ret = parse_session_id(session_id, app_id, session_ip) == TFS_SUCCESS))
      {
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, KA_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::update_session_info failed, this will not be happen!"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionManager::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
      }
      
      if (TFS_SUCCESS == ret)
      {
        // get update info
        ret = resource_manager_->keep_alive(session_id, keep_alive_info.s_base_info_.modify_time_, base_info);
        // Todo
      }
      return ret;
    }

    int SessionManager::logout(const std::string& session_id, const KeepAliveInfo& keep_alive_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      int32_t app_id = 0;
      int64_t session_ip = 0;
      if ((ret = parse_session_id(session_id, app_id, session_ip) == TFS_SUCCESS))
      {
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, LOGOUT_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionManager::update_session_info failed, this will not be happen!"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionManager::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
      }

      if (TFS_SUCCESS == ret)
      {
        // get update info
        ret = resource_manager_->logout(session_id);
        // Todo
      }

      return ret;
    }

    int SessionManager::dump(const StatFlag stat_flag)
    {
      int ret = TFS_SUCCESS;
      if (stat_flag == STAT_DB)
      {
        ret = dump_to_db();
      }
      else if (stat_flag == STAT_MONITOR)
      {
        ret = dump_to_monitor();
      }
      else
      {
        ret = EXIT_INVALID_ARGU;
      }
      return ret;
    }

    int SessionManager::expire_session()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    int SessionManager::update_session_info(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      int ret = TFS_SUCCESS;
      AppSessionMapIter mit = app_sessions_.find(app_id);
      if (mit == app_sessions_.end()) // keep_alive accept this scene: client maybe change rc when run
      {
        SessionCollectMap sc;
        sc.insert(pair<string, KeepAliveInfo>(session_id, keep_alive_info));
        app_sessions_.insert(pair<int32_t, SessionCollectMap>(app_id, sc));
      }
      else //find
      {
        SessionCollectMapIter scit = mit->second.find(session_id); 
        if (scit == mit->second.end()) //new session
        {
          mit->second.insert(pair<string, KeepAliveInfo>(session_id, keep_alive_info));
        }
        else //session exist. Login will failed, others will accumulate keep alive stat info
        {
          if (LOGIN_FLAG == update_flag)
          {
            ret = EXIT_SESSION_EXIST_ERROR;
          }
          else
          {
            scit->second += keep_alive_info;
          }
        }
      }
      return ret;
    }

    int SessionManager::dump_to_db()
    {
      int ret = TFS_SUCCESS;
      // dump session stat
      // dump app stat
      // if write to db failed, write to local file
      return ret;
    }

    int SessionManager::dump_to_monitor()
    {
      int ret = TFS_SUCCESS;
      return ret;
    }

    void SessionManager::gene_session_id(const int32_t app_id, const int64_t session_ip, std::string& session_id)
    {
      std::stringstream tmp_stream; 
      tmp_stream << app_id << SEPARATOR_KEY << session_ip << SEPARATOR_KEY << gene_uuid_str();
      tmp_stream >> session_id;
    }

    int SessionManager::parse_session_id(const std::string& session_id, int32_t& app_id, int64_t& session_ip)
    {
      int ret = TFS_SUCCESS;
      size_t first_pos = session_id.find_first_of(SEPARATOR_KEY, 0);
      if (string::npos != first_pos)
      {
        app_id = atoi(session_id.substr(0, first_pos).c_str());
        size_t second_pos = session_id.find_first_of(SEPARATOR_KEY, first_pos + 1);
        if (string::npos != second_pos)
        {
          session_ip = atoll(session_id.substr(first_pos + 1, second_pos - first_pos).c_str());
        }
        else
        {
          ret = EXIT_SESSIONID_INVALID_ERROR;
        }
      }
      else
      {
        ret = EXIT_SESSIONID_INVALID_ERROR;
      }

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "SessionManager::parse_session_id failed, session_id: %s", session_id.c_str());
      }

      return ret;
    }


    void SessionManager::SessionStatTask::runTimerTask()
    {
      manager_.dump(stat_flag_);
      return;
    }

    void SessionManager::SessionExpireTask::runTimerTask()
    {
      manager_.expire_session();
      return;
    }
  }
}
