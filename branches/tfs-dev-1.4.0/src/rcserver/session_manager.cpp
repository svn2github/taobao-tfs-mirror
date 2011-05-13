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

    map<OperType, string> ISessionTask::key_map_;

    int SessionManager::initialize(bool reload_flag)
    {
      int ret = TFS_SUCCESS;
      if (NULL == resource_manager_ || 0 == timer_ || (!is_init_ && reload_flag) || (is_init_ && !reload_flag))
      {
        ret = EXIT_INVALID_ARGU;
        TBSYS_LOG(ERROR, "call ResourceManager::initialize failed, is_init_: %d, reload_flag: %d, ret: %d",
            is_init_, reload_flag, ret);
      }
      else
      {
        if ((!is_init_ && !reload_flag) || (is_init_ && reload_flag))
        {
          tbutil::Mutex::Lock lock(mutex_);
          if ((!is_init_ && !reload_flag) || (is_init_ && reload_flag))
          {
            if (is_init_ && reload_flag)
            {
              destroy();
              wait_for_shut_down();
            }

            int64_t monitor_interval = 5, stat_interval = 10;
            monitor_task_ = new SessionMonitorTask(*this);
            stat_task_ = new SessionStatTask(*this, resource_manager_);

            if (((ret = timer_->scheduleRepeated(monitor_task_, tbutil::Time::seconds(monitor_interval))) != 0)
                || ((ret = timer_->scheduleRepeated(stat_task_, tbutil::Time::seconds(stat_interval))) != 0)
               )
            {
              TBSYS_LOG(ERROR, "call scheduleRepeated failed, stat_interval_: %"PRI64_PREFIX"d, monitor_interval_: %"PRI64_PREFIX"d,"
                  " ret: %d",
                  stat_interval, monitor_interval, ret);

              destroy();
              wait_for_shut_down();

              ret = TFS_ERROR;
            }
            else
            {
              is_init_ = true;
              ret = TFS_SUCCESS;
            }
          }
        }
      }

      return ret;
    }

    int SessionManager::wait_for_shut_down()
    {
      if (0 != timer_)
      {
        if (0 != monitor_task_)
        {
          timer_->cancel(monitor_task_);
          monitor_task_ = 0;
        }
        if (0 != stat_task_)
        {
          timer_->cancel(stat_task_);
          stat_task_ = 0;
        }
      }

      return TFS_SUCCESS;
    }

    void SessionManager::destroy()
    {
      if (0 != monitor_task_)
      {
        monitor_task_->destroy();
      }
      if (0 != stat_task_)
      {
        stat_task_->destroy();
      }
      is_init_ = false;
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

        KeepAliveInfo keep_alive_info(session_id);
        //keep_alive_info.s_base_info_.session_id_ = session_id;
        if ((ret = update_session_info(app_id, session_id, keep_alive_info, LOGIN_FLAG)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed,"
              " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, LOGIN_FLAG, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call ResourceManager::login failed, app_key: %s, session_ip: %d, ret: %d",
            app_key.c_str(), session_ip, ret);
      }
      return ret;
    }

    int SessionManager::keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info,
        bool& update_flag, BaseInfo& base_info)
    {
      int ret = TFS_SUCCESS;
      // get app_id from session_id
      int32_t app_id = 0;
      int64_t session_ip = 0;
      if ((ret = parse_session_id(session_id, app_id, session_ip)) == TFS_SUCCESS)
      {
        // first check update info, then update session info
        if ((ret = resource_manager_->check_update_info(app_id, keep_alive_info.s_base_info_.modify_time_,
                update_flag, base_info)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "call IResourceManager::check_update_info failed. app_id: %d, modify_time: %"PRI64_PREFIX"d, ret: %d",
              app_id, keep_alive_info.s_base_info_.modify_time_, ret);
        }
        else
        {
          if ((ret = update_session_info(app_id, session_id, keep_alive_info, KA_FLAG)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed, this will not be happen!"
                " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
                session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, KA_FLAG, ret);
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionManager::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
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
              " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag, ret: %d",
              session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, LOGOUT_FLAG, ret);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "call SessionManager::parse_session_id failed, session_id: %s, modify time: %"PRI64_PREFIX"d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, ret);
      }

      return ret;
    }

    int SessionManager::update_session_info(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      int ret = TFS_SUCCESS;
      if ((ret = monitor_task_->update_session_info(app_id, session_id, keep_alive_info, update_flag)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SessionMonitorTask::update_session_info failed."
            " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, update_flag, ret);
      }
      else if ((ret = stat_task_->update_session_info(app_id, session_id, keep_alive_info, update_flag)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "call SessionStatTask::update_session_info failed."
            " session_id: %s, modify time: %"PRI64_PREFIX"d, update_flag: %d, ret: %d",
            session_id.c_str(), keep_alive_info.s_base_info_.modify_time_, update_flag, ret);
      }

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
        TBSYS_LOG(ERROR, "SessionManager::parse_session_id failed, session_id: %s, ret: %d", session_id.c_str(), ret);
      }

      return ret;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    int ISessionTask::update_session_info(const int32_t app_id, const std::string& session_id,
        const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag)
    {
      ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
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
            TBSYS_LOG(INFO, "session id exist when login, session id: %s, flag: %d", session_id.c_str(), update_flag);
            ret = EXIT_SESSION_EXIST_ERROR;
          }
          else
          {
            scit->second += keep_alive_info;

            if (LOGOUT_FLAG == update_flag)
            {
              scit->second.s_base_info_.is_logout_ = true;
            }
            else
            {
              scit->second.s_base_info_.is_logout_ = false;
            }
          }
        }
      }
      return ret;
    }

    void ISessionTask::display(const int32_t app_id, const SessionStat& s_stat)
    {
      //Todo: replace app id with app name
      TBSYS_LOG(INFO, "monitor app: %d, cache_hit_ratio: %"PRI64_PREFIX"d", app_id, s_stat.cache_hit_ratio_);
      std::map<OperType, AppOperInfo>::const_iterator mit = s_stat.app_oper_info_.begin();
      for ( ; mit != s_stat.app_oper_info_.end(); ++mit)
      {
        TBSYS_LOG(INFO, "monitor app: %d, oper_type: %s, oper_times: %"PRI64_PREFIX"d, oper_size: %"PRI64_PREFIX"d,"
            " oper_rt: %"PRI64_PREFIX"d, oper_succ: %"PRI64_PREFIX"d",
            app_id, key_map_[mit->first].c_str(), mit->second.oper_times_,
            mit->second.oper_size_, mit->second.oper_rt_, mit->second.oper_succ_);
      }
      return;
    }

    void SessionMonitorTask::runTimerTask()
    {
      if (!destroy_)
      {
        AppSessionMap tmp_sessions;
        {
          ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
          tmp_sessions.swap(app_sessions_);
          assert(app_sessions_.size() == 0);
        }

        typedef std::map<std::string, KeepAliveInfo> SessionCollectMap;
        typedef std::map<int32_t, SessionCollectMap> AppSessionMap;
        // dump session stat
        // use app id now. should replace it with app name
        AppSessionMapConstIter mit = tmp_sessions.begin();
        for ( ; mit != tmp_sessions.end(); ++mit)
        {
          SessionStat merge_stat;
          SessionCollectMapConstIter sit = mit->second.begin();
          for ( ; sit != mit->second.end(); ++sit)
          {
            merge_stat += sit->second.s_stat_;
          }
          display(mit->first, merge_stat);
        }
      }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    void SessionStatTask::runTimerTask()
    {
      if (!destroy_)
      {
        AppSessionMap tmp_sessions;
        {
          ScopedRWLock scoped_lock(rw_lock_, WRITE_LOCKER);
          tmp_sessions.swap(app_sessions_);
          assert(app_sessions_.size() == 0);
        }

        vector<SessionBaseInfo> v_session_infos;
        map<string, SessionStat> m_session_stats;
        MIdAppStat m_app_stat;
        //Todo:
        //load app stat log

        AppSessionMapConstIter mit = tmp_sessions.begin();
        for ( ; mit != tmp_sessions.end(); ++mit)
        {
          AppStat app_stat(mit->first);
          SessionCollectMapConstIter sit = mit->second.begin();
          for ( ; sit != mit->second.end(); ++sit)
          {
            v_session_infos.push_back(sit->second.s_base_info_);
            // assign
            m_session_stats[sit->first] = sit->second.s_stat_;
            app_stat.add(sit->second.s_stat_);
          }
          m_app_stat[mit->first] = app_stat;
        }

        //we only care the result of update app stat
        //update session info
        resource_manager_->update_session_info(v_session_infos);
        //update session stat
        resource_manager_->update_session_stat(m_session_stats);
        //update app stat
        if (resource_manager_->update_app_stat(m_app_stat) != TFS_SUCCESS)
        {
          //Todo
          //add log
        }
      }
    }
  }
}
