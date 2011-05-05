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
#ifndef TFS_RCSERVER_SESSIONMANAGER_H_
#define TFS_RCSERVER_SESSIONMANAGER_H_

#include <map>
#include <string>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/define.h"
#include "i_resource_manager.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    static const char SEPARATOR_KEY = '-';
    enum StatFlag
    {
      STAT_DB = 1,
      STAT_MONITOR
    };

    enum UpdateFlag
    {
      LOGIN_FLAG = 1,
      KA_FLAG,
      LOGOUT_FLAG
    };

    struct KeepAliveInfo
    {
      KeepAliveInfo()
      {
        //init to 0
      }
      SessionBaseInfo s_base_info_;
      SessionStat s_stat_;
      uint64_t last_report_time_;

      KeepAliveInfo& operator +=(const KeepAliveInfo& right)
      {
        return *this;
      }
    };

    class SessionManager
    {
      public:
        SessionManager(IResourceManager* resource_manager, tbutil::TimerPtr timer)
          : resource_manager_(resource_manager), timer_(timer), destroy_(false),
            stat_db_task_(0), stat_monitor_task_(0), expire_task_(0)
        {
        }
        ~SessionManager()
        {
        }

      public:
        int initialize();
        int wait_for_shut_down();
        void destroy();

        int login(const std::string& app_key, const int64_t session_ip,
            std::string& session_id, BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info, BaseInfo& base_info);
        int logout(const std::string& session_id, const KeepAliveInfo& keep_alive_info);

        // write session info to data source
        int dump(const StatFlag stat_flag);
        // expire session who not report for a expire time interval
        int expire_session();

      private:
        int update_session_info(const int32_t app_id, const std::string& session_id,
            const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag);
        int dump_to_db();
        int dump_to_monitor();

        void gene_session_id(const int32_t app_id, const int64_t session_ip, std::string& session_id);
        int parse_session_id(const std::string& session_id, int32_t& app_id, int64_t& session_ip);

      private:
        class SessionStatTask : public tbutil::TimerTask
        {
          public:
            SessionStatTask(SessionManager& manager, const StatFlag stat_flag)
              : manager_(manager), stat_flag_(stat_flag)
            {
            }
            virtual ~SessionStatTask()
            {
            }

            virtual void runTimerTask();
          private:
            SessionManager& manager_;
            StatFlag stat_flag_;
        };
        typedef tbutil::Handle<SessionStatTask> SessionStatTaskPtr;

        class SessionExpireTask : public tbutil::TimerTask
        {
          public:
            SessionExpireTask(SessionManager& manager)
              : manager_(manager)
            {
            }
            virtual ~SessionExpireTask()
            {
            }

            virtual void runTimerTask();
          private:
            SessionManager& manager_;
        };
        typedef tbutil::Handle<SessionExpireTask> SessionExpireTaskPtr;

      private:
        DISALLOW_COPY_AND_ASSIGN(SessionManager);

        typedef std::map<std::string, KeepAliveInfo> SessionCollectMap;
        typedef SessionCollectMap::const_iterator SessionCollectMapConstIter;
        typedef SessionCollectMap::iterator SessionCollectMapIter;

        typedef std::map<int32_t, SessionCollectMap> AppSessionMap;
        typedef AppSessionMap::iterator AppSessionMapIter;

        AppSessionMap app_sessions_;
        IResourceManager* resource_manager_;

        tbutil::TimerPtr timer_;
        bool destroy_;

        SessionStatTaskPtr stat_db_task_;
        SessionStatTaskPtr stat_monitor_task_;
        SessionExpireTaskPtr expire_task_;

    };
  }
}
#endif //TFS_RCSERVER_SESSIONMANAGER_H_
