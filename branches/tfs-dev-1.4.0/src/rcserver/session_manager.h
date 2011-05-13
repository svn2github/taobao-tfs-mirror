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
#include <time.h>
#include <Timer.h>
#include <Shared.h>
#include <Handle.h>
#include "common/define.h"
#include "common/lock.h"
#include "i_resource_manager.h"
#include "resource_server_data.h"

namespace tfs
{
  namespace rcserver
  {
    static const char SEPARATOR_KEY = '-';

    enum UpdateFlag
    {
      LOGIN_FLAG = 1,
      KA_FLAG,
      LOGOUT_FLAG
    };

    class SessionManager;
    class ISessionTask : public tbutil::TimerTask
    {
      public:
        explicit ISessionTask(SessionManager& manager)
          : manager_(manager), destroy_(false)
        {
          key_map_[OPER_INVALID] = "invalid";
          key_map_[OPER_READ] = "read";
          key_map_[OPER_WRITE] = "write";
          key_map_[OPER_UNIQUE_WRITE] = "unique_write";
          key_map_[OPER_UNLINK] = "unlink";
          key_map_[OPER_UNIQUE_UNLINK] = "unique_unlink";
        }

        virtual ~ISessionTask()
        {
        }

        virtual void runTimerTask() = 0;
        void destroy()
        {
          destroy_ = true;
        }
        int update_session_info(const int32_t app_id, const std::string& session_id,
            const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag);

      protected:
        static void display(const int32_t app_id, const SessionStat& s_stat);

      protected:
        SessionManager& manager_;
        bool destroy_;
        AppSessionMap app_sessions_;
        common::RWLock rw_lock_;
        static std::map<OperType, std::string> key_map_;
    };

    class SessionMonitorTask : public ISessionTask
    {
      public:
        explicit SessionMonitorTask(SessionManager& manager)
          : ISessionTask(manager)
        {
        }

        virtual ~SessionMonitorTask()
        {
        }

        virtual void runTimerTask();

      private:
        IResourceManager* resource_manager_;
    };
    typedef tbutil::Handle<SessionMonitorTask> SessionMonitorTaskPtr;

    class SessionStatTask : public ISessionTask
    {
      public:
        SessionStatTask(SessionManager& manager, IResourceManager* resource_manager)
          : ISessionTask(manager), resource_manager_(resource_manager)
        {
        }
        virtual ~SessionStatTask()
        {
        }

        virtual void runTimerTask();
      private:
        IResourceManager* resource_manager_;
    };
    typedef tbutil::Handle<SessionStatTask> SessionStatTaskPtr;
    class SessionManager
    {
      public:
        SessionManager(IResourceManager* resource_manager, tbutil::TimerPtr timer)
          : resource_manager_(resource_manager), timer_(timer),
            monitor_task_(0), stat_task_(0)
        {
        }
        ~SessionManager()
        {
        }

      public:
        int initialize(const bool reload_flag = false);
        int wait_for_shut_down();
        void destroy();

        int login(const std::string& app_key, const int64_t session_ip,
            std::string& session_id, BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info,
            bool& update_flag, BaseInfo& base_info);
        int logout(const std::string& session_id, const KeepAliveInfo& keep_alive_info);

      private:
        int update_session_info(const int32_t app_id, const std::string& session_id,
            const KeepAliveInfo& keep_alive_info, UpdateFlag update_flag);

        void gene_session_id(const int32_t app_id, const int64_t session_ip, std::string& session_id);
        int parse_session_id(const std::string& session_id, int32_t& app_id, int64_t& session_ip);

      private:
        DISALLOW_COPY_AND_ASSIGN(SessionManager);

        IResourceManager* resource_manager_;
        tbutil::TimerPtr timer_;

        SessionMonitorTaskPtr monitor_task_;
        SessionStatTaskPtr stat_task_;

        bool is_init_;
        tbutil::Mutex mutex_;
    };

  }
}
#endif //TFS_RCSERVER_SESSIONMANAGER_H_
