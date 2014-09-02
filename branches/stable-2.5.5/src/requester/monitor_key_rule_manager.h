/*
* (C) 2007-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   duanfei <duanfei@taobao.com>
*      - initial release
*
*/

#ifndef TFS_REQUESTER_MONITOR_KEY_RULE_MANAGER_H_
#define TFS_REQUESTER_MONITOR_KEY_RULE_MANAGER_H_

#include <tbsys.h>
#include <TbThread.h>

#include "common/lock.h"
#include "common/internal.h"
#include "common/rc_define.h"
#include "common/http_agent.h"


namespace tfs
{
  namespace requester
  {
    class MonitorKeyRuleManager
    {
      typedef std::map<std::string, common::MonitorKeyInfo> MONITOR_KEY_RULE;
      typedef MONITOR_KEY_RULE::const_iterator CONST_RULE_ITER;
      typedef MONITOR_KEY_RULE::iterator RULE_ITER;
      public:
        MonitorKeyRuleManager(const uint64_t http_server, const uint64_t rc_server,
          const std::string& service_type, const std::string& version, const std::string& addr,
          const std::string& url, const std::string& host);
        virtual ~MonitorKeyRuleManager();

        void initialize();
        void destroy();

        void send(const std::string& key , const std::string& msg);

      private:
        void keepalive();
        bool check_(const std::string& key);

      private:
      class KeepAliveThreadHelper: public tbutil::Thread
      {
        public:
          explicit KeepAliveThreadHelper(MonitorKeyRuleManager& manager):
            manager_(manager) {start();}
          virtual ~KeepAliveThreadHelper() {}
          void run();
        private:
          MonitorKeyRuleManager& manager_;
          DISALLOW_COPY_AND_ASSIGN(KeepAliveThreadHelper);
      };
      typedef tbutil::Handle<KeepAliveThreadHelper> KeepAliveThreadHelperPtr;

      private:
        DISALLOW_COPY_AND_ASSIGN(MonitorKeyRuleManager);
        MONITOR_KEY_RULE rules_;
        KeepAliveThreadHelperPtr keepalive_thread_;
        common::HttpAgent agent_;
        std::string addr_;
        std::string service_type_;
        std::string version_;
        std::string url_;
        std::string host_;
        uint64_t server_;
        int64_t  next_keepalive_time_;
        int64_t  last_update_time_;
        common::RWLock lock_;
        bool destroyed_;
    };
  }/** end namespace commond **/
}/** end namesapce tfs **/

#endif
