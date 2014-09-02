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

#include "common/func.h"
#include "common/client_manager.h"
#include "monitor_key_rule_manager.h"
#include "message/rc_session_message.h"

using namespace tfs::common;
using namespace tfs::message;
namespace tfs
{
  namespace requester
  {
    MonitorKeyRuleManager::MonitorKeyRuleManager(const uint64_t http_server, const uint64_t rc_server,
          const std::string& service_type, const std::string& version, const std::string& addr,
          const std::string& url, const std::string& host) :
      keepalive_thread_(0),
      agent_(http_server),
      addr_(addr),
      service_type_(service_type),
      version_(version),
      url_(url),
      host_(host),
      server_(rc_server),
      next_keepalive_time_(0),
      last_update_time_(0),
      destroyed_(false)
    {

    }

    MonitorKeyRuleManager::~MonitorKeyRuleManager()
    {

    }

    void MonitorKeyRuleManager::initialize()
    {
      keepalive_thread_ = new (std::nothrow)KeepAliveThreadHelper(*this);
      assert(0 != keepalive_thread_);
    }

    void MonitorKeyRuleManager::destroy()
    {
      destroyed_ = true;
      if (0 != keepalive_thread_)
      {
        keepalive_thread_->join();
        keepalive_thread_ = 0;
      }
      agent_.destroy();
    }


    bool MonitorKeyRuleManager::check_(const std::string& key)
    {
      bool ret = false;
      const int32_t MAX_INTERVAL = 60;//60s
      ScopedRWLock lock(lock_, READ_LOCKER);
      RULE_ITER iter = rules_.find(key);
      if (rules_.end() != iter)
      {
        MonitorKeyInfo& info = iter->second;
        int64_t now = Func::get_monotonic_time();
        int64_t diff = abs(now - info.last_report_time_);
        ret = diff >= MAX_INTERVAL;
        if (ret)
          info.last_report_time_ = now;
      }
      return ret;
    }

    void MonitorKeyRuleManager::send(const std::string& key, const std::string& msg)
    {
      bool ret = check_(key);
      if (ret)
      {
        const std::string method("POST");
        const std::string header("");
        char buf[256] = {'\0'};
        snprintf(buf, 256, "name=%s&msg=%s", key.c_str(), msg.c_str());
        const std::string body(buf);
        agent_.send(method, host_, url_, header, body);
      }
    }

    void MonitorKeyRuleManager::keepalive()
    {
      while (!destroyed_)
      {
        int64_t now = Func::get_monotonic_time();
        if (now > next_keepalive_time_)
        {
          int32_t retry = 2;
          int32_t ret = TFS_SUCCESS;
          message::ReqRcKeepAliveByMonitorKeyClientMessage req;
          req.set_addr(addr_);
          req.set_service_type(service_type_);
          req.set_version(version_);
          req.set_last_update_time(last_update_time_);
          do
          {
            NewClient* client = NewClientManager::get_instance().create_client();
            ret = (NULL == client) ? EXIT_CLIENT_MANAGER_CREATE_CLIENT_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS == ret)
            {
              tbnet::Packet* ret_msg = NULL;
              ret = send_msg_to_server(server_, client, &req, ret_msg);
              if (TFS_SUCCESS == ret)
              {
                if (RSP_RC_KEEPALIVE_BY_MONITOR_KEY_CLIENT == ret_msg->getPCode())
                {
                  RspRcKeepAliveByMonitorKeyClientMessage* msg = dynamic_cast<RspRcKeepAliveByMonitorKeyClientMessage*>(ret_msg);
                  last_update_time_ = msg->get_last_update_time();
                  next_keepalive_time_ = Func::get_monotonic_time() + msg->get_keepalive_interval();
                  std::vector<MonitorKeyInfo>& keys = msg->get_keys();
                  ScopedRWLock lock(lock_, WRITE_LOCKER);
                  rules_.clear();
                  std::vector<MonitorKeyInfo>::const_iterator iter = keys.begin();
                  for (; iter != keys.end(); ++iter)
                  {
                    rules_.insert(MONITOR_KEY_RULE::value_type((*iter).key_, (*iter)));
                  }
                }
              }
              NewClientManager::get_instance().destroy_client(client);
            }
          }
          while (retry--> 0 && TFS_SUCCESS != ret);
        }
        usleep(1000000);
      }
    }

    void MonitorKeyRuleManager::KeepAliveThreadHelper::run()
    {
      manager_.keepalive();
    }
  }/** end namesapce common **/
}/** end namespace tfs **/

