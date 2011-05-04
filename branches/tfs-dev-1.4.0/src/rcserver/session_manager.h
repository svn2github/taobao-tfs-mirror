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
#include <set>
#include <string>
#include "common/define.h"

namespace tfs
{
  namespace rcserver
  {
    struct SessionBaseInfo
    {
      std::string session_id_;
      uint64_t client_version_;
      uint64_t cache_size_;
      uint64_t modify_time_;
      // need extend?
    };

    enum OperType 
    {
      OPER_READ = 1,
      OPER_WRITE,
      OPER_UNIQUE_WRITE,
      OPER_UNLINK,
      OPER_UNIQUE_UNLINK
    };

    struct AppOperInfo
    {
      OperType oper_type_;
      uint64_t oper_times_;  //total
      uint64_t oper_size_;   //succ
      uint64_t oper_rt_;     //succ 累加值
      uint64_t oper_succ_;
    };

    struct SessionStat
    {
      std::set<AppOperInfo> app_oper_info_;
      uint64_t cache_hit_ratio;
    };

    struct KeepAliveInfo
    {
      SessionBaseInfo s_base_info_;
      SessionStat s_stat_;
      // implement operator +
    };

    class SessionManager
    {
      public:
        SessionManager(ResourceManager* resource_manager) : resource_manager_(resource_manager)
        {
        }
        ~SessionManager()
        {
        }

      public:
        int login(const std::string& app_key, const uint64_t session_ip,
            std::string& session_id, BaseInfo& base_info);
        int keep_alive(const std::string& session_id, const KeepAliveInfo& keep_alive_info);
        int logout(const std::string& session_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(SessionManager);

        typedef std::map<std::string, KeepAliveInfo> SessionCollectMap;
        typedef SessionCollectMap::const_iterator SessionCollectMapConstIter;
        typedef SessionCollectMap::iterator SessionCollectMapIter;
        SessionCollectMap session_infos_;
        ResourceManager* resource_manager_;
    };
  }
}
#endif //TFS_RCSERVER_SESSIONMANAGER_H_
