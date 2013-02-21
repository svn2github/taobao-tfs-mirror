/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: heart_manager.h 983 2013-01-15 09:59:33Z duanfei $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_HEART_MANAGER_H_
#define TFS_DATASERVER_HEART_MANAGER_H_

#include <vector>
#include <Timer.h>
#include "message/message_factory.h"

namespace tfs
{
  namespace dataserver
  {
    class DataService;
    class DataServerHeartManager
    {
    public:
      DataServerHeartManager(DataService& service, const std::vector<uint64_t>& ns_ip_port);
      virtual ~DataServerHeartManager();
      int initialize();
      void wait_for_shut_down();
    private:
      class HeartBeatThreadHelper : public tbutil::Thread
      {
        public:
          HeartBeatThreadHelper(DataServerHeartManager& manager, int8_t who): manager_(manager),who_(who) { start();}
          virtual ~HeartBeatThreadHelper() {}
          void run();
        private:
          DataServerHeartManager& manager_;
          int8_t who_;
          DISALLOW_COPY_AND_ASSIGN(HeartBeatThreadHelper);
      };
      typedef tbutil::Handle<HeartBeatThreadHelper> HeartBeatThreadHelperPtr;
    private:
      DISALLOW_COPY_AND_ASSIGN(DataServerHeartManager);
      void run_(const int32_t who);
      int keepalive(int8_t& heart_interval, const int32_t who, const int64_t timeout);
      uint64_t ns_ip_port_[common::MAX_SINGLE_CLUSTER_NS_NUM];
      HeartBeatThreadHelperPtr heart_beat_thread_[common::MAX_SINGLE_CLUSTER_NS_NUM];
      DataService& service_;
    };
  }/** end namespace dataserver**/
}/** end namespace tfs **/
#endif

