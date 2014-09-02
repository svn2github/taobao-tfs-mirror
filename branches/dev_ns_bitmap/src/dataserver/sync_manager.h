/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_manager.h 746 2013-08-28 07:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_DATASERVER_SYNC_MANAGER_H_
#define TFS_DATASERVER_SYNC_MANAGER_H_

#include <TbThread.h>
#include <Mutex.h>
#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    class SyncManager
    {
      typedef std::list<common::SyncFileEntry> QUEUE;
      typedef QUEUE::iterator QUEUE_ITER;
      typedef QUEUE::const_iterator QUEUE_CONST_ITER;
    public:
      SyncManager(const uint64_t sync_dest_addr, const uint64_t self_addr, const uint64_t self_ns_addr, const int32_t limit, const float warn_ratio);
      virtual ~SyncManager();
      int initialize();
      int destroy();
      int insert(const uint64_t dest_ns_addr, const int64_t app_id, const uint64_t block_id, const uint64_t file_id, const int32_t type);
    private:
      void do_sync_(const int32_t timeout_ms, const bool print);
      bool out_of_limit_() const;
      void warn_(const time_t now);
      void run_();
      class WorkThreadHelper: public tbutil::Thread
      {
        public:
          explicit WorkThreadHelper(SyncManager& manager):
            manager_(manager)
        {
          start();
        }
          virtual ~WorkThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(WorkThreadHelper);
          SyncManager& manager_;
      };
      typedef tbutil::Handle<WorkThreadHelper> WorkThreadHelperPtr;
    private:
      tbutil::Mutex mutex_;
      WorkThreadHelperPtr work_thread_;
      QUEUE queue_;
      uint64_t dest_addr_;
      uint64_t self_addr_;
      uint64_t self_ns_addr_;
      int64_t last_warn_time_;
      int64_t last_out_limit_time_;
      int32_t queue_size_;
      int32_t queue_limit_;
      int32_t queue_warn_limit_;
    };
  }/** end namespace dataserver **/
}/** end namesapce tfs **/
#endif //TFS_DATASERVER_SYNC_MANAGER_H_
