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
#ifndef TFS_SYNCSERVER_SYNC_MANAGER_H_
#define TFS_SYNCSERVER_SYNC_MANAGER_H_

#include <TbThread.h>
#include <Mutex.h>
#include "common/internal.h"
#include "ss_define.h"

namespace tfs
{
  namespace syncserver
  {
    class SyncManager
    {
      static const int32_t THREAD_BUFFER_SIZE    = 2 * 1024 * 1024;
      typedef std::list<common::SyncFileEntry> QUEUE;
      typedef QUEUE::iterator QUEUE_ITER;
      typedef QUEUE::const_iterator QUEUE_CONST_ITER;
      typedef int (*sync_func)(char* buf, const int32_t length, const common::SyncFileEntry& entry);
      class WorkThreadHelperManager;
      class WorkThreadHelper : public tbutil::Thread
      {
      public:
        WorkThreadHelper(WorkThreadHelperManager& manager, sync_func func, const uint64_t dest_ns_addr, const int32_t limit, const int32_t queue_warn_limit):
          manager_(manager),
          buffer_(new (std::nothrow)char[THREAD_BUFFER_SIZE]),
          func_(func),
          dest_ns_addr_(dest_ns_addr),
          last_warn_time_(0),
          last_out_limit_time_(0),
          queue_size_(0),
          queue_limit_(limit),
          queue_warn_limit_(queue_warn_limit)
        {
          start();
        }
        virtual ~WorkThreadHelper() { tbsys::gDeleteA(buffer_);}
        int insert(const common::SyncFileEntry& entry);
        void run();
        inline uint64_t get_dest_ns_addr() const { return dest_ns_addr_;}
      private:
        bool out_of_limit_() const;
        void warn_(const time_t now);
        int do_sync_(common::SyncFileEntry& entry);

      private:
        DISALLOW_COPY_AND_ASSIGN(WorkThreadHelper);
        WorkThreadHelperManager& manager_;
        QUEUE queue_;
        tbutil::Mutex mutex_;
        char*   buffer_;
        sync_func func_;
        uint64_t dest_ns_addr_;
        int64_t last_warn_time_;
        int64_t last_out_limit_time_;
        int32_t queue_size_;
        int32_t queue_limit_;
        int32_t queue_warn_limit_;
      };
      typedef tbutil::Handle<WorkThreadHelper> WorkThreadHelperPtr;
      class WorkThreadHelperManager
      {
      public:
        WorkThreadHelperManager(SyncManager& manager, const uint64_t source_ds_addr) :
          manager_(manager),
          source_ds_addr_(source_ds_addr),
          work_thread_count_(0) {}
        virtual ~WorkThreadHelperManager() {}

        int initialize(const common::ArrayHelper<std::pair<uint64_t, sync_func> >& sync_mirror_info,
            const int32_t queue_limit, const int32_t queue_warn_limit);
        int destroy();
        int insert(common::SyncFileEntry& entry);
        inline void set_source_ds_addr(const uint64_t addr) { source_ds_addr_ = addr;}
        inline uint64_t get_source_ds_addr() const { return source_ds_addr_;}
      private:
        SyncManager& manager_;
        WorkThreadHelperPtr work_thread_[MAX_SYNC_CLUSTER_SIZE];
        uint64_t source_ds_addr_;
        int32_t  work_thread_count_;
      };
    public:
      SyncManager(const int32_t limit, const float warn_ratio);
      virtual ~SyncManager();
      int initialize(const common::ArrayHelper<std::pair<uint64_t, int32_t> >& sync_mirror_info);
      int destroy();
      int insert(common::SyncFileEntry& entry);
    private:
      static int do_sync_whith_client_v0(char* buf, const int32_t length, const common::SyncFileEntry& entry);
      static int do_sync_whith_client_v1(char* buf, const int32_t length, const common::SyncFileEntry& entry);
    private:
      static const int32_t MAX_WORK_THREAD_SIZE = 12;
      WorkThreadHelperManager* work_thread_manager_[MAX_WORK_THREAD_SIZE];
      tbutil::Mutex mutex_;
      int32_t queue_limit_;
      int32_t queue_warn_limit_;
    };
  }/** end namespace syncserver **/
}/** end namesapce tfs **/
#endif //TFS_SYNCSERVER_SYNC_MANAGER_H_
