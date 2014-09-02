/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_manager.h 746 2013-09-02 11:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_DATASERVER_MIGRATE_MANAGER_H_
#define TFS_DATASERVER_MIGRATE_MANAGER_H_

#include <TbThread.h>
#include <Mutex.h>
#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    class MigrateManager
    {
    public:
      MigrateManager(const uint64_t migrate_dest_addr, const uint64_t self_addr);
      virtual ~MigrateManager();
      int initialize();
      int destroy();
    private:
      void run_();
      int do_migrate_heartbeat_(const int32_t timeout_ms);
      class WorkThreadHelper: public tbutil::Thread
      {
        public:
          explicit WorkThreadHelper(MigrateManager& manager):
            manager_(manager)
        {
          start();
        }
          virtual ~WorkThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(WorkThreadHelper);
          MigrateManager& manager_;
      };
      typedef tbutil::Handle<WorkThreadHelper> WorkThreadHelperPtr;
    private:
      WorkThreadHelperPtr work_thread_;
      uint64_t dest_addr_;
      uint64_t self_addr_;
    };
  }/** end namespace dataserver **/
}/** end namesapce tfs **/
#endif //TFS_DATASERVER_MIGRATE_MANAGER_H_
