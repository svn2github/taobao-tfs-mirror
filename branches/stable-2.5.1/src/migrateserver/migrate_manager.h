/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: migrate_manager.h 746 2013-08-28 07:27:59Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#ifndef TFS_MIGRATESERVER_MIGRATE_MANAGER_H_
#define TFS_MIGRATESERVER_MIGRATE_MANAGER_H_

#include <TbThread.h>
#include <Mutex.h>
#include "common/internal.h"
#include "ms_define.h"

namespace tfs
{
  namespace migrateserver
  {
    class MigrateManager
    {
      struct MigrateEntry
      {
        uint64_t block_id_;
        uint64_t source_addr_;
        uint64_t dest_addr_;
        int64_t  last_update_time_;
      };

      class WorkThreadHelper : public tbutil::Thread
      {
        public:
          explicit WorkThreadHelper(MigrateManager& manager):
            manager_(manager)
        {
          start();
        }
          virtual ~WorkThreadHelper() {}
          void run() { manager_.run_();}
        private:
          DISALLOW_COPY_AND_ASSIGN(WorkThreadHelper);
          MigrateManager& manager_;
      };
      typedef tbutil::Handle<WorkThreadHelper> WorkThreadHelperPtr;
      typedef std::multimap<int64_t, std::pair<uint64_t, uint64_t> > BLOCK_MAP;
      typedef BLOCK_MAP::iterator BLOCK_MAP_ITER;
      typedef std::map<uint64_t, common::DataServerStatInfo> SERVER_MAP;
      typedef SERVER_MAP::iterator SERVER_MAP_ITER;
      typedef SERVER_MAP::const_iterator CONST_SERVER_MAP_ITER;
      public:
      explicit MigrateManager();
      virtual ~MigrateManager();
      int initialize();
      int destroy();
      int keepalive(const common::DataServerStatInfo& server);
      void timeout(const int64_t now);
      private:
      void run_();
      int get_index_header_(const uint64_t server, const int32_t type);
      inline bool is_full(const common::BlockInfoV2& info) const
      {
        return ((info.size_ + common::BLOCK_RESERVER_LENGTH) >= max_block_size_
          || info.file_count_ >= common::MAX_SINGLE_BLOCK_FILE_COUNT);
      }
      inline int64_t calc_block_weight_(const common::IndexHeaderV2& iheader, const int32_t type);
      void get_all_servers_(common::ArrayHelper<std::pair<uint64_t, int32_t> >& servers) const;

      void calc_system_disk_migrate_info_(MigrateEntry& entry) const;
      int choose_migrate_entry_(MigrateEntry& output);
      int do_migrate_(MigrateEntry& current);
      bool choose_move_dest_server_(const uint64_t source_addr, uint64_t& dest_addr) const;
      void statistic_all_server_info_(int64_t& total_capacity, int64_t& use_capacity) const;
      void get_ns_config_parameter_();
      private:
      tbutil::Mutex mutex_;
      SERVER_MAP servers_;
      BLOCK_MAP  blocks_[2];// 0-system disk, 1-full disk
      WorkThreadHelperPtr work_thread_;
      uint64_t ns_vip_port_;
      int64_t not_full_block_count_; // not full block need to migrate back for reuse
      double balance_percent_;
      double penalty_percent_;
      int64_t hot_time_range_;
      int32_t max_block_size_;
      int32_t migrate_complete_wait_time_;
      int64_t update_statistic_interval_;
      common::AccessRatio full_disk_access_ratio_;
      common::AccessRatio system_disk_access_ratio_;
      bool need_migrate_back_;
    };
  }/** end namespace syncserver **/
}/** end namesapce tfs **/
#endif //TFS_MIGRATESERVER_MIGRATE_MANAGER_H_
