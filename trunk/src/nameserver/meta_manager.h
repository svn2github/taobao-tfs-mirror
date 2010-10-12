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
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com> 
 *      - modify 2009-03-27
 *   duanfei <duanfei@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_NAMESERVER_META_MANAGEMENT_H_
#define TFS_NAMESERVER_META_MANAGEMENT_H_

#include "layout_manager.h"
#include "lease_clerk.h"
#include "oplog_sync_manager.h"

namespace tfs
{
  namespace nameserver
  {
    class NameServer;
    class MetaManager
    {
    public:
      MetaManager(NameServer* fsns);
      virtual ~MetaManager();

      typedef std::map<uint64_t, std::vector<uint32_t> > EXPIRE_BLOCK_LIST;

      inline const LayoutManager& get_block_ds_mgr() const
      {
        return meta_mgr_;
      }
      inline LayoutManager& get_block_ds_mgr()
      {
        return meta_mgr_;
      }
      inline const LeaseClerk& get_lease_clerk() const
      {
        return lease_mgr_;
      }
      inline LeaseClerk& get_lease_clerk()
      {
        return lease_mgr_;
      }

    public:
      // handle ds
      int join_ds(const common::DataServerStatInfo& ds_stat_info, bool& isnew);
      int leave_ds(const uint64_t ds_id);

      // handle blocks
      int report_blocks(const uint64_t ds_id, const std::vector<common::BlockInfo>& blocks, EXPIRE_BLOCK_LIST& expires);

      BlockCollect* add_new_block(uint32_t& block_id, const uint64_t ds_id = 0);

      int read_block_info(const uint32_t block_id, common::VUINT64& ds_list);
      int write_block_info(uint32_t& block_id, int32_t mode, uint32_t& lease_id, int32_t& version,
          common::VUINT64& ds_list);
      int update_block_info(const common::BlockInfo& block_info, const uint64_t ds_id, bool addnew);
      int write_commit(const common::BlockInfo& block_info, const uint64_t ds_id, const uint32_t lease_id,
              const common::UnlinkFlag unlink_flag, const common::WriteCompleteStatus status, bool& neednew,
              string& errmsg);

      int initialize(const int32_t chunk_num);
      int save();
      int checkpoint();

      int check_primary_writable_block(const uint64_t ds_id, const int32_t add_block_count, bool promote = false);
      int promote_primary_write_block(const ServerCollect* srvcol, int32_t& need);

      // sync log
      const OpLogSyncManager* get_oplog_sync_mgr() const
      {
        return &oplog_sync_mgr_;
      }
      OpLogSyncManager* get_oplog_sync_mgr()
      {
        return &oplog_sync_mgr_;
      }

      NameServer* get_fs_name_system()
      {
        return fs_name_system_;
      }

      int wait_for_shut_down();
      int destroy();
    private:
      DISALLOW_COPY_AND_ASSIGN( MetaManager);
      uint32_t elect_write_block(const common::VINT64& fail_ds);
      uint32_t register_expire_block(EXPIRE_BLOCK_LIST & expireList, const uint64_t ds_id, const uint32_t block_id);

      NameServer* fs_name_system_;
      LayoutManager meta_mgr_;
      LeaseClerk lease_mgr_;
      OpLogSyncManager oplog_sync_mgr_;

      time_t zonesec_;
      time_t last_rotate_log_time_;
      int32_t current_writing_index_;
    };

  }
}
#endif 
