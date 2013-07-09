/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: sync_backup.h 746 2011-09-06 07:27:59Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2013-06-03
 *
 */
#ifndef TFS_DATASERVER_SYNCBACKUP_H_
#define TFS_DATASERVER_SYNCBACKUP_H_

#include "common/internal.h"
#include "clientv2/tfs_client_impl_v2.h"
#include <Memory.hpp>
#include <TbThread.h>
#include "block_manager.h"

namespace tfs
{
  namespace dataserver
  {
    enum SyncType
    {
      SYNC_TO_TFS_MIRROR = 1,
    };

    struct SyncData
    {
      int32_t cmd_;
      uint64_t block_id_;
      uint64_t file_id_;
      uint64_t old_file_id_;
      uint32_t retry_time_;
      int32_t reserve_[6];  // reserve 24 bytes for extention later
    };

    class SyncBase;
    class SyncBackup
    {
    public:
      SyncBackup();
      virtual ~SyncBackup();

      virtual bool init() = 0;
      virtual void destroy() = 0;
      virtual int do_sync(const SyncData* sf);
      virtual int copy_file(const uint64_t block_id, const uint64_t file_id);
      virtual int remove_file(const uint64_t block_id, const uint64_t file_id, const int32_t undel);
      virtual int rename_file(const uint64_t block_id, const uint64_t file_id, const uint64_t old_file_id);
      virtual int remote_copy_file(const uint64_t block_id, const uint64_t file_id);

    protected:
      DISALLOW_COPY_AND_ASSIGN(SyncBackup);
      clientv2::TfsClientImplV2* tfs_client_;
      bool client_init_flag_;

      char src_addr_[common::MAX_SYNC_IPADDR_LENGTH];
      char dest_addr_[common::MAX_SYNC_IPADDR_LENGTH];
    };

    class TfsMirrorBackup : public SyncBackup
    {
      public:
        TfsMirrorBackup(SyncBase& sync_base, const char* src_addr, const char* dest_addr);
        virtual ~TfsMirrorBackup();

        BlockManager& get_block_manager();

        bool init();
        void destroy();
        int do_sync(const SyncData* sf);

      private:
        DISALLOW_COPY_AND_ASSIGN(TfsMirrorBackup);

      private:
        int copy_file(const uint64_t block_id, const uint64_t file_id);
        int remove_file(const uint64_t block_id, const uint64_t file_id, const common::TfsUnlinkType action);
        int remote_copy_file(const uint64_t block_id, const uint64_t file_id);
        int remote_stat_file(const char* ns_addr,
            const uint64_t block_id, const uint64_t file_id, common::FileInfoV2& finfo);
        int stat_file(const uint64_t block_id, const uint64_t file_id, common::FileInfoV2& finfo);

        // check return value to see if file exist in dest cluster
        bool file_not_exist(const int ret);

      class DoSyncMirrorThreadHelper: public tbutil::Thread
      {
        public:
          explicit DoSyncMirrorThreadHelper(SyncBase& sync_base):
              sync_base_(sync_base)
          {
            start();
          }
          virtual ~DoSyncMirrorThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(DoSyncMirrorThreadHelper);
          SyncBase& sync_base_;
      };
      typedef tbutil::Handle<DoSyncMirrorThreadHelper> DoSyncMirrorThreadHelperPtr;

      class DoFailSyncMirrorThreadHelper: public tbutil::Thread
      {
        public:
          explicit DoFailSyncMirrorThreadHelper(SyncBase& sync_base):
              sync_base_(sync_base)
          {
            start();
          }
          virtual ~DoFailSyncMirrorThreadHelper(){}
          void run();
        private:
          DISALLOW_COPY_AND_ASSIGN(DoFailSyncMirrorThreadHelper);
          SyncBase& sync_base_;
      };
      typedef tbutil::Handle<DoFailSyncMirrorThreadHelper> DoFailSyncMirrorThreadHelperPtr;

    private:
      SyncBase& sync_base_;
      DoSyncMirrorThreadHelperPtr  do_sync_mirror_thread_;
      DoFailSyncMirrorThreadHelperPtr  do_fail_sync_mirror_thread_;

    };

  }
}

#endif //TFS_DATASERVER_SYNCBACKUP_H_
