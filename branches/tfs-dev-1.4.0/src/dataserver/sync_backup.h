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
 *   zongdai <zongdai@taobao.com> 
 *      - modify 2010-04-23
 *
 */
#ifndef TFS_DATASERVER_SYNCBACKUP_H_
#define TFS_DATASERVER_SYNCBACKUP_H_

#include "common/internal.h"
#include "new_client/tfs_client_api.h"
#include <Memory.hpp>

namespace tfs
{
  namespace dataserver
  {
    enum SyncType
    {
      SYNC_TO_TFS_MIRROR = 1,
      SYNC_TO_NFS_MIRROR
    };

    struct SyncData
    {
      int32_t cmd_;
      uint32_t block_id_;
      uint64_t file_id_;
      uint64_t old_file_id_;
      int32_t retry_count_;
      int32_t retry_time_;
    };

    class SyncBackup
    {
      public:
        SyncBackup();
        virtual ~SyncBackup()
        {
        }

        virtual bool init() = 0;
        virtual int do_sync(const SyncData* sf);
        virtual int do_second_sync(const SyncData* sf);
        virtual int copy_file(const uint32_t block_id, const uint64_t file_id);
        virtual int remove_file(const uint32_t block_id, const uint64_t file_id, const int32_t undel);
        virtual int rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id);
        virtual int remote_copy_file(const uint32_t block_id, const uint64_t file_id);

      protected:
        DISALLOW_COPY_AND_ASSIGN(SyncBackup);

        static const int32_t TMP_PATH_SIZE = 256;
        static const int32_t BLOCK_DIR_NUM = 100;
    };

    class NfsMirrorBackup: public SyncBackup
    {
      public:
        NfsMirrorBackup() :
          SyncBackup()
        {
        }
        virtual ~NfsMirrorBackup()
        {
        }

      public:
        virtual bool init();
        virtual int copy_file(const uint32_t block_id, const uint64_t file_id);
        virtual int remove_file(const uint32_t block_id, const uint64_t file_id, const int32_t undel);
        virtual int rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id);
        virtual int remote_copy_file(const uint32_t block_id, const uint64_t file_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(NfsMirrorBackup);

        char backup_path_[TMP_PATH_SIZE];
        char remove_path_[TMP_PATH_SIZE];
        client::TfsClient* source_client_;

        static int write_n(const int32_t fd, const char* buffer, const int32_t length);
        static int mk_dir(const char* path);
        static int mk_dirs(const char* path);
        static void get_backup_path(char* buf, const char* path, const uint32_t block_id);
        static void get_backup_file_name(char* buf, const char* path, const uint32_t block_id, const uint64_t file_id);
        static int move(const char* source, const char* dest);
    };

    class TfsMirrorBackup: public SyncBackup
    {
      public:
        TfsMirrorBackup() :
          SyncBackup()
        {
          tfs_client_ = NULL;
          second_tfs_client_ = NULL;
          source_client_ = NULL;
        }
        virtual ~TfsMirrorBackup()
        {
          tbsys::gDelete(tfs_client_);
          tbsys::gDelete(second_tfs_client_);
          tbsys::gDelete(source_client_);
        }

        virtual bool init();
        virtual int do_sync(const SyncData* sf);
        virtual int do_second_sync(const SyncData* sf);

      private:
        DISALLOW_COPY_AND_ASSIGN(TfsMirrorBackup);

        client::TfsClient* tfs_client_;
        client::TfsClient* second_tfs_client_;
        client::TfsClient* source_client_;

      private:
        int do_sync(client::TfsClient* tfs_client, const SyncData* sf);
        int copy_file(client::TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id);
        int remove_file(client::TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id, const int32_t undel);
        int rename_file(client::TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id, const uint64_t old_file_id);
        int remote_copy_file(client::TfsClient* tfs_client, const uint32_t block_id, const uint64_t file_id);
    };

  }
}

#endif //TFS_DATASERVER_SYNCBACKUP_H_
