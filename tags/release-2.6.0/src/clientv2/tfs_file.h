/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_TFSFILE_H_
#define TFS_CLIENTV2_TFSFILE_H_

#include <sys/types.h>
#include <fcntl.h>

#include "common/lock.h"
#include "common/internal.h"
#include "fsname.h"
#include "tfs_session.h"

namespace tfs
{
  namespace clientv2
  {
    enum WriteStatus
    {
      WRITE_STATUS_OK,
      WRITE_STATUS_FAIL
    };

    struct File
    {
      common::FamilyInfoExt family_info_;
      common::VUINT64 ds_;
      uint64_t lease_id_;
      int64_t offset_;
      int32_t version_;
      uint32_t crc_;
      int32_t mode_;
      int32_t opt_flag_;
      int32_t read_index_;
      WriteStatus write_status_;
      CacheHitStatus cache_hit_;

      File();
      ~File();
      bool has_family() const;
      bool check_read();
      bool check_write();
      uint64_t get_write_ds() const;
      int32_t get_read_retry_time() const;
      void set_read_index(const int32_t read_index);
      void set_next_read_index();
      uint64_t get_read_ds() const;
   };

    class TfsFile
    {
      public:
        TfsFile();
        ~TfsFile();

        void transfer_mode(const int32_t mode, const bool create);
        int open(const char* file_name, const char* suffix, const int32_t mode);
        int open(const uint64_t block_id, const uint64_t file_id, const int32_t mode);
        int64_t lseek(const int64_t offset, const int whence);
        int64_t stat(common::TfsFileStat& file_stat);
        int64_t stat_once(common::TfsFileStat& file_stat);
        int64_t read(void* buf, const int64_t count, common::TfsFileStat* file_stat = NULL);
        int64_t read_once(void* buf, const int64_t count, common::TfsFileStat* file_stat = NULL);
        int64_t write(const void* buf, const int64_t count);
        int close(const int32_t status);
        int unlink(const common::TfsUnlinkType action, int64_t& file_size);

        void set_option_flag(const int32_t flag);
        const char* get_file_name();
        void wrap_file_info(common::TfsFileStat& file_stat, const common::FileInfoV2& file_info);
        void set_session(TfsSession* session);
        TfsSession* get_session();
        bool is_cache_hit() const;

      private:
        int open_ex();
        int stat_ex(common::TfsFileStat& file_stat);
        int read_ex(char* buf, const int64_t count, int64_t& read_len,
            common::TfsFileStat* file_stat = NULL);
        int write_ex(const char* buf, int64_t count);
        int close_ex(const int32_t status);
        int unlink_ex(const int32_t action, int64_t& file_size, const bool prepare = false);

      private:
        File file_;
        FSName fsname_;
        common::RWLock rw_lock_;
        TfsSession* session_;
    };
  }
}

#endif
