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

#include "common/internal.h"
#include "fsname.h"

namespace tfs
{
  namespace clientv2
  {
    enum FileStatus
    {
      TFS_FILE_OPEN_NO = 0,
      TFS_FILE_OPEN_YES,
      TFS_FILE_WRITE_ERROR
    };

    struct File
    {
      uint64_t lease_id_;
      common::VUINT64 ds_;
      int64_t offset_;
      int32_t version_;
      uint32_t crc_;
      int32_t mode_;
      int32_t opt_flag_;
      FileStatus status_;
      int32_t read_index_;
      common::FamilyInfoExt family_info_;

      File()
      {
        memset(this, 0, sizeof(*this));
      }

      ~File()
      {
      }

      bool is_valid() const
      {
        return has_ds() || has_family();
      }

      bool has_ds() const
      {
        return ds_.size() > 0;
      }

      bool has_family() const
      {
        return common::INVALID_FAMILY_ID != family_info_.family_id_;
      }

      int32_t get_retry_time() const
      {
        return ds_.size() > 0 ? ds_.size() : 1;
      }

      uint64_t get_write_ds()
      {
        uint64_t server_id = 0;
        if (ds_.size() > 0)
        {
          server_id = ds_[0];
        }
        return server_id;
      }

      uint64_t get_read_ds()
      {
        uint64_t server_id = 0;
        if (ds_.size() > 0)
        {
          read_index_ %= ds_.size();
          server_id = ds_[read_index_];
        }
        else if(common::INVALID_FAMILY_ID != family_info_.family_id_)
        {
          int32_t index = GET_MASTER_INDEX(family_info_.family_aid_info_);
          server_id = family_info_.members_[index].first;
        }
        return server_id;
      }

      void set_next_read_ds()
      {
        read_index_++;
      }
    };

    class TfsFile
    {
      public:
        TfsFile(const uint64_t ns_addr, int32_t cluster_id);
        ~TfsFile();

        int open(const char* file_name, const char* suffix, const int32_t mode);
        int64_t lseek(const int64_t offset, const int whence);
        int64_t stat(common::TfsFileStat& file_stat);
        int64_t read(void* buf, const int64_t count);
        int64_t write(const void* buf, const int64_t count);
        int close();
        int unlink(const common::TfsUnlinkType action, int64_t& file_size);

        void set_option_flag(const int32_t flag);
        const char* get_file_name();
        void wrap_file_info(common::TfsFileStat& file_stat, const common::FileInfoV2& file_info);

      private:
        int do_open();
        int do_stat(common::TfsFileStat& file_stat);
        int do_read(char* buf, const int64_t count, int64_t& read_len);
        int do_write(const char* buf, int64_t count);
        int do_close();
        int do_unlink(const int32_t action, int64_t& file_size);

      private:
        File file_;
        FSName fsname_;
        uint64_t ns_addr_;
        int32_t cluster_id_;
        common::RWLock rw_lock_;
    };
  }
}

#endif
