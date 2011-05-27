/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: bg_task.cpp 166 2011-03-15 07:35:48Z zongdai@taobao.com $
 *
 * Authors:
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSLARGEFILE_H_
#define TFS_CLIENT_TFSLARGEFILE_H_

#include <stdint.h>
#include "common/internal.h"
#include "tfs_file.h"
#include "local_key.h"

namespace tfs
{
  namespace client
  {
    class TfsLargeFile : public TfsFile
    {
    public:
      TfsLargeFile();
      virtual ~TfsLargeFile();

      virtual int open(const char* file_name, const char* suffix, const int flags, ... );
      virtual int64_t read(void* buf, const int64_t count);
      virtual int64_t write(const void* buf, const int64_t count);
      virtual int64_t lseek(const int64_t offset, const int whence);
      virtual int64_t pread(void* buf, const int64_t count, const int64_t offset);
      virtual int64_t pwrite(const void* buf, const int64_t count, const int64_t offset);
      virtual int fstat(common::TfsFileStat* file_info, const common::TfsStatType mode = common::NORMAL_STAT);
      virtual int close();
      virtual int64_t get_file_length();
      virtual int unlink(const char* file_name, const char* suffix, const common::TfsUnlinkType action);

    protected:
      virtual int64_t get_segment_for_read(const int64_t offset, char* buf, const int64_t count);
      virtual int64_t get_segment_for_write(const int64_t offset, const char* buf, const int64_t count);
      virtual int read_process(int64_t& read_size);
      virtual int write_process();
      virtual int32_t finish_write_process(const int status);
      virtual int close_process();
      virtual int unlink_process();

    private:
      int upload_key();
      int load_meta(common::FileInfo& file_info);

    private:
      static const int64_t MAX_META_SIZE = 1 << 22;
      static const int64_t INVALID_FILE_SIZE = -1;

    private:
      LocalKey local_key_;
      bool read_meta_flag_;
      char* meta_suffix_;
    };
  }
}
#endif  // TFS_CLIENT_TFSLARGEFILE_H_
