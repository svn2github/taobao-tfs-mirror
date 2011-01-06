/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_file.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENT_TFSSMALLFILE_H_
#define TFS_CLIENT_TFSSMALLFILE_H_

#include "tfs_file.h"

namespace tfs
{
  namespace client
  {
    class TfsSmallFile : public TfsFile
    {
    public:
      TfsSmallFile();
      TfsSmallFile(uint32_t block_id, common::VUINT64& ds_list, const char* write_buf, const int64_t count);
      virtual ~TfsSmallFile();

      virtual int open(const char* file_name, const char *suffix, int flags, ... );
      virtual int64_t read(void* buf, int64_t count);
      virtual int64_t write(const void* buf, int64_t count);
      virtual int64_t lseek(int64_t offset, int whence);
      virtual int64_t pread(void *buf, int64_t count, int64_t offset);
      virtual int64_t pwrite(const void *buf, int64_t count, int64_t offset);
      virtual int fstat(common::FileInfo* file_info, int32_t mode = common::NORMAL_STAT);
      virtual int close();
      virtual int unlink(const char* file_name, const char* suffix, const int action);

    protected:
      virtual int get_segment_for_read(int64_t offset, char* buf, int64_t count);
      virtual int get_segment_for_write(int64_t offset, const char* buf, int64_t count);
      virtual int read_process();
      virtual int write_process();
      virtual int32_t finish_write_process(int status);
      virtual int close_process();
      virtual int unlink_process();
    };
  }
}
#endif  // TFS_CLIENT_TFSSMALLFILE_H_
