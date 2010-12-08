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
      virtual int read(void* buf, size_t count);
      virtual int write(const void* buf, size_t count);
      virtual off_t lseek(off_t offset, int whence);
      virtual ssize_t pread(void *buf, size_t count, off_t offset);
      virtual ssize_t pwrite(const void *buf, size_t count, off_t offset);
      virtual int close();

      int do_async_request(const InnerFilePhase file_phase, const int64_t wait_id);
      int do_async_response(const InnerFilePhase file_phase, /*response*/);

    private:
      int async_req_create_file();
      int async_rsp_create_file();

      int async_req_write_data();
      int async_rsp_write_data();

      int async_req_close_file();
      int async_rsp_close_file();
    };
  }
}
#endif  // TFS_CLIENT_TFSSMALLFILE_H_
