/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: tfs_file.cpp 19 2010-10-18 05:48:09Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "tfs_small_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;


TfsSmallFile::TfsSmallFile()
{

}

TfsSmallFile::~TfsSmallFile()
{

}

int TfsSmallFile::open(const char *file_name, const char *suffix, int flags, ... )
{
  flag_ = flags;
  return TFS_SUCCESS;
}

int TfsSmallFile::read(void* buf, size_t count)
{

}

int TfsSmallFile::write(const void* buf, size_t count)
{

}

off_t TfsSmallFile::lseek(off_t offset, int whence)
{

}

ssize_t TfsSmallFile::pread(void *buf, size_t count, off_t offset)
{

}

ssize_t TfsSmallFile::pwrite(const void *buf, size_t count, off_t offset)
{

}


int TfsSmallFile::close()
{

}

int TfsSmallFile::do_async_request(const InnerFilePhase file_phase, const int64_t wait_id)
{
  int ret = TFS_SUCCESS;
  switch (file_phase)
  {
    case FILE_PHASE_CREATE_FILE:
      ret = async_req_create_file(wait_id);
      break;
    case FILE_PHASE_WRITE_DATA:
      ret = async_req_write_data(wait_id);
      break;
    case FILE_PHASE_CLOSE_FILE:
      ret = async_req_close_file(wait_id);
      break;
  }
}

int TfsSmallFile::do_async_response(const InnerFilePhase file_phase, /*response*/)
{
}

int TfsSmallFile::async_req_create_file()
{
}

int TfsSmallFile::async_rsp_create_file()
{
}

int TfsSmallFile::async_req_write_data()
{
}

int TfsSmallFile::async_rsp_write_data()
{
}

int TfsSmallFile::async_req_close_file()
{
}

int TfsSmallFile::async_rsp_close_file()
{
}
