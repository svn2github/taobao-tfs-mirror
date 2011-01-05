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

TfsSmallFile::TfsSmallFile()
{
}

TfsSmallFile::~TfsSmallFile()
{
}

int TfsSmallFile::open(const char *file_name, const char *suffix, int flags, ... )
{
  return open_ex(file_name, suffix, flags);
}

int64_t TfsSmallFile::read(void* buf, int64_t count)
{
  return read_ex(buf, count, offset_, false);
}

int64_t TfsSmallFile::write(const void* buf, int64_t count)
{
  return write_ex(buf, count, offset_, false);
}

int64_t TfsSmallFile::lseek(int64_t offset, int whence)
{
  return lseek_ex(offset, whence);
}

int64_t TfsSmallFile::pread(void *buf, int64_t count, int64_t offset)
{
  return pread_ex(buf, count, offset);
}

int64_t TfsSmallFile::pwrite(const void *buf, int64_t count, int64_t offset)
{
  return pwrite(buf, count, offset);
}

int TfsSmallFile::fstat(FileInfo* file_info, int32_t mode)
{
  return fstat_ex(file_info, mode);
}

int TfsSmallFile::close()
{
  return close_ex();
}

int TfsSmallFile::unlink(const char* file_name, const char* suffix, const int action)
{
  int ret = open_ex(file_name, suffix, T_WRITE | T_NOLEASE);
  if (TFS_SUCCESS != ret)
  {
    TBSYS_LOG(DEBUG, "tfs unlink fail, file_name: %s, action: %d", (file_name == NULL) ? file_name : "NULL", action);
  }
  else
  {
    meta_seg_->file_number_ = action;
    ret = unlink_process();
  }
  return ret;
}

int TfsSmallFile::get_segment_for_read(int64_t offset, char* buf, int64_t count)
{
  return get_segment_for_write(offset, buf, count);
}

int TfsSmallFile::get_segment_for_write(int64_t offset, const char* buf, int64_t count)
{
  destroy_seg();
  return get_meta_segment(offset, const_cast<char*>(buf), count);
}

int TfsSmallFile::read_process()
{
  int ret = TFS_SUCCESS;
  if ((ret = process(FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read data fail, ret: %d", ret);
  }
  return ret;
}

int TfsSmallFile::write_process()
{
  int ret = TFS_SUCCESS;
  // write data
  if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "write data fail, ret: %d", ret);
  }
  return ret;
}

int32_t TfsSmallFile::finish_write_process()
{
  int32_t count = 0;
  SEG_DATA_LIST_ITER it = processing_seg_list_.begin();
  while (it != processing_seg_list_.end())
  {
    if (SEG_STATUS_SUCCESS == (*it)->status_)
    {
      count++;
    }
  }
  return count;
}

int TfsSmallFile::close_process()
{
  int ret = TFS_SUCCESS;
  get_meta_segment(0, NULL, 0);
  if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
  }
  return ret;
}

int TfsSmallFile::unlink_process()
{
  int ret = TFS_SUCCESS;
  get_meta_segment(0, NULL, 0);
  if ((ret = process(FILE_PHASE_UNLINK_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "unlink file fail, ret: %d", ret);
  }
  return ret;
}
