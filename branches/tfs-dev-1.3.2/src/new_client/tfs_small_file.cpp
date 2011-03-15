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
  return read_ex(buf, count, offset_);
}

int64_t TfsSmallFile::write(const void* buf, int64_t count)
{
  return write_ex(buf, count, offset_);
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
  return pwrite_ex(buf, count, offset);
}

int TfsSmallFile::fstat(TfsFileStat* file_stat, const TfsStatFlag mode)
{
  int ret = TFS_ERROR;
  if (file_stat != NULL)
  {
    FileInfo file_info;
    ret = fstat_ex(&file_info, mode);
    if (TFS_SUCCESS == ret)
    {
      file_stat->file_id_ = file_info.id_;
      file_stat->offset_ = file_info.offset_;
      file_stat->size_ = file_info.size_;
      file_stat->usize_ = file_info.usize_;
      file_stat->modify_time_ = file_info.modify_time_;
      file_stat->create_time_ = file_info.create_time_;
      file_stat->flag_ = file_info.flag_;
      file_stat->crc_ = file_info.crc_;
    }
  }
  return ret;
}

int TfsSmallFile::close()
{
  return close_ex();
}

int TfsSmallFile::unlink(const char* file_name, const char* suffix, const TfsUnlinkType action)
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

int TfsSmallFile::get_size_for_rw(const int64_t check_size, const int64_t count, int64_t& cur_size, bool& not_end)
{
  return get_size_for_rw_ex(check_size, count, cur_size, not_end, ClientConfig::segment_size_);
}

int TfsSmallFile::read_process()
{
  int ret = TFS_SUCCESS;
  //set status
  processing_seg_list_[0]->status_ = SEG_STATUS_OPEN_OVER;

  if ((ret = process(FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read data fail, ret: %d", ret);
  }
  return ret;
}

int TfsSmallFile::write_process()
{
  int ret = TFS_SUCCESS;
  // set status
  processing_seg_list_[0]->status_ = SEG_STATUS_CREATE_OVER;
  // write data
  if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "write data fail, ret: %d", ret);
  }
  return ret;
}

int32_t TfsSmallFile::finish_write_process(int status)
{
  int32_t count = 0;

  // for small file, once fail,
  if (status != TFS_SUCCESS)
  {
    // open with filename, just retry this block
    for (SEG_DATA_LIST_ITER it = processing_seg_list_.begin();
         it != processing_seg_list_.end(); it++)
    {
      // for small file, segment just write, not close
      if (SEG_STATUS_BEFORE_CLOSE_OVER == (*it)->status_)
      {
        if ((*it)->delete_flag_)
        {
          tbsys::gDelete(*it);
        }
        processing_seg_list_.erase(it);
        count++;
      }
      else
      {
        (*it)->seg_info_.crc_ = 0;
        // restart from beginning
        (*it)->status_ = SEG_STATUS_OPEN_OVER;
      }
    }
  }
  else
  {
    count = processing_seg_list_.size();
    // do nothing, clear by following process
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
