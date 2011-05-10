/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#include <Memory.hpp>
#include "tbsys.h"

#include "common/directory_op.h"
#include "gc_file.h"

using namespace tfs::client;
using namespace tfs::common;

const char* tfs::client::GC_FILE_PATH = "/tmp/TFSlocalkeyDIR/gc/";

GcFile::GcFile(const bool need_save_seg_infos) 
  :need_save_seg_infos_(need_save_seg_infos), file_pos_(sizeof(SegmentHead)), file_op_(NULL)
{
}

GcFile::~GcFile()
{
  // no gc segment, remove file
  if (file_op_ != NULL)
  {
    if (0 == seg_head_.count_)
    {
      TBSYS_LOG(DEBUG, "no gc info, remove file");
      file_op_->unlink_file();
    }
    else if (need_save_seg_infos_) // not load, save remaining segment infos
    {
      save_gc();
    }
  }
  tbsys::gDelete(file_op_);
}

int GcFile::initialize(const char* name)
{
  char file_path[MAX_PATH_LENGTH];
  snprintf(file_path, MAX_PATH_LENGTH, "%s%s", GC_FILE_PATH, name);
  int ret = TFS_SUCCESS;

  if (!DirectoryOp::create_full_path(GC_FILE_PATH))
  {
    TBSYS_LOG(ERROR, "initialize local gc fail, create directory %s failed, error: %d, desc: %s",
              GC_FILE_PATH, errno, strerror(errno));
    ret = TFS_ERROR;
  }
  else
  {
    if (access(file_path, F_OK) != 0)
    {
      file_op_ = new FileOperation(file_path, O_RDWR|O_CREAT);
    }
    else
    {
      file_op_ = new FileOperation(file_path, O_RDWR|O_APPEND);
      load_head();
      TBSYS_LOG(DEBUG, "load head count: %d, size: %"PRI64_PREFIX"d", seg_head_.count_, seg_head_.size_);
    }
    TBSYS_LOG(DEBUG, "initialize gc file success");
  }
  return ret;
}

int GcFile::load_file(const char* name)
{
  tbsys::gDelete(file_op_);
  file_op_ = new FileOperation(name, O_RDWR);
  return load();
}

int GcFile::add_segment(const SegmentInfo& seg_info)
{
  int ret = TFS_SUCCESS;
  seg_info_.push_back(seg_info);
  seg_head_.count_++;
  seg_head_.size_ += seg_info.size_;

  TBSYS_LOG(DEBUG, "add gc segment. blockid: %u, fileid: %"PRI64_PREFIX"u, offset: %"PRI64_PREFIX"d, size: %d, crc: %u",
            seg_info.block_id_, seg_info.file_id_, seg_info.offset_, seg_info.size_, seg_info.crc_);
  return ret;
}

void GcFile::dump(char* buf, const int32_t buffer_size)
{
  size_t size = seg_info_.size();
  for (size_t i = 0; i < size && (i + 1) * sizeof(SegmentInfo) <= buffer_size; i++)
  {
    memcpy(buf + i * sizeof(SegmentInfo), &seg_info_[i], sizeof(SegmentInfo));
  }
}

int GcFile::save()
{
  int ret = TFS_SUCCESS;

  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "save fail, file not initialized");
    ret = TFS_ERROR;
  }
  else if (static_cast<int>(seg_info_.size()) > GC_BATCH_WIRTE_COUNT)
  {
    ret = save_gc();
  }
  return ret;
}

int GcFile::remove()
{
  int ret = TFS_SUCCESS;
  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "remove gc file fail, not initialized");
    ret = TFS_ERROR;
  }
  else if ((ret = file_op_->unlink_file()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "remove gc file fail, ret: %d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "remove gc file success");
  }
  return ret;
}

int32_t GcFile::get_data_size() const
{
  return sizeof(SegmentHead) + seg_head_.count_ * sizeof(SegmentInfo);
}

int64_t GcFile::get_file_size() const
{
  return seg_head_.size_;
}

int32_t GcFile::get_segment_size() const
{
  return seg_head_.count_;
}

int GcFile::save_gc()
{
  int ret = TFS_SUCCESS;
  int32_t size = seg_info_.size() * sizeof(SegmentInfo);
  char* buf = new char[size];
  dump(buf, size);
  // to avoid the info conflict caused when fail between writing segment info and flushing segment head,
  // use pwrite innstead of write with append
  if ((ret = file_op_->pwrite_file(buf, size, file_pos_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "gc save fail, write file error, ret: %d", ret);
  }
  else if ((ret = file_op_->pwrite_file(reinterpret_cast<char*>(&seg_head_), sizeof(SegmentHead), 0)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "gc flush head fail, ret: %d", ret);
  }
  else                        // write fail, not clear, wait for next chance
  {
    TBSYS_LOG(DEBUG, "gc save segment success, count: %d, raw size: %d, need gc segment count: %d, size: %"PRI64_PREFIX"d",
              seg_info_.size(), size, seg_head_.count_, seg_head_.size_);
    file_op_->flush_file();
    file_pos_ += size;
    seg_info_.clear();
  }
  tbsys::gDeleteA(buf);

  return ret;
}

int GcFile::load()
{
  int ret = TFS_SUCCESS;
  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "load fail, not initialize");
    ret = TFS_ERROR;
  }
  else if ((ret = load_head()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "load segment head fail, ret: %d", ret);
  }
  else
  {
    char* buf = new char[sizeof(SegmentInfo) * seg_head_.count_];

    if ((ret = file_op_->pread_file(buf, sizeof(SegmentInfo) * seg_head_.count_, sizeof(SegmentHead))) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load segment info fail, ret: %d", ret);
    }
    else
    {
      seg_info_.clear();
      SegmentInfo* seg_info = reinterpret_cast<SegmentInfo*>(buf);
      for (int32_t i = 0; i < seg_head_.count_; i++)
      {
        seg_info_.push_back(seg_info[i]);
        TBSYS_LOG(DEBUG, "load segment info, offset: %"PRI64_PREFIX"d, blockid: %u, fileid: %"PRI64_PREFIX"u, size: %d, crc: %u", seg_info[i].offset_, seg_info[i].block_id_, seg_info[i].file_id_, seg_info[i].size_, seg_info[i].crc_);
      }
    }
    tbsys::gDeleteA(buf);
  }
  return ret;
}

int GcFile::load_head()
{
  // check size 0 ?
  return file_op_->pread_file(reinterpret_cast<char*>(&seg_head_), sizeof(SegmentHead), 0);
}
