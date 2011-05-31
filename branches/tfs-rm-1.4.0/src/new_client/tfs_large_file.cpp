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
#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;

TfsLargeFile::TfsLargeFile() : read_meta_flag_(true), meta_suffix_(NULL)
{
}

TfsLargeFile::~TfsLargeFile()
{
}

int TfsLargeFile::open(const char* file_name, const char* suffix, const int flags, ... )
{
  int ret = TFS_SUCCESS;

  flags_ = flags;
  if (!(flags_ & T_WRITE))       // not write, load meta first
  {
    if (NULL == file_name || file_name[0] != 'L')
    {
      TBSYS_LOG(ERROR, "open large file fail, illegal file name: %s", file_name ? file_name : "NULL");
      ret = TFS_ERROR;
    }
    else
    {
      if ((ret = open_ex(file_name, suffix, flags)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "open meta file fail, ret: %d", ret);
      }

      // if T_STAT is set, do not load meta here
      if (TFS_SUCCESS == ret && !(flags & T_STAT))
      {
        FileInfo file_info;
        if ((ret = fstat_ex(&file_info, NORMAL_STAT)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "stat meta file %s fail, ret: %d", fsname_.get_name(true), ret);
        }
        else
        {
          ret = load_meta(file_info);
        }
      }
    }
  }
  else  // write flag
  {
    if (NULL != file_name && file_name[0] != '\0')
    {
      TBSYS_LOG(ERROR, "write large file fail, not support update now. file_name: %s", file_name ? file_name : "NULL");
      ret = TFS_ERROR;
    }
    else
    {
      va_list args;
      va_start(args, flags);
      char* local_key = va_arg(args, char*);
      if (NULL == local_key)
      {
        TBSYS_LOG(ERROR, "open with T_LARGE|T_WRITE flag occur null key");
        ret = TFS_ERROR;
      }
      else
      {
        if ((ret = local_key_.initialize(local_key, tfs_session_->get_ns_addr())) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "initialize local key fail, ret: %d", ret);
        }
      }

      if (TFS_SUCCESS == ret)
      {
        meta_suffix_ = const_cast<char*>(suffix);
        offset_ = 0;
        eof_ = TFS_FILE_EOF_FLAG_NO;
        file_status_ = TFS_FILE_OPEN_YES;
      }
      va_end(args);
    }
  }

  return ret;
}

int64_t TfsLargeFile::read(void* buf, const int64_t count)
{
  return read_ex(buf, count, offset_);
}

int64_t TfsLargeFile::write(const void* buf, const int64_t count)
{
  return write_ex(buf, count, offset_);
}

int64_t TfsLargeFile::lseek(const int64_t offset, const int whence)
{
  return lseek_ex(offset, whence);
}

int64_t TfsLargeFile::pread(void* buf, const int64_t count, const int64_t offset)
{
  return pread_ex(buf, count, offset);
}

int64_t TfsLargeFile::pwrite(const void* buf, const int64_t count, const int64_t offset)
{
  return pwrite_ex(buf, count, offset);
}

int TfsLargeFile::fstat(TfsFileStat* file_stat, const TfsStatType mode)
{
  TBSYS_LOG(DEBUG, "stat file start, mode: %d", mode);
  FileInfo file_info;
  int ret = fstat_ex(&file_info, mode);
  if (TFS_SUCCESS == ret)
  {
    if (0 == file_info.flag_)
    {
      // load meta
      if ((ret = load_meta(file_info)) == TFS_SUCCESS)
      {
        file_stat->file_id_ = file_info.id_;
        file_stat->offset_ = file_info.offset_;
        file_stat->size_ = local_key_.get_file_size();
        // usize = real_size + meta_size
        file_stat->usize_ += local_key_.get_file_size();
        file_stat->modify_time_ = file_info.modify_time_;
        file_stat->create_time_ = file_info.create_time_;
        file_stat->flag_ = file_info.flag_;
        file_stat->crc_ = file_info.crc_;
      }
    }
    else // file is delete or conceal
    {
      // TODO: ?
      // 1. unhide/undelete meta file
      // 2. read meta file head
      // 3. hide/delete meta file
      file_stat->file_id_ = meta_seg_->seg_info_.file_id_;
      file_stat->offset_ = static_cast<int32_t>(INVALID_FILE_SIZE);
      file_stat->size_ = INVALID_FILE_SIZE;
      file_stat->usize_ = INVALID_FILE_SIZE;
      file_stat->modify_time_ = static_cast<time_t>(INVALID_FILE_SIZE);
      file_stat->create_time_ = static_cast<time_t>(INVALID_FILE_SIZE);
      file_stat->flag_ = file_info.flag_;
      file_stat->crc_ = static_cast<uint32_t>(INVALID_FILE_SIZE);
    }
  }

  return ret;
}

int TfsLargeFile::close()
{
  return close_ex();
}

int64_t TfsLargeFile::get_file_length()
{
  return local_key_.get_file_size();
}

int TfsLargeFile::unlink(const char* file_name, const char* suffix, const TfsUnlinkType action)
{
  // read meta first
  int ret = TFS_SUCCESS;
  if ((ret = open_ex(file_name, suffix, T_READ)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "open meta file fail, ret: %d", ret);
  }
  else
  {
    if (DELETE == action) // if DELETE, need load meta. CONCEAL or REVEAL, skip it
    {
      FileInfo file_info;
      // TODO: status is HIDE
      // 1. unhide meta file
      // 2. read meta file
      // 3. unlink all file
      // 4. unlink meta file ?
      if ((ret = fstat_ex(&file_info, NORMAL_STAT)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "stat meta file %s fail, ret: %d", fsname_.get_name(true), ret);
      }
      else
      {
        ret = load_meta(file_info);
      }
    }
    else if (UNDELETE == action)
    {
      TBSYS_LOG(ERROR, "tfs not support undelete large file now");
      ret = EXIT_NOT_PERM_OPER;
      //Todo
      //1. undelete meta file
      //2. load meta file
      //3. undelete all seg file
    }
  }

  if (TFS_SUCCESS == ret)
  {
    // unlink meta
    meta_seg_->file_number_ = action;
    meta_seg_->status_ = SEG_STATUS_OPEN_OVER;

    get_meta_segment(0, NULL, 0);
    if ((ret = unlink_process()) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "large file unlink meta fail, ret: %d", ret);
    }
    else //do not care segment status
    {
      //unlink data
      if ((action & DELETE)) // delete or undelete will affect segments. if conceal or reveal, skip. now do not support UNDELETE
      {
        SEG_SET& seg_list = local_key_.get_seg_info();
        SEG_SET_CONST_ITER sit = seg_list.begin();
        for ( ; sit != seg_list.end(); ++sit)
        {
          destroy_seg();
          SegmentData* seg_data = new SegmentData();
          seg_data->seg_info_ = *sit;
          seg_data->file_number_ = action;
          processing_seg_list_.push_back(seg_data);
          unlink_process();
        }
      }
    }
  }

  return ret;
}

int64_t TfsLargeFile::get_segment_for_read(const int64_t offset, char* buf, const int64_t count)
{
  int64_t ret = count;
  destroy_seg();
  if (read_meta_flag_)
  {
    ret = get_meta_segment(offset, buf, count);
  }
  else
  {
    ret = local_key_.get_segment_for_read(offset, buf, count, processing_seg_list_);
  }
  return ret;
}

int64_t TfsLargeFile::get_segment_for_write(const int64_t offset, const char* buf, const int64_t count)
{
  destroy_seg();
  return local_key_.get_segment_for_write(offset, buf, count, processing_seg_list_);
}

int TfsLargeFile::read_process(int64_t& read_size)
{
  int ret = TFS_SUCCESS;
  if ((ret = tfs_session_->get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get block info fail, ret: %d", ret);
  }
  else
  {
    // segList.size() != 0
    // just use first ds list size to be retry times. maybe random ..
    int32_t retry_count = processing_seg_list_[0]->ds_.size();
    do
    {
      ret = process(FILE_PHASE_READ_FILE);
      finish_read_process(ret, read_size);
    } while (ret != TFS_SUCCESS && --retry_count > 0);

  }
  return ret;
}

int TfsLargeFile::write_process()
{
  int ret = TFS_SUCCESS;
  // get block info fail, must exit.
  if ((ret = tfs_session_->get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "batch get block info error, count: %d, flag: %d", processing_seg_list_.size(), flags_);
  }
  else
  {
    // create file
    if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_CREATE_FILE)))
    {
      TBSYS_LOG(ERROR, "create file name fail, ret: %d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "create file success, ret: %d", ret);
      // write data
      if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_WRITE_DATA)))
      {
        TBSYS_LOG(ERROR, "write data fail, ret: %d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "write data success, ret: %d", ret);
        // close file
        if (EXIT_ALL_SEGMENT_ERROR == (ret = process(FILE_PHASE_CLOSE_FILE)))
        {
          TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "close file success, ret: %d", ret);
        }
      }
    }
  }

  return ret;
}

int32_t TfsLargeFile::finish_write_process(const int status)
{
  int32_t count = 0;
  int ret = TFS_ERROR;
  SEG_DATA_LIST_ITER it = processing_seg_list_.begin();

  // just for unnecessary iteration and vector erase operation, cope with different status.
  // a little duplicated code

  if (TFS_SUCCESS == status)
  {
    count = processing_seg_list_.size();
    for (; it != processing_seg_list_.end(); it++)
    {
      if ((ret = local_key_.add_segment((*it)->seg_info_)) != TFS_SUCCESS) // should never happen
      {
        break;
      }
      else if ((*it)->delete_flag_)
      {
        tbsys::gDelete(*it);
      }
    }
    processing_seg_list_.clear();
  }
  else
  {
    while (it != processing_seg_list_.end())
    {
      if (SEG_STATUS_ALL_OVER == (*it)->status_) // all over
      {
        if ((ret = local_key_.add_segment((*it)->seg_info_)) != TFS_SUCCESS)
        {
          break;
        }
        else
        {
          if ((*it)->delete_flag_)
          {
            tbsys::gDelete(*it);
          }
          it = processing_seg_list_.erase(it);
          count++;
        }
      }
      else
      {
        // reset crc here
        (*it)->seg_info_.crc_ = 0;
        // Todo: set file_id_ to zero depend on update flag
        (*it)->seg_info_.file_id_= 0;
        it++;
      }
    }
  }

  // maybe move from here
  if (TFS_SUCCESS == ret && count > 0)
  {
    ret = local_key_.save();
  }

  return (ret != TFS_SUCCESS) ? 0 : count;
}

int TfsLargeFile::close_process()
{
  int ret = TFS_ERROR;
  if ((ret = upload_key()) != TFS_SUCCESS) // upload key data
  {
    TBSYS_LOG(ERROR, "close tfs file fail: upload key fail, ret: %d");
  }
  return ret;
}

int TfsLargeFile::unlink_process()
{
  int ret = TFS_SUCCESS;
  if (1 != processing_seg_list_.size())
  {
    TBSYS_LOG(ERROR, "unlink file fail, processing seg list illegal size: %d", processing_seg_list_.size());
    ret = TFS_ERROR;
  }
  else
  {
    SegmentData* seg_data = processing_seg_list_[0];
    if (NULL == seg_data)
    {
      TBSYS_LOG(ERROR, "unlink file fail, processing seg list is null");
      ret = TFS_ERROR;
    }
    else
    {
      if ((ret = tfs_session_->get_block_info(seg_data->seg_info_.block_id_, seg_data->ds_, T_WRITE)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "unlink get block info fail, ret: %d", ret);
      }
      else
      {
        if ((ret = process(FILE_PHASE_UNLINK_FILE)) != TFS_SUCCESS)
        {
          TBSYS_LOG(ERROR, "unlink file fail, ret: %d", ret);
        }
      }
    }
  }
  return ret;
}

int TfsLargeFile::upload_key()
{
  int ret = TFS_SUCCESS;
  if ((ret = local_key_.validate(offset_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "local key validate fail, ret: %d");
  }
  else
  {
    int32_t size = local_key_.get_data_size();
    char* buf = new char[size];
    local_key_.dump_data(buf, size);
    // TODO .. open large with mainname
    if ((ret = open_ex(NULL, meta_suffix_, T_WRITE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "upload key fail, open file fail, ret: %d", ret);
    }
    else
    {
      destroy_seg();
      meta_seg_->buf_ = buf;
      meta_seg_->seg_info_.offset_ = 0;
      meta_seg_->seg_info_.size_ = size;
      processing_seg_list_.push_back(meta_seg_);

      if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, write data fail, ret: %d", ret);
      }
      else if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, close file fail, ret: %d", ret);
      }
    }

    if (TFS_SUCCESS == ret)
    {
      // remove fail, then data can be gc
      if ((ret = local_key_.remove()) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "upload key fail, remove local key fail, ret: %d", ret);
      }
    }
    tbsys::gDeleteA(buf);
  }
  return ret;
}

int TfsLargeFile::load_meta(FileInfo& file_info)
{
  int ret = TFS_SUCCESS;
  int32_t size = file_info.size_;
  assert (size <= MAX_META_SIZE);
  char* seg_buf = new char[size];

  //set meta_seg
  meta_seg_->seg_info_.offset_ = 0;
  meta_seg_->seg_info_.size_ = size;
  meta_seg_->buf_ = seg_buf;
  meta_seg_->status_ = SEG_STATUS_OPEN_OVER;
  read_meta_flag_ = true;

  if ((ret = read_ex(seg_buf, size, 0, false)) != size)
  {
    TBSYS_LOG(ERROR, "read meta file fail, size: %d, retsize: %d", size, ret);
    ret = TFS_ERROR;
  }
  else
  {
    if ((ret = local_key_.load(seg_buf)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "construct meta file info fail, ret: %d", ret);
    }
    else if ((ret = local_key_.validate()) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "local key validate fail when read file, ret: %d", ret);
    }
  }
  read_meta_flag_ = false;
  tbsys::gDeleteA(seg_buf);

  return ret;
}
