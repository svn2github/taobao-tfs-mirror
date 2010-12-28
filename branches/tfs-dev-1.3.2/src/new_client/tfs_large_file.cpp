#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;

TfsLargeFile::TfsLargeFile()
{
}

TfsLargeFile::~TfsLargeFile()
{
}

int TfsLargeFile::open(const char* file_name, const char *suffix, const int flags, ... )
{
  int ret = TFS_SUCCESS;

  flags_ = flags;
  if (!(flags_ & T_WRITE))       // not write
  {
    if ((ret = open_ex(file_name, suffix, flags)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open file fail, ret: %d", ret);
    }

    if (TFS_SUCCESS == ret)
    {
      FileInfo file_info;
      if ((ret = fstat_ex(&file_info, 0)) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "stat file %s fail, ret: %d", fsname_.get_name(), ret);
      }

      if (TFS_SUCCESS == ret)
      {
        int32_t size = file_info.size_;
        assert (size <= MAX_META_SIZE);
        char* seg_buf = new char[size];
        if ((ret = read_ex(seg_buf, size, 0, false)) != size)
        {
          TBSYS_LOG(ERROR, "read meta file fail, size: %d, retsize: %d", size, ret);
          ret = TFS_ERROR;
        }

        if (TFS_SUCCESS == ret)
        {
          if ((ret = local_key_.load(seg_buf)) != TFS_SUCCESS)
          {
            TBSYS_LOG(ERROR, "construct meta file info fail, ret: %d", ret);
          }
        }
        tbsys::gDelete(seg_buf);
      }
    }
  }
  else  // write flag
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
      offset_ = 0;
      eof_ = TFS_FILE_EOF_NO;
      is_open_ = TFS_FILE_OPEN_YES;
    }
    va_end(args);
  }

  return ret;
}

int64_t TfsLargeFile::read(void* buf, int64_t count)
{
  return read_ex(buf, count, offset_, true);
}

int64_t TfsLargeFile::write(const void* buf, int64_t count)
{
  return write_ex(buf, count, offset_, true);
}

int64_t TfsLargeFile::lseek(int64_t offset, int whence)
{
  return lseek_ex(offset, whence);
}

int64_t TfsLargeFile::pread(void* buf, int64_t count, int64_t offset)
{
  return pread_ex(buf, count, offset);
}

int64_t TfsLargeFile::pwrite(const void* buf, int64_t count, int64_t offset)
{
  return pwrite_ex(buf, count, offset);
}

int TfsLargeFile::fstat(FileInfo* file_info, int32_t mode)
{
  int ret = fstat_ex(file_info, mode);
  if (TFS_SUCCESS == ret)
  {
    // dangerous .. length crash
    file_info->size_ = static_cast<int32_t>(local_key_.get_file_size());
  }
  // log in stat_ex
  return ret;
}

int TfsLargeFile::close()
{
  return close_ex();
}

int TfsLargeFile::get_segment_for_read(int64_t offset, char* buf, int64_t count)
{
  destroy_seg();
  return local_key_.get_segment_for_read(offset_, buf, count, processing_seg_list_);
}

int TfsLargeFile::get_segment_for_write(int64_t offset, const char* buf, int64_t count)
{
  destroy_seg();
  return local_key_.get_segment_for_write(offset_, buf, count, processing_seg_list_);
}

int TfsLargeFile::read_process()
{
  int ret = TFS_ERROR;
  if ((ret = tfs_session_->get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get block info fail, ret: %d", ret);
    return ret;
  }

  if ((ret = process(FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read data fail, ret: %d", ret);
    return ret;
  }
  return ret;
}

int TfsLargeFile::write_process()
{
  int ret = TFS_ERROR;
  if ((ret = tfs_session_->get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "batch get block info error, count: %d, flag: %d", processing_seg_list_.size(), flags_);
    return ret;
  }

  // create file
  if ((ret = process(FILE_PHASE_CREATE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "create file name fail, ret: %d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "create file success, ret: %d", ret);
  }

  // write data
  if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "write data fail, ret: %d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "write data success, ret: %d", ret);
  }

  // close file
  if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "close tfs file fail, ret: %d", ret);
    return ret;
  }
  else
  {
    TBSYS_LOG(DEBUG, "close file success, ret: %d", ret);
  }

  return ret;
}

int32_t TfsLargeFile::finish_write_process()
{
  int32_t count = 0;
  int ret = TFS_ERROR;
  SEG_DATA_LIST_ITER it = processing_seg_list_.begin();
  for (; it != processing_seg_list_.end(); it++)
  {
    if (SEG_STATUS_SUCCESS == (*it)->status_)
    {
      if ((ret = local_key_.add_segment((*it)->seg_info_)) != TFS_SUCCESS)
      {
        break;
      }
      count++;
    }
  }
  if (TFS_SUCCESS == ret)
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
  if (remove_key() != TFS_SUCCESS) // TODO .. ignore local key fail ?
  {
    TBSYS_LOG(ERROR, "remove key fail, desc: %s", strerror(errno));
  }

  if (TFS_SUCCESS == ret)
  {
    fsname_.set_file_id(meta_seg_->seg_info_.file_id_);
    fsname_.set_block_id(meta_seg_->seg_info_.block_id_);
  }
  return ret;
}

int TfsLargeFile::upload_key()
{
  int32_t size = local_key_.get_data_size();
  char* buf = new char[size];
  local_key_.dump_data(buf);
  int ret = TFS_ERROR;
  if ((ret = open_ex(NULL, NULL, T_WRITE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "upload key fail, open file fail, ret: %d");
  }
  else
  {
    destroy_seg();
    meta_seg_->buf_ = buf;
    meta_seg_->cur_offset_ = 0;
    meta_seg_->cur_size_ = size;

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

  tbsys::gDeleteA(buf);
  return ret;
}

int TfsLargeFile::remove_key()
{
  return local_key_.remove();
}
