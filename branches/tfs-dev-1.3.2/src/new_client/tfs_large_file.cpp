#include "tfs_large_file.h"

using namespace tfs::client;
using namespace tfs::common;
using namespace tfs::message;


TfsLargeFile::TfsLargeFile()
{
}

TfsLargeFile::~TfsLargeFile()
{
}

int TfsLargeFile::open(const char* file_name, const char *suffix, int flags, ... )
{
  int ret = TFS_ERROR;

  flags_ = flags;
  if (!(flags_ & T_WRITE))       // not write
  {
    if ((ret = open_ex(file_name, suffix, flags)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "open file fail, ret:%d", ret);
      return ret;
    }

    FileInfo file_info;
    if ((ret = stat_ex(&file_info, 0)) != TFS_SUCCESS) // TODO
    {
      TBSYS_LOG(ERROR, "stat file %s fail, ret:%d", fsname_.get_name(), ret);
      return ret;
    }

    int32_t size = file_info.size_;
    char* seg_buf = new char[size];
    if ((ret = read_ex(seg_buf, size, 0, false)) != size)
    {
      TBSYS_LOG(ERROR, "read meta file fail, size:%d, retsize:%d", size, ret);
      return ret;
    }

    if ((ret = local_key_.load(seg_buf)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "construct meta file info fail, ret:%d", ret);
      return ret;
    }
    tbsys::gDelete(seg_buf);
  }
  else  // write flag
  {
    va_list args;
    va_start(args, flags);
    char* local_key = va_arg(args, char*);
    if (!local_key)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "open with T_LARGE|T_WRITE flag occur null key");
      return ret;
    }
    if ((ret = local_key_.initialize(local_key, tfs_session_->get_ns_addr())) != TFS_SUCCESS)
    {
      snprintf(error_message_, ERR_MSG_SIZE, "initialize local key fail, ret(%d)", ret);
      return ret;
    }
  }
  is_open_ = TFS_FILE_OPEN_YES;
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
    TBSYS_LOG(ERROR, "get block info fail, ret:%d", ret);
    return ret;
  }

  if ((ret = process(FILE_PHASE_READ_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read data fail, ret:%d", ret);
    return ret;
  }
  return ret;
}

int TfsLargeFile::write_process()
{
  int ret = TFS_ERROR;
  if ((ret = tfs_session_->get_block_info(processing_seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "batch get block info error, count:%d, flag:%d", processing_seg_list_.size(), flags_);
    return ret;
  }

  // create file
  if ((ret = process(FILE_PHASE_CREATE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "create file name fail, ret:%d", ret);
    return ret;
  }

  // write data
  if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "write data fail, ret:%d", ret);
    return ret;
  }

  // close file
  if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "close tfs file fail, ret:%d", ret);
    return ret;
  }

  if ((ret = update_key()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "update key fail, ret:%d", ret);
    return ret;
  }

  if ((ret = flush_key()) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "flush key fail, ret:%d", ret);
    return ret;
  }
  return ret;
}

int TfsLargeFile::close_process()
{
  int ret = TFS_ERROR;
  if ((ret = upload_key()) != TFS_SUCCESS) // upload key data
  {
    TBSYS_LOG(ERROR, "close tfs file fail: upload key fail, ret:%d");
  }
  if (remove_key() != TFS_SUCCESS) // ignore local key fail ?
  {
    TBSYS_LOG(ERROR, "remove key fail, ret:%d", ret);
  }
  return ret;
}

int TfsLargeFile::update_key()
{
  for (vector<SegmentData*>::iterator it = processing_seg_list_.begin();
       it != processing_seg_list_.end(); it++)
  {
    if ((*it)->status_)
    {
      local_key_.add_segment((*it)->seg_info_);
      // erase success
      tbsys::gDelete(*it);
      processing_seg_list_.erase(it);
    }
  }
  return TFS_SUCCESS;
}

int TfsLargeFile::flush_key()
{
  return local_key_.save();
}

int TfsLargeFile::upload_key()
{
  int32_t size = local_key_.get_data_size();
  char* buf = new char[size];
  local_key_.dump_data(buf);
  int ret = TFS_ERROR;
  if ((ret = open_ex(NULL, NULL, T_WRITE)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "upload key fail, open file fail, ret:%d");
  }
  else
  {
    destroy_seg();
    SegmentData* seg_data = new SegmentData();
    seg_data->buf_ = buf;
    seg_data->seg_info_.offset_ = 0;
    seg_data->seg_info_.size_ = size;
    processing_seg_list_.push_back(seg_data);

    if ((ret = process(FILE_PHASE_WRITE_DATA)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "upload key fail, write data fail, ret:%d", ret);
    }
    else if ((ret = process(FILE_PHASE_CLOSE_FILE)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "upload key fail, close file fail, ret:%d", ret);
    }
  }

  tbsys::gDelete(buf);
  return ret;
}

int TfsLargeFile::remove_key()
{
  return local_key_.remove();
}
