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
    return open_ex(file_name, suffix, flags);
  }

  // write flag
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
  is_open_ = TFS_FILE_OPEN_YES;
  return ret;
}

int64_t TfsLargeFile::read(void* buf, int64_t count)
{
  int ret = TFS_ERROR;
  if (flags_ & T_WRITE)
  {
    TBSYS_LOG(ERROR, "file open without read flag");
    return ret;
  }

  if (is_open_ != TFS_FILE_OPEN_YES)
  {
    TBSYS_LOG(ERROR, "file not open");
    return ret;
  }

  char* seg_buf = new char[1024]; // TODO ...
  if ((ret = read_ex(seg_buf, 1024)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "read meta file fail, ret:%d", ret);
    return ret;
  }

  if ((ret = local_key_.load(seg_buf)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "construct meta file info fail, ret:%d", ret);
    return ret;
  }

  if ((ret = local_key_.get_segment_for_read(offset_, reinterpret_cast<const char*>(buf), count, seg_list_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get segment for read fail, ret:%d", ret);
    return ret;
  }

  if ((ret = tfs_session_->batch_get_block_info(seg_list_, flags_)) != TFS_SUCCESS)
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

int64_t TfsLargeFile::write(const void* buf, int64_t count)
{
  //do not consider the update operation now
  int ret = TFS_ERROR;
  if (!(flags_ & T_WRITE))
  {
    TBSYS_LOG(ERROR, "file open without write flag");
    return ret;
  }

  if (is_open_ != TFS_FILE_OPEN_YES)
  {
    TBSYS_LOG(ERROR, "file not open");
    return ret;
  }

  if ((ret = local_key_.get_segment_for_write(offset_, reinterpret_cast<const char*>(buf), count, seg_list_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get segment error, ret: %d", ret);
    return ret;
  }

  int32_t seg_count = seg_list_.size();
  if (!seg_count)
  {
    TBSYS_LOG(DEBUG, "data already written, offset:%d, size: %d", offset_, count);
    return ret;
  }

  if ((ret = tfs_session_->batch_get_block_info(seg_list_, flags_)) != TFS_SUCCESS)
  {
    TBSYS_LOG(ERROR, "batch get block info error, count:%d, flag:%d", seg_count, flags_);
    return ret;
  }

  // int64_t current_count = 0;
  // int64_t remain_count = count;
  // while (remain_count > 0)
  // {
  //   if (remain_count < BATCH_SIZE)
  //   {
  //     current_count = remain_count;
  //   }
  //   else
  //   {
  //     current_count = BATCH_SIZE;
  //   }

  //   int64_t remainder = current_count % SEGMENT_SIZE;
  //   int64_t block_count = ((0 == remainder) ? current_count / SEGMENT_SIZE : current_count / SEGMENT_SIZE + 1);

  //   // open
  //   std::multimap<uint32_t, VUINT64> segments;
  //   int ret = batch_open(block_count, segments);
  //   if (TFS_SUCCESS != ret)
  //   {
  //     //do retry?
  //   }
  //   else
  //   {
  //     //create tfs file
  //     int index = 0;
  //     std::multimap<uint32_t, VUINT64>::iterator mit = segments.begin();
  //     current_files_.clear();
  //     int64_t current_remain_size = current_count;
  //     for ( ; mit != segments.end() && current_remain_size > 0; ++mit)
  //     {
  //       int64_t local_block_size = (current_remain_size >= SEGMENT_SIZE) ? SEGMENT_SIZE : current_remain_size;
  //       TfsSmallFile* small_file = new TfsSmallFile(mit->first, mit->second, buf + index * SEGMENT_SIZE, local_block_size);
  //       current_files_.push_back(small_file);
  //       ++index;
  //       current_remain_size -= local_block_size;
  //     }
  //   }

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

  for (int32_t i = 0; i < seg_list_.size(); i++)
  {
    if (seg_list_[i]->status_)
    {
      local_key_.add_segment(seg_list_[i]->seg_info_);
    }
  }

  local_key_.save();
  return ret;
}

int64_t TfsLargeFile::lseek(int64_t offset, int whence)
{

}

int64_t TfsLargeFile::pread(void* buf, int64_t count, int64_t offset)
{

}

int64_t TfsLargeFile::pwrite(const void* buf, int64_t count, int64_t offset)
{

}

int TfsLargeFile::close()
{

}
