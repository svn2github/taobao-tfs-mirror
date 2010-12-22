#include "local_key.h"
#include "common/error_msg.h"
#include <unistd.h>

using namespace tfs::client;
using namespace tfs::common;

LocalKey::LocalKey()
{
}

//LocalKey::LocalKey(const char* local_key, const uint64_t addr)
//{
//  initialize(local_key, addr);
//}

LocalKey::~LocalKey()
{
  tbsys::gDelete(file_op_);
  destroy_info();
}

int LocalKey::initialize(const char* local_key, const uint64_t addr)
{
  const static char* g_tmp_path = "/tmp";
  char name[MAX_PATH_LENGTH];
  strncpy(name, g_tmp_path, MAX_PATH_LENGTH - 1);
  char* tmp_file = name + strlen(g_tmp_path);

  int ret = TFS_SUCCESS;
  if (!realpath(local_key, tmp_file))
  {
    TBSYS_LOG(ERROR, "initialize local key %s fail: %s", local_key, strerror(errno));
    ret = TFS_ERROR;
  }

  if (TFS_SUCCESS == ret)
  {
    // convert tmp file name
    char* pos = NULL;
    while ((pos = strchr(tmp_file, '/')))
    {
      tmp_file = pos;
      *pos = '!';
    }
    snprintf(name + strlen(name), MAX_PATH_LENGTH - strlen(name), "%" PRI64_PREFIX "u", addr);

    memset(&seg_head_, 0, sizeof(SegmentHead));
    seg_info_.clear();
    int is_exist = access(name, F_OK);    
    if (0 != is_exist) //not exist
    {
      file_op_ = new FileOperation(name, O_RDWR|O_CREAT);
    }
    else
    {
      file_op_ = new FileOperation(name, O_RDWR);
      ret = load();
    }
  }

  return ret;
}

int LocalKey::load()
{
  int ret = TFS_SUCCESS;
  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "local key file path not initialize");
    ret = TFS_ERROR;
  }

  if (TFS_SUCCESS == ret)
  {
    if ((ret = file_op_->pread_file(reinterpret_cast<char*>(&seg_head_), sizeof(SegmentHead), 0)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load segment head fail, ret: %d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "load segment count %d, size: %"PRI64_PREFIX"d", seg_head_.count_, seg_head_.size_);

      char* buf = new char[sizeof(SegmentInfo)*seg_head_.count_];
      if ((ret = file_op_->pread_file(buf, sizeof(SegmentInfo) * seg_head_.count_, sizeof(SegmentHead))) != TFS_SUCCESS)
      {
        TBSYS_LOG(ERROR, "load segment info fail, ret: %d", ret);
      }
      else
      {
        ret = load_segment(buf);
      }
      tbsys::gDelete(buf);
    }
  }
  return ret;
}

int LocalKey::load(const char* buf)
{
  load_head(buf);
  return load_segment(buf + sizeof(SegmentHead));
}

int LocalKey::add_segment(SegmentInfo& seg_info)
{
  int ret = seg_info_.insert(seg_info).second ? TFS_SUCCESS : TFS_ERROR;
  if (TFS_SUCCESS == ret)
  {
    seg_head_.count_++;         // TODO .. lock ?
    seg_head_.size_ += seg_info.size_;
  }
  return ret;
}

int LocalKey::save()
{
  int ret = TFS_SUCCESS;

  if (NULL == file_op_)
  {
    TBSYS_LOG(ERROR, "local save file path not initialize");
    ret = TFS_ERROR;
  }
  else
  {
    int32_t size = get_data_size();
    char* buf = new char[size];
    dump_data(buf);

    if ((ret = file_op_->pwrite_file(buf, size, 0)) != TFS_SUCCESS)
    {
      TBSYS_LOG(ERROR, "save segment info fail, count: %d, size: %d, ret: %d", seg_info_.size(), size, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "save segment info successful, count: %d, size: %d", seg_info_.size(), size);
      file_op_->flush_file();
    }

    tbsys::gDelete(buf);
  }
  return ret;
}

int LocalKey::remove()
{
  return file_op_->unlink_file();
}

int64_t LocalKey::get_file_size()
{
  return seg_head_.size_;
}

int32_t LocalKey::get_data_size()
{
  return sizeof(SegmentHead) + seg_info_.size() * sizeof(SegmentInfo);
}

int LocalKey::dump_data(char* buf)
{
  memcpy(buf, &seg_head_, sizeof(SegmentHead));
  char* pos = buf + sizeof(SegmentHead);
  SEG_SET_ITER it;
  for (it = seg_info_.begin(); it != seg_info_.end(); ++it)
  {
    memcpy(pos, &(*it), sizeof(SegmentInfo));
    pos += sizeof(SegmentInfo);
  }
  return TFS_SUCCESS;
}

int LocalKey::get_segment_for_write(const int64_t offset, const char* buf,
                                    int64_t size, SEG_DATA_LIST& seg_list)
{
  int64_t cur_offset = offset, next_offset = offset, remain_size = size, last_remain_size = size;
  const char* cur_buf = buf;
  SegmentInfo seg_info;
  SEG_SET_ITER it;

  while (remain_size > 0)
  {
    last_remain_size = remain_size;
    next_offset = cur_offset;
    seg_info.offset_ = cur_offset;

    if ((it = seg_info_.lower_bound(seg_info)) == seg_info_.end())
    {
      next_offset = cur_offset + remain_size;
    }
    else
    {
      if (it->offset_ == cur_offset)
      {
        remain_size -= it->size_;
      }
      else
      {
        if (seg_info_.begin() == it)
        {
          next_offset = it->offset_;
        }
        else
        {
          SEG_SET_ITER pre_it = it;
          pre_it--;
          if (pre_it->offset_ + pre_it->size_ > cur_offset)
          {
            remain_size -= pre_it->size_ - (cur_offset - pre_it->offset_);
          }
          else
          {
            next_offset = it->offset_;
          }
        }
      }
    }
    get_segment(cur_offset, next_offset, cur_buf, remain_size, seg_list);
    cur_buf += last_remain_size - remain_size;
    cur_offset += last_remain_size - remain_size;
  }
  return TFS_SUCCESS;
}

int LocalKey::get_segment_for_read(const int64_t offset, const char* buf,
                                   const int64_t size, SEG_DATA_LIST& seg_list)
{
  SegmentInfo seg_info;
  seg_info.offset_ = offset;
  SEG_SET_ITER it = seg_info_.lower_bound(seg_info);

  if (seg_info_.end() == it)
  {
    TBSYS_LOG(ERROR, "can not find meta info for offset: %"PRI64_PREFIX"d", offset);
    return TFS_ERROR;
  }

  int64_t check_size = 0, cur_size = 0;
  SegmentData* seg_data = NULL;

  if (it->offset_ != offset)
  {
    if (seg_info_.begin() == it) // should never happen
    {
      TBSYS_LOG(ERROR, "can not find meta info for offset: %"PRI64_PREFIX"d", offset);
      return TFS_ERROR;
    }
    else
    {
      SEG_SET_ITER pre_it = it;
      it--;
      check_size += pre_it->size_ - (offset - pre_it->offset_);
      seg_data = new SegmentData();
      seg_data->buf_ = const_cast<char*>(buf);
      seg_list.push_back(seg_data);
    }
  }
  while (it != seg_info_.end() && check_size < size)
  {
    if (check_size + it->size_ > size)
    {
      cur_size = it->size_ - check_size;
    }
    else
    {
      cur_size = it->size_;
    }

    seg_data = new SegmentData();
    seg_data->seg_info_ = *it;
    seg_data->seg_info_.size_ = cur_size;
    seg_data->buf_ = const_cast<char*>(buf) + check_size;

    check_size += cur_size;
  }

  return TFS_SUCCESS;
}

void LocalKey::get_segment(const int64_t start, const int64_t end,
                         const char* buf, int64_t& size, SEG_DATA_LIST& seg_list)
{
  if (start < end)
  {
    int64_t offset = start, cur_size = 0, check_size = 0;
    SegmentData* seg_data = NULL;
    bool not_end = true;

    while (not_end)
    {
      if (offset + SEGMENT_SIZE > end || // reach file offset end
          check_size + SEGMENT_SIZE > size) // reach buffer offset end
      {
        cur_size = min(end - offset, size - check_size);
        not_end = false;
      }
      else
      {
        cur_size = SEGMENT_SIZE;
      }

      seg_data = new SegmentData();
      seg_data->seg_info_.offset_ = offset;
      seg_data->seg_info_.size_ = cur_size;
      seg_data->buf_ = const_cast<char*>(buf) + check_size;
      seg_data->cur_size_ = cur_size; // cur_offset = 0
      seg_list.push_back(seg_data);
      check_size += cur_size;
    }
    size -= check_size;
  }
}

int LocalKey::load_head(const char* buf)
{
  memcpy(&seg_head_, buf, sizeof(SegmentHead));
  TBSYS_LOG(DEBUG, "load segment head, count %d, size: %"PRI64_PREFIX"d", seg_head_.count_, seg_head_.size_);
  return TFS_SUCCESS;
}

int LocalKey::load_segment(const char* buf)
{
  int ret = TFS_SUCCESS;
  // clear last segment info ?
  destroy_info();

  int32_t count = seg_head_.count_;
  const SegmentInfo* segment = reinterpret_cast<const SegmentInfo*>(buf);
  for (int32_t i = 0; i < count; ++i)
  {
    TBSYS_LOG(DEBUG, "load segment info, offset: %"PRI64_PREFIX"d, blockid: %u, fileid: %"PRI64_PREFIX"u",
              segment[i].offset_, segment[i].block_id_, segment[i].file_id_);
    if (!seg_info_.insert(*(segment + i)).second)
    {
      TBSYS_LOG(ERROR, "load segment info fail, count: %d, failno: %d", count, i + 1);
      ret = TFS_ERROR;
      break;
    }
  }
  return ret;
}

void LocalKey::destroy_info()
{
  seg_info_.clear();
}
