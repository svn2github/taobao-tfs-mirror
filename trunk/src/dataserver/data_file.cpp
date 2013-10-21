/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_file.cpp 706 2011-08-12 08:24:41Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "common/func.h"
#include "common/error_msg.h"
#include "common/parameter.h"

#include "data_file.h"

using namespace tfs::common;
namespace tfs
{
  namespace dataserver
  {
    DataFile::DataFile(const uint64_t fn, const uint64_t block_id, const uint64_t file_id, const std::string& work_dir):
      fd_(-1),
      length_(0),
      crc_(0),
      status_(-1)
    {
      atomic_set(&ref_count_, 0);
      path_ << work_dir << "/tmp/" << fn << "_" << block_id << "_" << file_id;
    }

    DataFile::~DataFile()
    {
      length_ = 0;
      if (fd_ >= 0)
      {
        ::close(fd_);
        fd_ = -1;
        ::unlink(path_.str().c_str());
      }
    }

    int DataFile::pwrite(const FileInfoInDiskExt& info, const char *data, const int32_t nbytes, const int32_t offset)
    {
      tbutil::Mutex::Lock Lock(mutex_);
      int32_t ret = (NULL != data && nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (length_ > 0) ? (offset + static_cast<int32_t>(sizeof(info)) >= length_) ? TFS_SUCCESS : EXIT_WRITE_OFFSET_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          int32_t real_offset = sizeof(info) + offset;
          int32_t length = real_offset + nbytes;
          if (length <= WRITE_DATA_TMPBUF_SIZE)
          {
            if (0 == offset)
            {
              memcpy(data_, &info, sizeof(info));
              memcpy((data_ + real_offset), data, nbytes);
            }
            else
            {
              memcpy((data_ + real_offset), data, nbytes);
            }
            crc_ = Func::crc(crc_, data, nbytes);
          }
          else
          {
            if (fd_ < 0)
            {
              fd_ = open(path_.str().c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
              ret = fd_ >= 0 ? TFS_SUCCESS : -errno;
              if (TFS_SUCCESS == ret)
              {
                if (length_ <= 0)
                {
                  memcpy(data_, &info, sizeof(info));
                  length_ += sizeof(info);
                }
                ret = (length_ == ::pwrite(fd_, data_, length_, 0)) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
              }
            }
            if (TFS_SUCCESS == ret)
            {
              ret = fd_ >= 0 ? TFS_SUCCESS : EXIT_NOT_OPEN_ERROR;
            }
            if (TFS_SUCCESS == ret)
            {
              int32_t wnbytes = ::pwrite(fd_, data, nbytes, real_offset);
              ret = (nbytes == wnbytes) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
              if (TFS_SUCCESS == ret)
              {
                crc_ = Func::crc(crc_, data, nbytes);
              }
            }
          }

          if (TFS_SUCCESS == ret)
          {
            length_ = std::max(length_, length);
          }
        }
      }
      return TFS_SUCCESS == ret ? nbytes : ret;
    }

    int DataFile::pread(char*& data, int32_t& nbytes, const int32_t offset)
    {
      tbutil::Mutex::Lock Lock(mutex_);
      data = NULL;
      int32_t ret = (nbytes > 0 && offset >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = offset >= length_ ? EXIT_READ_FILE_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (fd_ >= 0)
          {
            const int32_t max = WRITE_DATA_TMPBUF_SIZE;
            nbytes = std::min(nbytes, max);
            int32_t length = ::pread(fd_, data_, nbytes, offset);
            ret = length > 0 ? TFS_SUCCESS : EXIT_READ_FILE_ERROR;
            if (TFS_SUCCESS == ret)
            {
              nbytes = length;
              data   = data_;
            }
          }
          else
          {
            nbytes = std::min(nbytes, length_);
            data   = data_;
          }
        }
      }
      return (TFS_SUCCESS == ret) ? nbytes : ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
