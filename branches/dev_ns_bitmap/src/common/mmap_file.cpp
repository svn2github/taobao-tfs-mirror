/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file.cpp 552 2011-06-24 08:44:50Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *
 */

#include <tbsys.h>
#include "error_msg.h"
#include "mmap_file.h"

namespace tfs
{
  namespace common
  {
    MMapFile::MMapFile(const common::MMapOption& mmap_option, const int fd):
      data_(NULL),
      length_(0),
      fd_(fd),
      option_(mmap_option)
    {

    }

    MMapFile::~MMapFile()
    {
      if (NULL != data_)
      {
        memset(&option_, 0, sizeof(option_));
        ::msync(data_, length_, MS_ASYNC);
        ::munmap(data_, length_);
        data_ = NULL;
      }
    }

    int MMapFile::msync()
    {
      int32_t ret = (NULL == data_) ? EXIT_MMAP_DATA_INVALID: TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == ::msync(data_, length_, MS_ASYNC) ? TFS_SUCCESS : -errno;
      }
      return ret;
    }

    int MMapFile::mmap(const bool write)
    {
      int32_t ret = (fd_ >= 0 && option_.check()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t flags = PROT_READ;
        if (write)
          flags |= PROT_WRITE;
        length_ = option_.first_mmap_size_;
        ret = ensure_file_size_(length_);
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "ensure file size failed in mmap, fd: %d, ret: %d, lenght: %d", fd_, ret, length_);
        if (TFS_SUCCESS == ret)
        {
          data_ = ::mmap(0, length_, flags | PROT_EXEC, MAP_SHARED, fd_, 0);
          ret = (data_ != MAP_FAILED) ? TFS_SUCCESS : -errno;
          if (TFS_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "mmap failed: fd: %d, ret: %d,%s, length: %d", fd_, ret, strerror(errno), length_);
            length_ = 0;
            fd_     = 0;
            data_ = NULL;
          }
        }
      }
      TBSYS_LOG(DEBUG, "mmap file: ret: %d,%s, fd: %d, length: %d, data: %p",
          ret, TFS_SUCCESS == ret ? "successful" : "failed", fd_, length_, data_);
      return ret;
    }

    int MMapFile::mremap(const int32_t advise_per_mmap_size)
    {
      int32_t ret = (fd_ >= 0 && NULL != data_) ? TFS_SUCCESS : EXIT_MMAP_DATA_INVALID;
      if (TFS_SUCCESS != ret)
        TBSYS_LOG(WARN, "mremap not mapped yet, fd: %d, data: %p", fd_, data_);
      if (TFS_SUCCESS == ret)
      {
        if (length_ == option_.max_mmap_size_)
        {
          ret = EXIT_ALREADY_MMAPPED_MAX_SIZE_ERROR;
          TBSYS_LOG(INFO, "already mapped max size, now size: %d, max size: %d", length_, option_.max_mmap_size_);
        }
        else
        {
          int32_t new_size = length_ + std::max(option_.per_mmap_size_, advise_per_mmap_size);
          new_size = std::min(new_size, option_.max_mmap_size_);
          ret = ensure_file_size_(new_size);
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "ensure file size failed in mremap, fd: %d, ret: %d, lenght: %d", fd_, ret, new_size);
          if (TFS_SUCCESS == ret)
          {
            void* new_data = ::mremap(data_, length_, new_size, MREMAP_MAYMOVE);
            ret = (MAP_FAILED != new_data) ? TFS_SUCCESS : -errno;
            if (TFS_SUCCESS != ret)
              TBSYS_LOG(ERROR, "mremap failed: fd: %d, ret: %d,%s, length: %d", fd_, ret, strerror(errno), new_size);
            if (TFS_SUCCESS == ret)
            {
              data_ = new_data;
              length_ = new_size;
            }
          }
        }
      }
      TBSYS_LOG(INFO, "mremap file: ret: %d,%s, fd: %d, length: %d, old_length: %d, data: %p",
          ret, TFS_SUCCESS == ret ? "successful" : "failed", fd_, length_, length_- option_.per_mmap_size_, data_);
      return ret;
    }

    int MMapFile::munmap()
    {
      int32_t ret = (fd_ > 0 && NULL != data_) ? TFS_SUCCESS : EXIT_MMAP_DATA_INVALID;
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == ::munmap(data_, length_) ? TFS_SUCCESS : -errno;
      }
      return ret;
    }

    int MMapFile::ensure_file_size_(const int64_t size)
    {
      int32_t ret = (fd_ >= 0 && size >= 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        struct stat s;
        ret = fstat(fd_, &s) < 0 ? -errno : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (s.st_size < size)
          {
            ret = ftruncate(fd_, size) < 0 ? -errno : TFS_SUCCESS;
          }
        }
      }
      return ret;
    }
  } /** common **/
} /** tfs **/
