/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_op.cpp 33 2010-11-01 05:24:35Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <Memory.hpp>

#include "error_msg.h"
#include "file_opv2.h"

namespace tfs
{
  namespace common
  {
    FileOperation::FileOperation(const std::string& path, const int flag):
      path_(path),
      fd_(-1),
      flag_(flag)
    {

    }

    FileOperation::~FileOperation()
    {
      if (fd_ >= 0)
      {
        ::close(fd_);
        fd_ = -1;
      }
    }

    //返回值 < 0: 错误码, >= 0: 读出数据长度
    int FileOperation::pread(char* buf, const int32_t nbytes, const int64_t offset)
    {
      int32_t total = 0;
      int32_t ret = (NULL != buf && nbytes >= 0 && offset >= 0) ? 0 : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t retry_times = MAX_DISK_TIMES;
        while (0 == ret && total < nbytes && retry_times-- >= 0)
        {
          ret = check_();
          if (ret < 0)
          {
            ret = -errno;
          }
          else
          {
            ret = 0;
            int32_t length = ::pread64(fd_, (buf + total), (nbytes - total), (offset + total));
            if (length < 0)
            {
              ret = -errno;
              if (EINTR == -ret || EAGAIN == -ret || EBADF == -ret)
              {
                ret = 0;
                if (EBADF == -ret)
                  fd_ = -1;
              }
            }
            if (0 == length)
            {
              break;
            }

            if ( length > 0)
            {
              total += length;
            }
          }
          check_return_value_(ret);
        }
      }
      return  ret < 0 ? ret : total;
    }

    //返回值 < 0: 错误码, >= 0: 写入数据长度
    int FileOperation::pwrite(const char* buf, const int32_t nbytes, const int64_t offset)
    {
      int32_t total = 0;
      int32_t ret = (NULL != buf && nbytes >= 0 && offset >= 0) ? 0 : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t retry_times = MAX_DISK_TIMES;
        while (0 == ret && total < nbytes && retry_times-- >= 0)
        {
          ret = check_();
          if (ret < 0)
          {
            ret = -errno;
          }
          else
          {
            ret = 0;
            int32_t length = ::pwrite64(fd_, (buf + total), (nbytes - total), (offset + total));
            if (length < 0)
            {
              ret = -errno;
              if (EINTR == -ret || EAGAIN == -ret || EBADF == -ret)
              {
                ret = 0;
                if (EBADF == -ret)
                  fd_ = -1;
              }
            }
            if (0 == length)
            {
              break;
            }

            if ( length > 0)
            {
              total += length;
            }
          }
          check_return_value_(ret);
        }
      }
      return  ret < 0 ? ret : total;
    }

    int FileOperation::write(const char* buf, const int32_t nbytes)
    {
      int32_t total = 0;
      int32_t ret = (NULL != buf && nbytes >= 0) ? 0 : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t retry_times = MAX_DISK_TIMES;
        while (0 == ret && total < nbytes && retry_times-- >= 0)
        {
          ret = check_();
          if (ret < 0)
          {
            ret = -errno;
          }
          else
          {
            ret = 0;
            int32_t length = ::write(fd_, (buf + total), (nbytes - total));
            if (length < 0)
            {
              ret = -errno;
              if (EINTR == -ret || EAGAIN == -ret || EBADF == -ret)
              {
                ret = 0;
                if (EBADF == -ret)
                  fd_ = -1;
              }
            }
            if (0 == length)
            {
              break;
            }

            if ( length > 0)
            {
              total += length;
            }
          }
          check_return_value_(ret);
        }
      }
      return  ret < 0 ? ret : total;
    }

    int64_t FileOperation::size()
    {
      int64_t ret = check_();
      if (ret >= 0)
      {
        struct stat statbuf;
        ret = fstat(fd_, &statbuf);
        if (0 == ret)
        {
          ret = statbuf.st_size;
        }
      }
      return ret;
    }

    int FileOperation::ftruncate(const int64_t length)
    {
      int32_t ret = check_();
      if (ret >= 0)
        ret = ::ftruncate(fd_, length);
      return ret;
    }

    int FileOperation::fsync()
    {
      int32_t ret = flag_ & O_SYNC ? 0 : -1;
      if (ret < 0)
      {
        ret = check_();
        if (ret >= 0)
        {
          ret = ::fsync(ret);
        }
      }
      return ret;
    }

    int FileOperation::fdatasync()
    {
      int32_t ret = flag_ & O_SYNC ? 0 : -1;
      if (ret < 0)
      {
        ret = check_();
        if (ret >= 0)
        {
          ret = ::fdatasync(ret);
        }
      }
      return ret;
    }

    int FileOperation::unlink()
    {
      int32_t ret = this->close();
      if (0 == ret)
      {
        ret = ::unlink(path_.c_str());
      }
      return ret;
    }

    int32_t FileOperation::open()
    {
      if (fd_ >= 0)
        ::close(fd_);
      fd_ = ::open(path_.c_str(), flag_, OPEN_MODE);
      if (fd_ < 0)
        fd_ = -errno;
      return fd_;
    }

    int FileOperation::close()
    {
      int32_t ret = 0;
      if (fd_ >= 0)
      {
        ret = ::close(fd_);
        fd_ = -1;
      }
      return ret;
    }

    int32_t FileOperation::check_()
    {
      return fd_ < 0 ? this->open() : fd_;
    }

    void FileOperation::check_return_value_(const int32_t ret) const
    {
      if (EPERM == -ret || EIO == -ret || ENXIO == -ret
          || EACCES == -ret || EROFS == -ret)
      {
        TBSYS_LOG(WARN, "current file system mybe invalid,we'll shutdown, error: %s", strerror(ret));
        assert(false);
      }
    }
  }/** end namespace common **/
}/** end namespace tfs **/
