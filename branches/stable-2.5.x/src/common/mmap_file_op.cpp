/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file_op.cpp 326 2011-05-24 07:18:18Z daoan@taobao.com $
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
#include "mmap_file_op.h"
#include "error_msg.h"
#include "func.h"

namespace tfs
{
  namespace common
  {
    MMapFileOperation::MMapFileOperation(const std::string& file_name, int open_flags):
      FileOperation(file_name, open_flags),
      map_file_(NULL)
    {

    }

    MMapFileOperation::~MMapFileOperation()
    {
      tbsys::gDelete(map_file_);
    }

    char* MMapFileOperation::get_data() const
    {
      return (NULL != map_file_) ? map_file_->get_data() : NULL;
    }

    int MMapFileOperation::mmap(const MMapOption& mmap_option)
    {
      int32_t fd = -1;
      int32_t ret = mmap_option.check() ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        fd = check_();
        ret = fd >= 0 ? TFS_SUCCESS : fd;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = (NULL == map_file_) ? TFS_SUCCESS : EXIT_ALREADY_MMAPPED_ERROR;
        if (TFS_SUCCESS == ret)
        {
          map_file_ = new MMapFile(mmap_option, fd);
          ret = map_file_->mmap(true);
          if (TFS_SUCCESS != ret)
            tbsys::gDelete(map_file_);
        }
      }
      return ret;
    }

    int MMapFileOperation::mremap()
    {
      int32_t fd = check_();
      int32_t ret =  fd >= 0 ? TFS_SUCCESS : fd;
      if (TFS_SUCCESS == ret)
      {
        ret = (NULL == map_file_) ? EXIT_MMAP_DATA_INVALID : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          ret = map_file_->mremap();
        }
      }
      return ret;
    }

    int MMapFileOperation::munmap()
    {
      tbsys::gDelete(map_file_);
      return TFS_SUCCESS;
    }

    int MMapFileOperation::rename()
    {
      int32_t ret = (NULL != map_file_) ? TFS_SUCCESS : EXIT_NOT_OPEN_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::string new_path(path_ + "." + Func::time_to_str(time(NULL), 1));
        ::rename(path_.c_str(), new_path.c_str());
        path_ = new_path;
      }
      return ret;
    }

    int MMapFileOperation::flush()
    {
      return (NULL != map_file_) ? map_file_->msync() : EXIT_NOT_OPEN_ERROR;
    }

    int64_t MMapFileOperation::length() const
    {
      return (NULL != map_file_) ? map_file_->length() : 0;
    }
  }/** end namespace common**/
}/** end namespace tfs **/
