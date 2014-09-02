/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: file_op.h 33 2010-11-01 05:24:35Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_FILE_OPV2_H_
#define TFS_DATASERVER_FILE_OPV2_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include "internal.h"

namespace tfs
{
  namespace common
  {
    class FileOperation
    {
      public:
        FileOperation(const std::string& path, const int open_flags = O_RDWR | O_LARGEFILE);
        virtual ~FileOperation();
        int32_t open();
        int  close();
        int  unlink();
        int64_t size();
        int fdatasync();
        int fsync();
        int fsync_file_range(const int64_t offset, const int64_t nbytes, const int32_t flag);
        int ftruncate(const int64_t length);
        int write(const char* buf, const int32_t nbytes);
        int pread(char* buf, const int32_t nbytes, const int64_t offset);
        int pwrite(const char* buf, const int32_t nbytes, const int64_t offset);
        inline std::string& get_path() { return path_;}
        inline int32_t get_fd() const { return fd_;}

      private:
        DISALLOW_COPY_AND_ASSIGN(FileOperation);

      protected:
        int32_t check_() ;
        void check_return_value_(const int32_t ret) const;
        static const int MAX_DISK_TIMES = 5;
        static const int OPEN_MODE = 0644;
        std::string path_;//file path
        int32_t fd_;      //file handle
        int32_t flag_;    //open mode
    };
  }/** end namespace common **/
}/** end namespace tfs **/
#endif //TFS_DATASERVER_FILE_OP_H_
