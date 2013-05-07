/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file.h 552 2011-06-24 08:44:50Z duanfei@taobao.com $
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
#ifndef TFS_COMMON_MMAPFILE_H_
#define TFS_COMMON_MMAPFILE_H_

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "internal.h"

namespace tfs
{
  namespace common
  {
    class MMapFile
    {
      public:
        MMapFile(const MMapOption& mmap_option, const int fd);
        virtual ~MMapFile();

        inline char* get_data() const { return (NULL != data_) ? static_cast<char*>(data_) : NULL;}
        inline int64_t length() const { return length_;}

        int mmap(const bool write = false);
        int munmap();
        int mremap();
        int msync();

      private:
        int ensure_file_size_(const int64_t size);
        MMapFile();
        DISALLOW_COPY_AND_ASSIGN(MMapFile);
        void*  data_;
        int32_t length_;
        int32_t fd_;
        MMapOption option_;
    };
  } /** common **/
} /** tfs **/
#endif //TFS_COMMON_MMAPFILE_H_
