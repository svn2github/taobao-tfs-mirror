/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
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
#ifndef TFS_DATASERVER_MMAPFILE_H_
#define TFS_DATASERVER_MMAPFILE_H_

#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {
    class MMapFile
    {
      public:
        MMapFile();
        explicit MMapFile(const int fd);
        MMapFile(const MMapOption& mmap_option, const int fd);
        ~MMapFile();

        bool sync_file();
        bool map_file(const bool write = false);
        bool remap_file();
        void* get_data() const;
        int32_t get_size() const;
        bool munmap_file();

      private:
        bool ensure_file_size(const int32_t size);

      private:
        int32_t size_;
        int fd_;
        void* data_;
        MMapOption mmap_file_option_;
    };
  }
}
#endif //TFS_DATASERVER_MMAPFILE_H_
