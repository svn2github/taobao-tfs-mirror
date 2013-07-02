/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: mmap_file_op.h 764 2011-09-07 06:04:06Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *   duanfei < duanfei@taobao.com>
 *      - modify 2012-10-22
 *
 */
#ifndef TFS_COMMON_MMAP_FILE_H_
#define TFS_COMMON_MMAP_FILE_H_

#include "file_opv2.h"
#include "mmap_file.h"
#include <Memory.hpp>

namespace tfs
{
  namespace common
  {
    class MMapFileOperation: public FileOperation
    {
      public:
        MMapFileOperation(const std::string& file_name, int open_flags = O_RDWR | O_LARGEFILE);
        virtual ~MMapFileOperation();

        char* get_data() const;
        int mmap(const common::MMapOption& mmap_option);
        int mremap();
        int munmap();
        int rename();
        int flush();
        int64_t length() const;
      private:
        MMapFileOperation();
        DISALLOW_COPY_AND_ASSIGN(MMapFileOperation);
        common::MMapFile* map_file_;
    };
  }/** end namespace common**/
}/** end namespace tfs **/
#endif //TFS_COMMON_MMAP_FILE_H_
