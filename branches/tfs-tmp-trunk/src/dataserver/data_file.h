/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_file.h 356 2011-05-26 07:51:09Z daoan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_DATAFILE_H_
#define TFS_DATASERVER_DATAFILE_H_

#include "common/internal.h"

namespace tfs
{
  namespace dataserver
  {
    class DataFile
    {
      public:
        DataFile(const uint64_t fn, const std::string& work_dir);
        virtual ~DataFile();

        int pwrite(const common::FileInfoInDiskExt& info, const char* data, const int32_t nbytes, const int32_t offset);
        int pread(char*& data, int32_t& nbytes, const int32_t offset);

        inline int32_t length() const { return length_;}
        inline uint32_t crc() const { return crc_;}
      private:
        int32_t fd_;
        int32_t length_;        // current max buffer write length
        uint32_t crc_;          // crc checksum
        std::stringstream path_;// file path
        static const int32_t WRITE_DATA_TMPBUF_SIZE = 2 * 1024 * 1024;
        char data_[WRITE_DATA_TMPBUF_SIZE]; // data buffer
        DataFile();
        DISALLOW_COPY_AND_ASSIGN(DataFile);
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif //TFS_DATASERVER_DATAFILE_H_
