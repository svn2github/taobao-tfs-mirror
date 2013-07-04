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

#include "Mutex.h"
#include "common/internal.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    class DataFile
    {
      #ifdef TFS_GTEST
      friend class TestDataFile;
      FRIEND_TEST(TestDataFile, write_read);
      #endif
      public:
        DataFile(const uint64_t fn, const std::string& work_dir);
        virtual ~DataFile();

        int pwrite(const common::FileInfoInDiskExt& info, const char* data, const int32_t nbytes, const int32_t offset);
        int pread(char*& data, int32_t& nbytes, const int32_t offset);

        inline int32_t length() const { tbutil::Mutex::Lock Lock(mutex_); return length_;}
        inline uint32_t crc() const { tbutil::Mutex::Lock Lock(mutex_); return crc_;}

        inline int32_t get_last_update() const
        {
          return last_update_;
        }

        inline void set_last_update()
        {
          last_update_ = time(NULL);
        }

        inline int add_ref()
        {
          return atomic_add_return(1, &ref_count_);
        }

        inline void sub_ref()
        {
          atomic_dec(&ref_count_);
        }

        inline int get_ref() const
        {
          return atomic_read(&ref_count_);
        }

        inline void set_status(const int32_t status)
        {
          status_ = status;
        }

        inline int32_t get_status() const
        {
          return status_;
        }

      private:
        tbutil::Mutex mutex_;
        atomic_t ref_count_;    // reference count, only for compatible
        int32_t last_update_;   // last update time, only for compatible
        int32_t fd_;            // temp file descriptor
        int32_t length_;        // current max buffer write length
        uint32_t crc_;          // crc checksum
        int32_t status_;        // set to file when close called
        std::stringstream path_;// file path
        static const int32_t WRITE_DATA_TMPBUF_SIZE = 2 * 1024 * 1024 + 128;
        char data_[WRITE_DATA_TMPBUF_SIZE]; // data buffer
        DataFile();
        DISALLOW_COPY_AND_ASSIGN(DataFile);
    };

       typedef __gnu_cxx::hash_map<uint64_t, DataFile*, __gnu_cxx::hash<int> > DataFileMap;
       typedef DataFileMap::iterator DataFileMapIter;
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif //TFS_DATASERVER_DATAFILE_H_
