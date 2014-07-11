/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duafnei@taobao.com
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include "ds_define.h"
#include "data_file.h"
#include "common/error_msg.h"
#include "common/directory_op.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    class TestDataFile: public ::testing::Test
    {
      public:
        TestDataFile()
        {
        }
        virtual ~TestDataFile()
        {
        }
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {

        }
    };

    TEST_F(TestDataFile, write_read)
    {
      DirectoryOp::create_directory("./tmp/", 0740);
      srandom(time(NULL));
      const int32_t WRITE_DATA_TMPBUF_SIZE = DataFile::WRITE_DATA_TMPBUF_SIZE;
      const int32_t MAX_DATA_SIZE = WRITE_DATA_TMPBUF_SIZE + 1024;
      FileInfoInDiskExt ext;
      DataFile file(0xff, ".");
      char* data = new char [MAX_DATA_SIZE];
      for (int32_t i = 0; i < MAX_DATA_SIZE; ++i)
        data[i] = random() % 256;
      int32_t length = 2048, offset = 0;
      int32_t ret = file.pwrite(ext, data, length, offset);
      EXPECT_EQ(length, ret);
      offset += length;
      length = MAX_DATA_SIZE - length;
      ret = file.pwrite(ext, data, length, offset);
      EXPECT_EQ(length, ret);

      char* pdata = NULL;
      int32_t nbytes = 2048;
      offset = 0;
      ret = file.pread(pdata, nbytes, offset);
      EXPECT_TRUE(NULL != pdata);
      EXPECT_EQ(2048, nbytes);
      uint32_t new_crc = 0;
      new_crc = Func::crc(new_crc, pdata, nbytes);
      offset += nbytes;
      nbytes = (MAX_DATA_SIZE + sizeof(ext)) - nbytes;
      pdata = NULL;
      ret = file.pread(pdata, nbytes, offset);
      EXPECT_TRUE(NULL != pdata);
      EXPECT_EQ((MAX_DATA_SIZE + (int32_t)sizeof(ext)) - offset, nbytes);
      new_crc = Func::crc(new_crc, pdata, nbytes);
      EXPECT_EQ(file.crc(), new_crc);
      delete [] data;
      DirectoryOp::delete_directory_recursively("./tmp/", true);
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
