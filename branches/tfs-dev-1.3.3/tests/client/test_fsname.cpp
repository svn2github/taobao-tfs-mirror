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
 *   zhuhui <zhuhui_a.pt@taobao.com>
 *      - initial release
 *
 */
#include <gtest/gtest.h>
#include <string.h>
#include <stdio.h>
#include "fsname.h"

#define FILE_NAME_LEN 18

using namespace tfs::client;

class FSNameTest: public ::testing::Test
{
  public:
    FSNameTest()
    {
    }
    ~FSNameTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(FSNameTest, testencode)
{
  char tmp_buf[FILE_NAME_LEN + 1];

  for (int bid = 0; bid < 100; bid++)
  {
    for (int seqid = 0; seqid < 100; seqid++)
    {
      for (int suffix = 0; suffix < 10; suffix++)
      {
        FSName fsname(bid * 1000 + bid, seqid, suffix, suffix);
        const char *pname = fsname.get_name();
        strcpy(tmp_buf, pname);

        FSName fn2(pname);
        EXPECT_EQ(bid * 1000 + bid, static_cast<int32_t>(fn2.get_block_id()));
        EXPECT_EQ(seqid, fn2.get_seq_id());
        EXPECT_EQ(suffix, fn2.get_prefix());
        printf("%s\n", tmp_buf);
      }
    }
  }
}

int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
