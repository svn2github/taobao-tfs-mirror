/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_erasure_code.cpp 5 2012-07-19 07:44:56Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing.zyd
 *      - initial release
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>

#include "error_msg.h"
#include "erasure_code.h"

using namespace tfs::common;
using namespace tfs::dataserver;

char* gen_random_data(const int size)
{
  char* data = (char*)malloc(size * sizeof(char));
  srand(time(NULL));
  for (int i = 0; i < size; i++)
  {
    data[i] = rand() % 128;
  }
  return data;
}

char* copy_data(const char* data, const int size)
{
  char* dest = (char*)malloc(size * sizeof(char));
  memcpy(dest, data, size);
  return dest;
}

bool compare_data(const char* left, const char* right, const int size)
{
  bool same = true;
  for (int i = 0; i < size; i++)
  {
    if (left[i] != right[i])
    {
      same = false;
      break;
    }
  }
  return same;
}

class ErasureCodeTest: public ::testing::Test
{
  public:
    ErasureCodeTest()
    {
    }
    ~ErasureCodeTest()
    {
    }
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};

TEST_F(ErasureCodeTest, coding)
{
  int ret = 0;
  const int k = 5;
  const int m = 3;
  const int size = 1048576;
  char **data = NULL;
  data = (char**)malloc((k+m) * sizeof(char*));
  ASSERT_TRUE(NULL != data);

  ErasureCode encoder;

  for (int i = 0; i < k + m; i++)
  {
    data[i] = gen_random_data(1048576);
    ASSERT_TRUE(NULL != data[i]);
  }

  ret = encoder.config(k, m);
  ASSERT_EQ(TFS_SUCCESS, ret);

  // bind data
  encoder.bind(data, k + m, size);

  // encode
  ret = encoder.encode(size);
  ASSERT_EQ(TFS_SUCCESS, ret);

  //////////////////////////////////////////////////////////

  // erase m random disks
  int erased[k + m];
  memset(erased, 0, sizeof(int) * (k + m));

//  int test[m] = {5, 6};
  char* src[m];
  int erasures[m];
  srand(time(NULL));
  for (int i =  0; i < m; i++)
  {
    int dest = rand() % (k + m);
    // int dest = test[i];
    erased[dest] = 1;
    erasures[i] = dest;
    src[i] = copy_data(data[dest], size);
    printf("disk %d erased\n", dest);
  }

  // reset data for recovery test
  for (int i = 0; i < k + m ; i++)
  {
    if (erased[i])
    {
      bzero(data[i], size);
    }
  }

  ErasureCode decoder;
  ret = decoder.config(k, m, erased);
  ASSERT_EQ(TFS_SUCCESS, ret);

  decoder.bind(data, k + m, size);

  ret = decoder.decode(size);
  ASSERT_EQ(TFS_SUCCESS, ret);

  // compare data
  for (int i = 0; i < m; i++)
  {
    EXPECT_TRUE(compare_data(src[i], data[erasures[i]], size));
  }

}

TEST_F(ErasureCodeTest, excepiton)
{
  const int k = 5;
  const int m = 3;
  const int size = 8192;
  char *data[k+m];
  int ret = TFS_SUCCESS;

  for (int i = 0; i < m; i++)
  {
    data[i] = gen_random_data(size);
  }

  ErasureCode encoder;
  ret = encoder.config(k, m);
  ASSERT_EQ(TFS_SUCCESS, ret);

  encoder.bind(data, k + m, size);

  // set a bad data
  encoder.bind((char*)NULL, 0, 0);

  ret = encoder.encode(size);
  ASSERT_EQ(EXIT_DATA_INVALID, ret);

  // rebind good data
  encoder.bind(data, k + m, size);

  ret = encoder.encode(size + 1024);
  ASSERT_EQ(EXIT_DATA_INVALID, ret);

  ret = encoder.encode(size - 100);  // must multiple of ws*ps
  ASSERT_EQ(EXIT_SIZE_INVALID, ret);

  /////////////////////////////////

  ErasureCode decoder;
  int erased[k+m] = {0, 0, 0, 1, 1, 1, 0, 1};
  ret = decoder.config(k, m, erased);
  ASSERT_EQ(EXIT_NO_ENOUGH_DATA, ret);

  int erased2[k+m] = {0, 0, 0, 0, 1, 0, 1, 1};
  ret = decoder.config(k, m, erased2);
  ASSERT_EQ(TFS_SUCCESS, ret);

  decoder.bind(data, k + m, size);

  ret = decoder.decode(size + 1024);
  ASSERT_EQ(EXIT_DATA_INVALID, ret);

  ret = decoder.decode(size - 100);
  ASSERT_EQ(EXIT_SIZE_INVALID, ret);
}



int main(int argc, char* argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
