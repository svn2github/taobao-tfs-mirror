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
 *   qushan <qushan@taobao.com>
 *      - initial release
 *
 */
#include "common/func.h"
#include "tfs_large_file_cases.h"
#include <stdlib.h>
#include <tbsys.h>
#include <iostream>

using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::client;
using namespace std;
using namespace tbsys;

TfsClient* TfsLargeFileTest::tfs_client_;
static const int32_t MAX_LOOP = 20;

void TfsLargeFileTest::SetUpTestCase()
{
  srand(time(NULL) + rand() + pthread_self());
}

void TfsLargeFileTest::TearDownTestCase()
{
  ::unlink("tmplocal");
}

int32_t TfsLargeFileTest::generate_data(char* buffer, const int32_t length)
{
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int generate_length(int64_t& length, int64_t base)
{
  length = static_cast<int64_t>(rand()) * static_cast<int64_t>(rand()) % base + base;
  return 0;
}

int64_t TfsLargeFileTest::write_new_file(const bool large_flag, const int64_t length, uint32_t& crc,
                          char* tfsname, const char* suffix, const int32_t name_len)
{
  int64_t cost_time = 0, start_time = 0, end_time = 0;
  int fd = 0;
  if (large_flag) //large file
  {
    const char* key = "test_large";
    int local_fd = open(key, O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (local_fd < 0)
    {
      return local_fd;
    }
    close(local_fd);
    start_time = CTimeUtil::getTime();
    fd = tfs_client_->open(NULL, suffix, T_WRITE | T_LARGE, key);
    end_time = CTimeUtil::getTime();
  }
  else
  {
    start_time = CTimeUtil::getTime();
    fd = tfs_client_->open(NULL, suffix, T_WRITE);
    end_time = CTimeUtil::getTime();
  }
  cost_time += (end_time - start_time);
  if (fd <= 0)
  {
    return fd;
  }
  cout << "tfs open successful. large file flag: " << large_flag << " cost time " << cost_time << endl;

  int64_t ret = TFS_SUCCESS;
  char* buffer = new char[PER_SIZE];
  int64_t remain_size = length;
  int64_t cur_size = 0;
  while (remain_size > 0)
  {
    cur_size = (remain_size >= PER_SIZE) ? PER_SIZE : remain_size;
    generate_data(buffer, cur_size);
    start_time = CTimeUtil::getTime();
    ret = tfs_client_->write(fd, buffer, cur_size);
    end_time = CTimeUtil::getTime();
    if (ret != cur_size)
    {
      cout << "tfs write fail. cur_size: " << cur_size << endl;
      delete []buffer;
      return ret;
    }
    crc = Func::crc(crc, buffer, cur_size);
    uint32_t single_crc = 0;
    single_crc = Func::crc(single_crc, buffer, cur_size);
    cout << "write offset " << length - remain_size << " size " << cur_size << " write crc " << crc 
         << " single crc " << single_crc << " cost time " << end_time - start_time << endl;
    remain_size -= cur_size;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  cout << "write data successful. write length: " << length << " crc: " <<  crc << endl;

  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  start_time = CTimeUtil::getTime();
  ret = tfs_client_->close(fd, tfs_name, TFS_FILE_LEN);
  end_time = CTimeUtil::getTime();
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }
  cout << "tfs close tfsname:" << tfs_name << " cost time " << end_time - start_time << " total cost time " << cost_time << endl;
  cost_time += (end_time - start_time);
  if (NULL != tfsname)
  {
    strncpy(tfsname, tfs_name, name_len);
  }

  return length;
}

int TfsLargeFileTest::read_exist_file(const bool large_flag, const char* tfs_name, const char* suffix, uint32_t& read_crc)
{
  int64_t cost_time = 0, start_time = 0, end_time = 0;
  int fd = 0;
  start_time = CTimeUtil::getTime();
  if (large_flag)
  {
    fd = tfs_client_->open(tfs_name, suffix, T_READ | T_LARGE);
  }
  else
  {
    fd = tfs_client_->open(tfs_name, suffix, T_READ);
  }
  end_time = CTimeUtil::getTime();
  if (fd <= 0)
  {
    return fd;
  }
  cost_time += (end_time - start_time);
  cout << "tfs open successful. cost time " << cost_time << endl;

  //FileStat finfo;
  //int ret = tfs_client_->fstat(fd, &finfo, 1);
  //if (ret != TFS_SUCCESS)
  //{
  //  return ret;
  //}

  read_crc = 0;
  int64_t max_read_size = 0;
  if (large_flag)
  {
    max_read_size = PER_SIZE;
  }
  else
  {
    max_read_size = SEG_SIZE;
  }
  char* buffer = new char[PER_SIZE];
  int64_t cur_size = 0;
  int64_t offset = 0;
  int ret = 0;
  bool not_end = true;
  while (not_end)
  {
    //cur_size = PER_SIZE;
    cur_size = max_read_size;
    start_time = CTimeUtil::getTime();
    ret = tfs_client_->read(fd, buffer, cur_size);
    end_time = CTimeUtil::getTime();
    if (ret < 0)
    {
      cout << "tfs read fail. cur_size: " << cur_size << endl;
      delete []buffer;
      return ret;
    }
    else if (ret < cur_size)
    {
      cout << "tfs read reach end. cur_size: " << cur_size << " read size: " << ret << endl;
      not_end = false;
    }

    read_crc = Func::crc(read_crc, buffer, ret);
    offset += ret;
    uint32_t single_crc = 0;
    single_crc = Func::crc(single_crc, buffer, ret);
    cout << "read offset " << offset << " size " << ret << " read crc " << read_crc
         << " single crc " << single_crc << " cost time " << end_time - start_time << endl;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  start_time = CTimeUtil::getTime();
  ret = tfs_client_->close(fd);
  end_time = CTimeUtil::getTime();
  if (TFS_SUCCESS != ret)
  {
    return ret;
  }
  cost_time += (end_time - start_time);
  cout << "read length: " << offset << " read crc " << read_crc << " cost time " << cost_time << endl;

  return TFS_SUCCESS;
}

int TfsLargeFileTest::stat_exist_file(const bool large_flag, char* tfsname, tfs::common::FileStat& file_stat)
{
  int fd = 0;
  if (large_flag)
  {
    fd = tfs_client_->open(tfsname, NULL, T_STAT | T_LARGE);
  }
  else
  {
    fd = tfs_client_->open(tfsname, NULL, T_STAT);
  }
  if (fd <= 0)
  {
    return fd;
  }

  int ret = tfs_client_->fstat(fd, &file_stat, 1);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  cout << "tfsname: " << tfsname << " id: " << file_stat.file_id_ << " size: " << file_stat.size_  << " usize: " << file_stat.usize_
       << " offset: " << file_stat.offset_ << " flag: " << file_stat.flag_  << " crc: " << file_stat.crc_
       << " create time: " << file_stat.create_time_ << " modify time: " << file_stat.modify_time_ << endl;
  ret = tfs_client_->close(fd);
  return ret;
}

int TfsLargeFileTest::unlink_file(const char* tfsname, const char* suffix, const int action)
{
  return tfs_client_->unlink(tfsname, suffix, action);
}

void TfsLargeFileTest::test_read(const bool large_flag, int64_t base, const char* suffix)
{
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  uint32_t write_crc = 0;
  uint32_t read_crc = 0;
  int64_t length = 0;
  generate_length(length, base);  
  cout << "generate length: " << length << endl;
  ASSERT_EQ(length, write_new_file(large_flag, length, write_crc, tfs_name, suffix, TFS_FILE_LEN));
  ASSERT_EQ(TFS_SUCCESS, read_exist_file(large_flag, tfs_name, suffix, read_crc));
  cout << "write crc: " << write_crc << " read crc: " << read_crc << endl;
  ASSERT_EQ(write_crc, read_crc);
}

TEST_F(TfsLargeFileTest, write_small_file)
{
  int64_t length = 0;
  uint32_t crc = 0;
  srand((unsigned)time(NULL)); 
  // 512K ~ 1M
  generate_length(length, 1024 * 512);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(false, length, crc));
  // 1~2M
  crc = 0;
  generate_length(length, 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(false, length, crc));
  // 2~4M
  crc = 0;
  generate_length(length, 2 * 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(false, length, crc));
}

TEST_F(TfsLargeFileTest, read_small_file)
{
  //const int max_loop = 20;
  const int max_loop = 1;
  // 512K ~ 1M
  for (int i = 0; i < max_loop; ++i) 
    test_read(false, 1024 * 512);
  // 1~2M
  for (int i = 0; i < max_loop; ++i) 
    test_read(false, 1024 * 1024, ".jpg");
  // 2~4M
  for (int i = 0; i < max_loop; ++i) 
    test_read(false, 1024 * 1024, ".jpg");
}  

/*
TEST_F(TfsLargeFileTest, write_large_file)
{
  int64_t length = 0;
  uint32_t crc = 0;
  srand((unsigned)time(NULL)); 
  // 512K ~ 1M
  generate_length(length, 1024 * 512);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 1~2M
  crc = 0;
  generate_length(length, 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 2~4M
  crc = 0;
  generate_length(length, 2 * 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 5~10M
  crc = 0;
  generate_length(length, 5 * 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 40~80M
  crc = 0;
  generate_length(length, 40 * 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 1024M ~ 2048M 
  crc = 0;
  generate_length(length, 1024 * 1024 * 1024);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
  // 3G ~ 6G
  crc = 0;
  int64_t base = 3 * 1024 * 1024 * 1024L;
  generate_length(length, base);  
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc));
}
*/

TEST_F(TfsLargeFileTest, read_large_file)
{
  //const int max_loop = 20;
  const int max_loop = 3;
  // 512K ~ 1M
  //for (int i = 0; i < max_loop; ++i) 
  //  test_read(true, 1024 * 512);
  //// 1~2M
  //for (int i = 0; i < max_loop; ++i) 
  //  test_read(true, 1024 * 1024, ".jpg");
  //// 2~4M
  //for (int i = 0; i < max_loop; ++i) 
  //  test_read(true, 2 * 1024 * 1024);
  //// 5~10M
  //for (int i = 0; i < max_loop; ++i) 
  //  test_read(true, 5 * 1024 * 1024, ".gif");
  //// 40~80M
  //for (int i = 0; i < max_loop; ++i) 
  //  test_read(true, 40 * 1024 * 1024);
  // 1G ~ 2G
  for (int i = 0; i < max_loop; ++i) 
    test_read(true, 1024 * 1024 * 1024, ".bmp");
  // 3G ~ 6G
  for (int i = 0; i < max_loop; ++i) 
    test_read(true, 3 * 1024 * 1024 * 1024L);
}

TEST_F(TfsLargeFileTest, stat_exist_large_file)
{
  int64_t length = 60720000;
  uint32_t crc = 0;
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc, tfs_name, NULL, TFS_FILE_LEN));

  FileStat finfo;
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
}

TEST_F(TfsLargeFileTest, unlink_large_file)
{
  int64_t length = 6072000;
  uint32_t crc = 0;
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc, tfs_name, NULL, TFS_FILE_LEN));

  FileStat finfo;
  memset(&finfo, 0, sizeof(FileStat));
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
  ASSERT_EQ(0, finfo.flag_);
  ASSERT_EQ(length, finfo.size_);
  ASSERT_EQ(length, finfo.usize_);

  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::unlink_file(tfs_name));
  usleep(100 * 1000);

  cout << "unlink success" << endl;
  memset(&finfo, 0, sizeof(FileStat));
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
  ASSERT_EQ(1, finfo.flag_);
  const int32_t INVALID_FILE_SIZE = -1;
  ASSERT_EQ(INVALID_FILE_SIZE, finfo.size_);
  ASSERT_EQ(INVALID_FILE_SIZE, finfo.usize_);

  //write new file
  length = 18000000;
  memset(tfs_name, 0, TFS_FILE_LEN);
  crc = 0;
  ASSERT_EQ(length, TfsLargeFileTest::write_new_file(true, length, crc, tfs_name, NULL, TFS_FILE_LEN));

  memset(&finfo, 0, sizeof(FileStat));
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
  ASSERT_EQ(0, finfo.flag_);
  ASSERT_EQ(length, finfo.size_);
  ASSERT_EQ(length, finfo.usize_);

  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::unlink_file(tfs_name, NULL, CONCEAL));
  usleep(100 * 1000);
  cout << "conceal success" << endl;

  memset(&finfo, 0, sizeof(FileStat));
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
  ASSERT_EQ(4, finfo.flag_);
  ASSERT_EQ(INVALID_FILE_SIZE, finfo.size_);
  ASSERT_EQ(INVALID_FILE_SIZE, finfo.usize_);

  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::unlink_file(tfs_name, NULL, REVEAL));
  usleep(100 * 1000);
  cout << "reveal success" << endl;

  memset(&finfo, 0, sizeof(FileStat));
  ASSERT_EQ(TFS_SUCCESS, TfsLargeFileTest::stat_exist_file(true, tfs_name, finfo));
  ASSERT_EQ(0, finfo.flag_);
  ASSERT_EQ(length, finfo.size_);
  ASSERT_EQ(length, finfo.usize_);
}

void usage(char* name)
{
  printf("Usage: %s -s nsip:port\n", name);
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char nsip[256];
  int i = 0;

  memset(nsip, 0, 256);
  while ((i = getopt(argc, argv, "s:i")) != EOF)
  {
    switch (i)
    {
      case 's':
        strcpy(nsip, optarg);
        break;
      default:
        usage(argv[0]);
        return TFS_ERROR;
    }
  }
  if ('\0' == nsip[0])
  {
    usage(argv[0]);
    return TFS_ERROR;
  }

  TfsLargeFileTest::tfs_client_ = TfsClient::Instance();
	int ret = TfsLargeFileTest::tfs_client_->initialize(nsip);
	if (ret != TFS_SUCCESS)
	{
    cout << "tfs client init fail" << endl;
		return TFS_ERROR;
	}

  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (ret == TFS_ERROR)
  {
    printf("input argument error...\n");
    return 1;
  }
  return RUN_ALL_TESTS();
}
