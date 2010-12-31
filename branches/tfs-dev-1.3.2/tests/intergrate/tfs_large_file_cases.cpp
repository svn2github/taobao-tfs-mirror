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
}

void TfsLargeFileTest::TearDownTestCase()
{
  ::unlink("tmplocal");
}

int32_t TfsLargeFileTest::generate_data(char* buffer, const int32_t length)
{
  srand(time(NULL) + rand() + pthread_self());
  int32_t i = 0;
  for (i = 0; i < length; i++)
  {
    buffer[i] = rand() % 90 + 32;
  }
  return length;
}

int64_t TfsLargeFileTest::write_new_file(const int64_t length, uint32_t& crc, char* tfsname, const int32_t name_len)
{
  int64_t cost_time = 0, start_time = 0, end_time = 0;
  const char* filename = NULL;
  const char* suffix = NULL;
  const char* key = "test_large";
  int local_fd = open(key, O_CREAT|O_WRONLY|O_TRUNC, 0644);
  if (local_fd < 0)
  {
    return local_fd;
  }
  close(local_fd);
  start_time = CTimeUtil::getTime();
  int fd = tfs_client_->open(filename, suffix, T_WRITE | T_LARGE, key);
  end_time = CTimeUtil::getTime();
  cost_time += (end_time - start_time);
  if (fd <= 0)
  {
    return fd;
  }
  cout << "tfs open successful. cost time " << cost_time << endl;

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
    cout << "write offset " << length - remain_size << " size " << cur_size << " write crc " << crc << " cost time " << end_time - start_time << endl;
    remain_size -= cur_size;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  cout << "write data successful" << endl;

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

int TfsLargeFileTest::write_read_file(const int64_t length)
{
  char tfs_name[TFS_FILE_LEN];
  memset(tfs_name, 0, TFS_FILE_LEN);
  uint32_t write_crc = 0;
  if (write_new_file(length, write_crc, tfs_name, TFS_FILE_LEN) != length)
  {
    return TFS_ERROR;
  }

  int64_t cost_time = 0, start_time = 0, end_time = 0;
  const char* suffix = NULL;
  start_time = CTimeUtil::getTime();
  int fd = tfs_client_->open(tfs_name, suffix, T_READ | T_LARGE);
  end_time = CTimeUtil::getTime();
  if (fd <= 0)
  {
    return fd;
  }
  cost_time += (end_time - start_time);
  cout << "tfs open successful. cost time " << cost_time << endl;

  uint32_t read_crc = 0;
  int64_t ret = TFS_SUCCESS;
  char* buffer = new char[PER_SIZE];
  int64_t remain_size = length;
  int64_t cur_size = 0;
  while (remain_size > 0)
  {
    cur_size = (remain_size >= PER_SIZE) ? PER_SIZE : remain_size;
    //generate_data(buffer, cur_size);
    start_time = CTimeUtil::getTime();
    ret = tfs_client_->read(fd, buffer, cur_size);
    end_time = CTimeUtil::getTime();
    if (ret != cur_size)
    {
      cout << "tfs read fail. cur_size: " << cur_size << endl;
      delete []buffer;
      return ret;
    }
    read_crc = Func::crc(read_crc, buffer, cur_size);
    cout << "read offset " << length - remain_size << " size " << cur_size << " read crc " << read_crc << " cost time " << end_time - start_time << endl;
    remain_size -= cur_size;
    cost_time += (end_time - start_time);
  }
  delete []buffer;

  start_time = CTimeUtil::getTime();
  ret = tfs_client_->close(fd, tfs_name, TFS_FILE_LEN);
  end_time = CTimeUtil::getTime();
  if (TFS_SUCCESS != ret)
  {
    return ret;
  }
  cost_time += (end_time - start_time);
  cout << "write crc " << write_crc << " read crc " << read_crc << " cost time " << cost_time << endl;

  if (write_crc != read_crc)
  {
    ret = TFS_ERROR;
    return ret;
  }
  return TFS_SUCCESS;
}

/*
int TfsLargeFileTest::write_update_file(const char* buffer, const int32_t length, const std::string& tfs_file_name)
{
  int32_t ret = TFS_SUCCESS;
  ret = tfs_client_->tfs_open((char*) tfs_file_name.c_str(), "", WRITE_MODE);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  ret = write_data(buffer, length);
  if (ret != length)
  {
    return ret;
  }

  ret = tfs_client_->tfs_close();
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  return TFS_SUCCESS;
}

int TfsLargeFileTest::write_update_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  int ret = write_new_file(buffer, length);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }

  generate_data(buffer, length);
  return write_update_file(buffer, length, tfs_client_->get_file_name());
}

int TfsLargeFileTest::stat_exist_file(char* tfs_file_name, FileInfo& file_info)
{
  int32_t ret = tfs_client_->tfs_open((char*) tfs_file_name, "", READ_MODE);
  if (ret != TFS_SUCCESS)
  {
    return ret;
  }
  ret = tfs_client_->tfs_stat(&file_info, READ_MODE);
  tfs_client_->tfs_close();
  return ret;
}

int TfsLargeFileTest::read_exist_file(const std::string& tfs_file_name, char* buffer, int32_t& length)
{
  int ret = tfs_client_->tfs_open((char*) tfs_file_name.c_str(), "", READ_MODE);
  if (ret != TFS_SUCCESS)
  {
    printf("tfsOpen :%s failed", tfs_file_name.c_str());
    return ret;
  }

  FileInfo file_info;
  ret = tfs_client_->tfs_stat(&file_info);
  if (ret != TFS_SUCCESS)
  {
    tfs_client_->tfs_close();
    printf("tfsStat:%s failed", tfs_file_name.c_str());
    return ret;
  }

  length = file_info.size_;
  int32_t num_readed = 0;
  int32_t num_per_read = min(length, 16384);
  uint32_t crc = 0;

  do
  {
    ret = tfs_client_->tfs_read(buffer + num_readed, num_per_read);
    if (ret < 0)
    {
      break;
    }
    crc = Func::crc(crc, buffer + num_readed, ret);
    num_readed += ret;
    if (num_readed >= length)
    {
      break;
    }
  }
  while (1);

  if ((ret < 0) || (num_readed < length))
  {
    printf("tfsread failed (%d), readed(%d)\n", ret, num_readed);
    ret = TFS_ERROR;
    goto error;
  }

  if (crc != file_info.crc_)
  {
    printf("crc check failed (%d), info.crc(%d)\n", crc, file_info.crc_);
    ret = TFS_ERROR;
    goto error;
  }
  else
  {
    ret = TFS_SUCCESS;
  }

error:
  tfs_client_->tfs_close();
  return ret;
}
*/

/*
int TfsLargeFileTest::rename_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  if (write_new_file(buffer, length) != TFS_SUCCESS)
    return TFS_ERROR;

  char oldname[256], newname[256];
  strcpy(oldname, tfs_client_->get_file_name());
  FileInfo file_info;

  // remove
  if (tfs_client_->rename(oldname, "", "newprefix") != TFS_SUCCESS)
    return TFS_ERROR;

  strcpy(newname, tfs_client_->get_file_name());
  if (stat_exist_file(oldname, file_info) == TFS_SUCCESS)
    return TFS_ERROR;
  if (stat_exist_file(newname, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  return TFS_SUCCESS;
}

int TfsLargeFileTest::unlink_file(const int32_t length)
{
  char buffer[length];
  generate_data(buffer, length);
  if (write_new_file(buffer, length) != TFS_SUCCESS)
    return TFS_ERROR;

  char tfsfile[256];
  strcpy(tfsfile, tfs_client_->get_file_name());
  FileInfo file_info;

  // remove
  if (tfs_client_->unlink(tfsfile, "", DELETE) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 1)
  {
    printf("unlink %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }

  // undelete
  if (tfs_client_->unlink(tfsfile, "", UNDELETE) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 0)
  {
    printf("undelete %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }

  // hide
  if (tfs_client_->unlink(tfsfile, "", CONCEAL) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 4)
  {
    printf("hide %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }
  //
  // unhide
  if (tfs_client_->unlink(tfsfile, "", REVEAL) != TFS_SUCCESS)
    return TFS_ERROR;
  usleep(100 * 1000);
  if (stat_exist_file(tfsfile, file_info) != TFS_SUCCESS)
    return TFS_ERROR;
  if (file_info.flag_ != 0)
  {
    printf("unhide %s failure , flag:%d\n", tfsfile, file_info.flag_);
    return TFS_ERROR;
  }
  return TFS_SUCCESS;
}
*/

TEST_F(TfsLargeFileTest, write_new_file)
{
  int64_t length = 6072000;
  uint32_t crc = 0;
  EXPECT_EQ(length, TfsLargeFileTest::write_new_file(length, crc));
}

TEST_F(TfsLargeFileTest, write_read_file)
{
  int64_t length = 2000;
  //EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::write_read_file(length));
  //length = 6074000;
  //EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::write_read_file(length));
  length = 60740000;
  //EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::write_read_file(length));
  //length = 5607400000;
  EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::write_read_file(length));
}

//TEST_F(TfsLargeFileTest, write_update_file)
//{
//  for (int i = 0; i < MAX_LOOP; ++i)
//  {
//    EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::write_update_file(200)) << TfsLargeFileTest::get_error_message();
//  }
//}
//
//TEST_F(TfsLargeFileTest, rename_file)
//{
//  for (int i = 0; i < MAX_LOOP; ++i)
//  {
//    EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::rename_file(200)) << TfsLargeFileTest::get_error_message();
//  }
//}
//
//TEST_F(TfsLargeFileTest, unlink_file)
//{
//  for (int i = 0; i < MAX_LOOP; ++i)
//  {
//    EXPECT_EQ(TFS_SUCCESS, TfsLargeFileTest::unlink_file(200)) << TfsLargeFileTest::get_error_message();
//  }
//}

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
