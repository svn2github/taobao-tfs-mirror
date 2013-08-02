#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string>
#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"
#include "clientv2/tfs_client_impl_v2.h"

#include "gtest/gtest.h"
using namespace std;
using namespace tfs::common;
using namespace tfs::clientv2;

#define tfs_client TfsClientImplV2::Instance()

void create_file(const char* name, char* buf, const int file_size)
{
  assert(NULL != name);
  assert(0 != file_size);

  bool alloc = false;
  if (NULL == buf)
  {
    alloc = true;
    buf = new (nothrow) char[file_size];
    assert(NULL != buf);
  }

  FILE* fp = fopen(name, "w+");
  assert(NULL != fp);

  int write_size = fwrite(buf, sizeof(char), file_size, fp);
  assert(write_size == file_size);

  fclose(fp);
  if (alloc)
  {
    delete buf;
  }
}

void remove_file(const char* name)
{
  assert(NULL != name);
  unlink(name);
}

bool compare_file(const char* left, const char* right)
{
  bool same = true;
  assert(NULL != left);
  assert(NULL != right);

  FILE* lfp = fopen(left, "r");
  assert(NULL != lfp);

  FILE* rfp = fopen(right, "r");
  assert(NULL != rfp);

  while (1)
  {
    const int32_t unit = 1024 * 1024;
    char lbuf[unit];
    char rbuf[unit];

    int32_t llen = fread(lbuf, sizeof(char), unit, lfp);
    int32_t rlen = fread(rbuf, sizeof(char), unit, rfp);

    if (llen != rlen)
    {
      same = false;
      break;
    }

    for (int index = 0; index < llen; index++)
    {
      if (lbuf[index] != rbuf[index])
      {
        same = false;
        break;
      }
    }

    if (llen < unit || rlen < unit)
    {
      break;
    }
  }

  fclose(lfp);
  fclose(rfp);

  return same;
}

class TestSaveFetch : public testing::Test
{
  public:
    TestSaveFetch()
    {
    }

    ~TestSaveFetch()
    {
    }

    virtual void SetUp()
    {
      create_file("1k", NULL, 1024);
      create_file("20k", NULL, 20 * 1024);
      create_file("100k", NULL, 100 * 1024);
      create_file("5m", NULL, 5 * 1024 * 1024);

      char* ns_ip = "10.232.36.201:8100";
      int ret = tfs_client->initialize(ns_ip);
      assert(TFS_SUCCESS == ret);
    }

    virtual void TearDown()
    {
      remove_file("1k");
      remove_file("20k");
      remove_file("100k");
      remove_file("5m");

      tfs_client->destroy();
    }
};

TEST_F(TestSaveFetch, stat)
{
  const char* test_file_name = "1k";
  const char* test_suffix = ".jpg";
  const int32_t test_file_size = 1024;

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;
  TfsFileStat file_stat;

  // save file
  len = tfs_client->save_file(tfs_name, tfs_name_len,
    test_file_name, T_CREATE, test_suffix);
  ASSERT_EQ(len, test_file_size);

  // stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 0);
}

TEST_F(TestSaveFetch, fetch)
{
  const char* test_file_name = "5m";
  const char* test_suffix = ".jpg";
  const char* test_local_name = "5m_local";
  const int32_t test_file_size = 5 * 1024 * 1024;

  const char* update_file_name = "100k";
  const char* update_local_name ="100k_local";
  const int32_t update_file_size = 100 * 1024;

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;

  // save file
  len = tfs_client->save_file(tfs_name, tfs_name_len,
    test_file_name, T_CREATE, test_suffix);
  ASSERT_EQ(len, test_file_size);

  // fetch file
  ret = tfs_client->fetch_file(test_local_name, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  bool same = compare_file(test_file_name, test_local_name);
  EXPECT_TRUE(same);

  // update file
  len = tfs_client->save_file_update(update_file_name, T_CREATE,
      tfs_name, test_suffix);
  EXPECT_EQ(len, update_file_size);

  // fetch updated file
  ret = tfs_client->fetch_file(update_local_name, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  same = compare_file(update_file_name, update_local_name);
  EXPECT_TRUE(same);

  // remove local temp file
  remove_file(test_local_name);
  remove_file(update_local_name);
}

TEST_F(TestSaveFetch, save_buf)
{
  const char* test_suffix = ".jpg";
  const char* test_local_name = "20k_local";
  const char* test_local_name_save_buf = "20k_buf";
  const char* update_local_name = "100k_local";
  const char* update_local_name_save_buf = "100k_buf";
  const int32_t test_file_size = 20 * 1024;
  const int32_t update_file_size = 100 * 1024;

  // data buffer to save as a tfs file
  char buf[test_file_size];
  char buf_update[update_file_size];
  create_file(test_local_name_save_buf, buf, test_file_size);
  create_file(update_local_name_save_buf, buf_update, update_file_size);

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;

  // save buf
  len = tfs_client->save_buf(tfs_name, tfs_name_len,
    buf, test_file_size, T_CREATE, test_suffix);
  ASSERT_EQ(len, test_file_size);

  // fetch file
  ret = tfs_client->fetch_file(test_local_name, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  bool same = compare_file(test_local_name_save_buf, test_local_name);
  EXPECT_TRUE(same);

  // save buf update
  len = tfs_client->save_buf_update(buf_update, update_file_size, T_WRITE,
      tfs_name, test_suffix);
  EXPECT_EQ(len, update_file_size);

  // fetch updated file
  ret = tfs_client->fetch_file(update_local_name, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  same = compare_file(update_local_name_save_buf, update_local_name);
  EXPECT_TRUE(same);

  // remove local temp file
  remove_file(test_local_name);
  remove_file(test_local_name_save_buf);
  remove_file(update_local_name);
  remove_file(update_local_name_save_buf);
}

TEST_F(TestSaveFetch, unlink)
{
  const char* test_file_name = "100k";
  const char* test_local_name = "100k_local";
  const char* test_suffix = ".jpg";
  const int32_t test_file_size = 100 * 1024;

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;
  TfsFileStat file_stat;

  // save file
  len = tfs_client->save_file(tfs_name, tfs_name_len,
    test_file_name, T_CREATE, test_suffix);
  ASSERT_EQ(len, test_file_size);

  // unlink file
  ret = tfs_client->unlink(len, tfs_name, NULL, DELETE);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  // cannot access deleted file
  ret = tfs_client->stat_file(&file_stat, tfs_name);
  EXPECT_EQ(ret, EXIT_FILE_INFO_ERROR);

  // cannot read file data
  ret = tfs_client->fetch_file(test_local_name, tfs_name, NULL);
  EXPECT_EQ(ret, EXIT_FILE_INFO_ERROR);

  // test force stat
  ret = tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 1);

  // test force fetch
  ret = tfs_client->fetch_file(test_local_name, tfs_name,
      NULL, READ_DATA_OPTION_FLAG_FORCE);
  EXPECT_EQ(ret, TFS_SUCCESS);
  bool same = compare_file(test_file_name, test_local_name);
  EXPECT_TRUE(same);

  // test revert unlink
  ret = tfs_client->unlink(len, tfs_name, NULL, UNDELETE);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  // stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 0);

  // hide file
  ret = tfs_client->unlink(len, tfs_name, NULL, CONCEAL);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  // stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name);
  EXPECT_EQ(ret, EXIT_FILE_INFO_ERROR);

  // force stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 4);

  // unhide file
  ret = tfs_client->unlink(len, tfs_name, NULL, REVEAL);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  // stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 0);

  // remove temp file
  remove_file(test_local_name);
}

TEST_F(TestSaveFetch, internal_close)
{
  const char* test_suffix = ".jpg";
  const int32_t test_file_size = 100 * 1024;

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;
  TfsFileStat file_stat;
  char buf[test_file_size];

  // save file
  int fd = tfs_client->open(NULL, test_suffix, NULL, T_CREATE | T_WRITE);
  EXPECT_TRUE(fd >= 0);

  len = tfs_client->write(fd, buf, test_file_size);
  EXPECT_EQ(len, test_file_size);

  ret = tfs_client->close(fd, tfs_name, tfs_name_len, 1); // set file to deleted status
  EXPECT_EQ(ret, TFS_SUCCESS);

  // stat file
  ret = tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 1);  // it should be deleted
}

TEST_F(TestSaveFetch, internal_unlink)
{
  const char* test_suffix = ".jpg";
  const int32_t test_file_size = 100 * 1024;

  const int32_t tfs_name_len = 32;
  char tfs_name[tfs_name_len];
  int ret = TFS_SUCCESS;
  int64_t len = 0;
  TfsFileStat file_stat;
  char buf[test_file_size];

  // save file
  int fd = tfs_client->open(NULL, test_suffix, NULL, T_CREATE | T_WRITE);
  EXPECT_TRUE(fd >= 0);

  len = tfs_client->write(fd, buf, test_file_size);
  EXPECT_EQ(len, test_file_size);

  ret = tfs_client->close(fd, tfs_name, tfs_name_len);
  EXPECT_EQ(ret, TFS_SUCCESS);

  // unlink file & set status, new version
  int status = 5; // deleted and hiden
  int action = 0;
  SET_OVERRIDE_FLAG(action, status);

  ret = tfs_client->unlink(len, tfs_name, NULL, static_cast<TfsUnlinkType>(action));
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  ret = tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 5);  // it should be deleted

  // old version
  action = 1 << 4;  // target status
  ret = tfs_client->unlink(len, tfs_name, NULL, static_cast<TfsUnlinkType>(action));
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(len, test_file_size);

  ret = tfs_client->stat_file(&file_stat, tfs_name, NULL, FORCE_STAT);
  EXPECT_EQ(ret, TFS_SUCCESS);
  EXPECT_EQ(file_stat.size_, test_file_size);
  EXPECT_EQ(file_stat.flag_, 1);  // it should be deleted
}



int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
