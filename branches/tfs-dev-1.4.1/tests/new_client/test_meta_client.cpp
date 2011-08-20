#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <utime.h>
#include <gtest/gtest.h>
#include "fsname.h"
#include "common/func.h"
#include "common/meta_server_define.h"
#include "tfs_meta_client_api.h"
#include "tfs_meta_client_api_impl.h"

using namespace tfs::common;
using namespace tfs::client;

class MetaClientTest : public testing::Test
{
public:
  MetaClientTest()
  {
  }

  ~MetaClientTest()
  {
  }

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
public:
  static NameMetaClient* meta_client_;
  static uint64_t server_id_;
};
NameMetaClient* MetaClientTest::meta_client_;
uint64_t MetaClientTest::server_id_;
const int64_t app_id = 7;
const int64_t user_id = 7;

char dir_path[MAX_FILE_PATH_LEN];
char new_dir_path[MAX_FILE_PATH_LEN];
char wrong_dir_path[MAX_FILE_PATH_LEN];
char file_path[MAX_FILE_PATH_LEN];
char new_file_path[MAX_FILE_PATH_LEN];
char wrong_file_path[MAX_FILE_PATH_LEN];

TEST_F(MetaClientTest, test_create_dir)
{
  strcpy(dir_path, "/cydir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->create_dir(dir_path));

  strcpy(dir_path, "/cydir");
  EXPECT_EQ(EXIT_TARGET_EXIST_ERROR, meta_client_->create_dir(dir_path));

  strcpy(dir_path, "/cydir/cysubdir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->create_dir(dir_path));

  strcpy(wrong_dir_path, "cydir");
  EXPECT_EQ(TFS_ERROR, meta_client_->create_dir(wrong_dir_path));

  strcpy(new_dir_path, "/cynewdir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->create_dir(new_dir_path));
}

TEST_F(MetaClientTest, test_create_file)
{
  strcpy(file_path, "cyfile");
  EXPECT_EQ(TFS_ERROR, meta_client_->create_file(file_path));

  strcpy(file_path, "/cyfile");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->create_file(file_path));
}

TEST_F(MetaClientTest, test_rm_dir)
{
  strcpy(dir_path, "/cydir");
  EXPECT_EQ(EXIT_DELETE_DIR_WITH_FILE_ERROR, meta_client_->rm_dir(dir_path));

  strcpy(dir_path, "/cydir/cysubdir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->rm_dir(dir_path));

  strcpy(dir_path, "/cydir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->rm_dir(dir_path));

  strcpy(new_dir_path, "/cynewdir");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->rm_dir(new_dir_path));
}

TEST_F(MetaClientTest, test_rm_file)
{
  strcpy(file_path, "/cyfile");
  EXPECT_EQ(TFS_SUCCESS, meta_client_->rm_file(file_path));
}

void usage(char* name)
{
  printf("Usage: %s -s nsip:port\n", name);
  exit(TFS_ERROR);
}

int parse_args(int argc, char *argv[])
{
  char nsip[256];
  char meta_server_ip[256];
  int i = 0;

  memset(nsip, 0, 256);
  memset(meta_server_ip, 0, 256);
  while ((i = getopt(argc, argv, "n:m:i")) != EOF)
  {
    switch (i)
    {
      case 'n':
        strcpy(nsip, optarg);
        break;
      case 'm':
        strcpy(meta_server_ip, optarg);
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

  MetaClientTest::server_id_ = Func::get_host_ip(nsip);
  MetaClientTest::meta_client_ = new NameMetaClient();
	MetaClientTest::meta_client_->set_meta_servers(meta_server_ip);
	MetaClientTest::meta_client_->set_server(nsip);
  MetaClientTest::meta_client_->set_app_id(app_id);
  MetaClientTest::meta_client_->set_user_id(user_id);

  return TFS_SUCCESS;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  int ret = parse_args(argc, argv);
  if (TFS_ERROR == ret)
  {
    printf("input argument error...\n");
    return TFS_ERROR;
  }
  return RUN_ALL_TESTS();
}
