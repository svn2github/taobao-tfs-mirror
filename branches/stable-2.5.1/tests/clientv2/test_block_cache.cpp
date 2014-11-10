#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string>
#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"
#include "common/internal.h"
#include "clientv2/tfs_session.h"


#include "gtest/gtest.h"
using namespace std;
using namespace tfs::common;
using namespace tfs::clientv2;

class TestBlockCache : public testing::Test
{
  public:
    TestBlockCache()
    {
    }

    ~TestBlockCache()
    {
    }

    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

const std::string ns_ip = "10.232.36.201:8100";
char* remote_cache_master_addr = "10.232.12.141:5198";
char* remote_cache_slave_addr = "10.232.12.142:5198";
char* remote_cache_group_name = "group_1";
int area = 2;

TEST_F(TestBlockCache, lru)
{
  ClientConfig::use_cache_ |= tfs::clientv2::USE_CACHE_FLAG_LOCAL;
  VUINT64 ds;
  FamilyInfoExt info;

  // test cache size control
  TfsSession session(ns_ip, 1000, 2);
  session.initialize();

  // insert block 100
  session.insert_local_block_cache(100, ds, info);
  ASSERT_TRUE(session.query_local_block_cache(100, ds, info));

  // insert block 200
  session.insert_local_block_cache(200, ds, info);
  ASSERT_TRUE(session.query_local_block_cache(200, ds, info));

  // insert block 300
  session.insert_local_block_cache(300, ds, info);
  ASSERT_TRUE(session.query_local_block_cache(300, ds, info));

  // 100 should be removed already, only 300, 200
  ASSERT_FALSE(session.query_local_block_cache(100, ds, info));

  // query 200
  ASSERT_TRUE(session.query_local_block_cache(200, ds, info));

  // insert 400
  session.insert_local_block_cache(400, ds, info);
  ASSERT_TRUE(session.query_local_block_cache(400, ds, info));

  // 300 should be removed already because of lru
  ASSERT_FALSE(session.query_local_block_cache(300, ds, info));

  // remove 200
  session.remove_local_block_cache(200);
  ASSERT_FALSE(session.query_local_block_cache(200, ds, info));

  session.destroy();
}

TEST_F(TestBlockCache, expire)
{
  ClientConfig::use_cache_ |= tfs::clientv2::USE_CACHE_FLAG_LOCAL;
  VUINT64 ds;
  FamilyInfoExt info;

  // test cache size control
  TfsSession session(ns_ip, 5, 2);
  session.initialize();

  // insert block 100
  session.insert_local_block_cache(100, ds, info);
  ASSERT_TRUE(session.query_local_block_cache(100, ds, info));

  sleep(10);  // wait 10 seconds, cache item already expired

  ASSERT_FALSE(session.query_local_block_cache(100, ds, info));

  session.destroy();
}

TEST_F(TestBlockCache, remote)
{
  ClientConfig::use_cache_ |= tfs::clientv2::USE_CACHE_FLAG_REMOTE;
  ClientConfig::remote_cache_master_addr_ = remote_cache_master_addr;
  ClientConfig::remote_cache_slave_addr_ = remote_cache_slave_addr;
  ClientConfig::remote_cache_group_name_ = remote_cache_group_name;
  ClientConfig::remote_cache_area_ = area;

  VUINT64 ds;
  FamilyInfoExt info;

  // test cache size control
  TfsSession session(ns_ip, 5, 2);
  session.initialize();

  // insert block 100
  session.insert_remote_block_cache(100, ds, info);
  ASSERT_TRUE(session.query_remote_block_cache(100, ds, info));

  // remove block 100
  session.remove_remote_block_cache(100);
  ASSERT_FALSE(session.query_remote_block_cache(100, ds, info));

  session.destroy();
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
