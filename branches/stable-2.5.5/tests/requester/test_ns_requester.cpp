#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string>
#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"
#include "common/client_manager.h"
#include "requester/ns_requester.h"
#include "message/message_factory.h"

#include "gtest/gtest.h"
using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::requester;

class TestNsRequester : public testing::Test
{
  public:
    TestNsRequester()
    {
      ns_id_ = Func::get_host_ip("10.232.36.201:8100");
      factory_ = new MessageFactory();
      streamer_ = new BasePacketStreamer(factory_);
      NewClientManager::get_instance().initialize(factory_, streamer_);
    }

    ~TestNsRequester()
    {
      NewClientManager::get_instance().destroy();
      tbsys::gDelete(factory_);
      tbsys::gDelete(streamer_);
    }

    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }

  public:
    uint64_t ns_id_;
    BasePacketFactory* factory_;
    BasePacketStreamer* streamer_;
};

TEST_F(TestNsRequester, get_cluster_id)
{
  int ret = TFS_SUCCESS;
  int32_t cluster_id = -1;
  ret = NsRequester::get_cluster_id(ns_id_, cluster_id);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(2, cluster_id);
}


TEST_F(TestNsRequester, get_group_count)
{
  int ret = TFS_SUCCESS;
  int32_t group_count = -1;
  ret = NsRequester::get_group_count(ns_id_, group_count);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(1, group_count);
}

TEST_F(TestNsRequester, get_group_seq)
{
  int ret = TFS_SUCCESS;
  int32_t group_seq = -1;
  ret = NsRequester::get_group_seq(ns_id_, group_seq);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(0, group_seq);
}

TEST_F(TestNsRequester, get_ds_list)
{
  int ret = TFS_SUCCESS;
  VUINT64 ds_list;
  ret = NsRequester::get_ds_list(ns_id_, ds_list);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(4U, ds_list.size());
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
