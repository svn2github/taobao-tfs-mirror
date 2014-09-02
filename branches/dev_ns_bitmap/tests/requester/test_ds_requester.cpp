#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string>
#include "common/define.h"
#include "common/func.h"
#include "common/error_msg.h"
#include "common/client_manager.h"
#include "requester/ns_requester.h"
#include "requester/ds_requester.h"
#include "message/message_factory.h"

#include "gtest/gtest.h"
using namespace std;
using namespace tfs::common;
using namespace tfs::message;
using namespace tfs::requester;

class TestDsRequester : public testing::Test
{
  public:
    TestDsRequester()
    {
      ns_id_ = Func::get_host_ip("10.232.36.201:8100");
      ds_id_ = Func::get_host_ip("10.232.36.203:3200");
      factory_ = new MessageFactory();
      streamer_ = new BasePacketStreamer(factory_);
      NewClientManager::get_instance().initialize(factory_, streamer_);
    }

    ~TestDsRequester()
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
    uint64_t ds_id_;
    BasePacketFactory* factory_;
    BasePacketStreamer* streamer_;
};

TEST_F(TestDsRequester, read_block_index)
{
  int ret = TFS_SUCCESS;
  uint64_t block_id = 101;
  IndexDataV2 index_data;
  ret = DsRequester::read_block_index(Func::get_host_ip("10.232.36.203:3200"), block_id, block_id, index_data);
  EXPECT_EQ(TFS_SUCCESS, ret);
  EXPECT_EQ(block_id, index_data.header_.info_.block_id_);
  TBSYS_LOG(DEBUG, "block file items %zd", index_data.finfos_.size());
}


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
