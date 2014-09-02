#include <gtest/gtest.h>
#include <iostream>
#include "common/http_agent.h"


namespace tfs
{
  namespace common
  {
    class TestHttpAgent: public virtual ::testing::Test
    {
      public:
        static void SetUpTestCase()
        {
        }
        static void TearDownTestCase()
        {
        }

        TestHttpAgent(){}
        ~TestHttpAgent(){}
    };

    TEST_F(TestHttpAgent, connect_kvmeta)
    {
      int32_t ret = 0;
      std::string ip_port = "10.232.36.203:44444";

      HttpAgent *ht = new HttpAgent();
      ret = ht->initialize(ip_port);

      std::string request_addr = "/v2/tfscom/metadata/5/12345/file/bbbbb";
      std::string header_options = "x-tfs-meta-abc:aaaaaaaaaaaa";
      ht->push_to_send_queue(request_addr, header_options);
    }

    TEST_F(TestHttpAgent, connect_monitor)
    {
      int32_t ret = 0, i;
      std::string ip_port = "10.232.35.40:32000";

      HttpAgent *ht = new HttpAgent();
      ret = ht->initialize(ip_port);

      if (TFS_SUCCESS == ret) {
        std::string request_addr = "/v2/";
        std::string header_options = "x-tfs-meta-abc:aaaaaaaaaaaa";
        for (i = 0;i < 20; i++) {
          ht->push_to_send_queue(request_addr, header_options);
          printf("i = %d\n", i);
          //usleep(1000000);
        }
      } else {
        printf("connect server error.\n");
      }
      for(;;) {
        usleep(1000);
      }
    }
  }
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
