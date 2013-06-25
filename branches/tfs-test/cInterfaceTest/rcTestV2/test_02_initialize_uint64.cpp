#include"tfs_init_for_test.h"
#include "func.h"

TEST_F(TfsInitTest,01_initialize_uint64_wrong_rc_ip)
{
   int Ret;
   const uint64_t rc_ip=-1;
   const char* app_key="appkey";
   const uint64_t app_ip=Func::str_to_addr("10.13.116.135", 0);
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(rc_ip, app_key, app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);

}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
