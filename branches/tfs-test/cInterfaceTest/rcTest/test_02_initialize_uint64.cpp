#include"tfs_init_for_test.h"
#include "common/func.h"
TEST_F(TfsInitTest,01_initialize_uint64_wrong_app_ip)
{
   int Ret;
   
   const uint64_t rc_ip=Func::get_host_ip("10.232.36.206:6261");
   const char* app_key="foobarRcAA";
   const uint64_t app_ip=-1;
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(rc_ip, app_key, app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);

}

TEST_F(TfsInitTest,02_initialize_uint64_wrong_rc_ip)
{
   int Ret;
   const uint64_t rc_ip=-1;
   const char* app_key="foobarRcAA";
   const uint64_t app_ip=Func::str_to_addr("10.13.116.135", 0);
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(rc_ip, app_key, app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);

}

TEST_F(TfsInitTest,03_initialize_uint64_wrong_app_ip_2)
{
   int Ret;
   
   const uint64_t rc_ip=Func::get_host_ip("10.232.36.206:6261");
   const char* app_key="foobarRcAA";
   const uint64_t app_ip=464656;
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(rc_ip, app_key, app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);

}

TEST_F(TfsInitTest,04_initialize_uint64_wrong_rc_ip_2)
{
   int Ret;
   const uint64_t rc_ip=15454;
   const char* app_key="foobarRcAA";
   const uint64_t app_ip=Func::str_to_addr("10.13.116.135", 0);
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(rc_ip, app_key, app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);

}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
