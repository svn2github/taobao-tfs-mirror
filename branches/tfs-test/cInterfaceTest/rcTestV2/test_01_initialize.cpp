#include"tfs_init_for_test.h"

TEST_F(TfsInitTest,01_initialize)
{
   int Ret;
   const char* str_rc_ip="10.232.36.203:6100";
   const char* app_key="tfscom";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInitTest,02_initialize_wrong_rc_ip)
{
   int Ret;
   const char* str_rc_ip="10.232.36.233:6261";
   const char* app_key="tfscom";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,03_initialize_empty_rc_ip)
{
   int Ret;
   const char* str_rc_ip="";
   const char* app_key="tfscom";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(Ret,0);
}

TEST_F(TfsInitTest,04_initialize_null_rc_ip)
{
   int Ret;
   const char* str_rc_ip=NULL;
   const char* app_key="tfscom";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(1,Ret);
}

TEST_F(TfsInitTest,05_initialize_wrong_2_rc_ip)
{
   int Ret;
   const char* str_rc_ip="10.23s.yui.89:7415";
   const char* app_key="tfscom";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,06_initialize_wrong_app_key)
{
   int Ret;
   const char* str_rc_ip="10.232.36.203:6100";
   const char* app_key="foobar";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,07_initialize_empty_app_key)
{
   int Ret;
   const char* str_rc_ip="10.232.36.203:6100";
   const char* app_key="";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,08_initialize_null_app_key)
{
   int Ret;
   const char* str_rc_ip="10.232.36.203:6100";
   const char* app_key=NULL;
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(1,Ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
































