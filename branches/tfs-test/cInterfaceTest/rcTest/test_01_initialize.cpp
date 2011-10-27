#include"tfs_init_for_test.h"

TEST_F(TfsInitTest,01_initialize)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInitTest,02_initialize_wrong_rc_ip)
{
   int Ret;
   const char* str_rc_ip="10.232.36.233:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,03_initialize_empty_rc_ip)
{
   int Ret;
   const char* str_rc_ip="";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(Ret,0);
}

//TEST_F(TfsInitTest,04_initialize_null_rc_ip)
//{
//   int Ret;
//   const char* str_rc_ip=NULL;
//   const char* app_key="foobarRcAA";
//   const char* str_app_ip="10.13.116.135";
//   const int32_t cache_times = 0;
//   const int32_t cache_items = 0;
//   const char* dev_name = NULL;
//   
//   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
//   EXPECT_GT(0,Ret);
//}

TEST_F(TfsInitTest,05_initialize_wrong_2_rc_ip)
{
   int Ret;
   const char* str_rc_ip="10.23s.yui.89:7415";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,06_initialize_wrong_app_key)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobar";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,07_initialize_empty_app_key)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}
//
//TEST_F(TfsInitTest,08_initialize_null_app_key)
//{
//   int Ret;
//   const char* str_rc_ip="10.232.36.206:6261";
//   const char* app_key=NULL;
//   const char* str_app_ip="10.13.116.135";
//   const int32_t cache_times = 0;
//   const int32_t cache_items = 0;
//   const char* dev_name = NULL;
//   
//   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
//   EXPECT_GT(0,Ret);
//}

TEST_F(TfsInitTest,09_initialize_wrong_app_ip)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.1sads.11ds.13ss";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,10_initialize_empty_app_ip)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,11_initialize_null_app_ip)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip=NULL;
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,12_initialize_with_dev_name)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = "bond0";
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInitTest,13_initialize_wrong_dev_name)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = "eth0";
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,14_initialize_empty_dev_name)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = 0;
   const char* dev_name = "";
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,15_initialize_wrong_cache_times)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = -1;
   const int32_t cache_items = 0;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,16_initialize_wrong_cache_items)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 0;
   const int32_t cache_items = -1;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInitTest,17_initialize_large_cache_para)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   const int32_t cache_times = 2147483647;
   const int32_t cache_items = 2147483647;
   const char* dev_name = NULL;
   
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInitTest,18_initialize_many_times)
{
   int Ret;
   const char* str_rc_ip="10.232.36.206:6261";
   const char* app_key="foobarRcAA";
   const char* str_app_ip="10.13.116.135";
   int32_t cache_times = 2147483647;
   int32_t cache_items = 2147483647;
   const char* dev_name = NULL;

   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_EQ(0,Ret);

   cache_times=0;
   cache_items=0;
   Ret=tfsclient->initialize(str_rc_ip, app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(Ret,0);
   
   Ret=tfsclient->initialize("sahkkljksahk", app_key, str_app_ip,cache_times,cache_items,dev_name);
   EXPECT_GT(Ret,0);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
