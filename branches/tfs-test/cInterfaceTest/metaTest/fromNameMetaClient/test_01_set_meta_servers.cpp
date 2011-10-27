#include"tfs_init_for_test.h"


TEST_F(TfsInitTest,01_set_meta_servers_right)
{
   int ret;
   const char* meta_server_str="10.232.36.206:8765";
   ret=tfsclient->initialize(meta_server_str);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInitTest,02_set_meta_servers_null)
{
   int ret;
   const char* meta_server_str=NULL;
   ret=tfsclient->initialize(meta_server_str);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInitTest,03_set_meta_servers_empty)
{
   int ret;
   const char meta_server_str[1]="";
   ret=tfsclient->initialize(meta_server_str);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInitTest,04_set_meta_servers_wrong_1)
{
   int ret;
   const char* meta_server_str="10.2s2.3w.2d6:5s51";
   ret=tfsclient->initialize(meta_server_str);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInitTest,05_set_meta_servers_wrong_2)
{
   int ret;
   const char* meta_server_str="10.232.36.206:8653";
   ret=tfsclient->initialize(meta_server_str);
   EXPECT_GT(0,ret);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
