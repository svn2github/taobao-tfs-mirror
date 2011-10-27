#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_saveFile_right)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_saveFile_null_localFile)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,NULL,"/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_03_saveFile_empty_localFile)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char name[1]="";
   Ret=tfsclient->save_file(ns_addr,appId,uid,name,"/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_04_saveFile_wrong_localFile)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,"jfsahkjhsdfkjlafhd","/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_05_saveFile_null_fileName)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,NULL);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_06_saveFile_empty_fileName)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char name[1]="";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,name);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_07_saveFile_wrong_fileName_1)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"///test///");
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_08_saveFile_wrong_fileName_2)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_09_saveFile_wrong_fileName_3)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_10_saveFile_leap_fileName)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_11_saveFile_with_Dir)
{
   int64_t Ret;
   int ret;
   
   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_12_saveFile_large)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a1G,"/test");
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_saveFile_many_times)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_saveFile_NULL_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr=NULL;
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_15_saveFile_empty_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char ns_addr[1]="";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_16_saveFile_wrong_ns_addr_1)
{
   int ret;
   int64_t Ret;
   
   const char*ns_addr="10.232.65.369:4525";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_17_saveFile_wrong_ns_addr_2)
{
   int ret;
   int64_t Ret;
   
   const char*ns_addr="10.2s.6s.3o9:4s25";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
