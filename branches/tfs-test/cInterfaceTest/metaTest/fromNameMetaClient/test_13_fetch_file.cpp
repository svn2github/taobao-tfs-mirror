#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_fetchFile_right)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","/test");
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_fetchFile_null_localFile)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,NULL,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_fetchFile_empty_localFile)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char name[1]="";
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,name,"/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_fetchFile_wrong_localFile)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"sjlkdsjkahska","/test");
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

//TEST_F(TfsInit,test_05_fetchFile_null_fileName)
//{
//   int ret;
//   int64_t Ret;
//   
//   const char* ns_addr="10.232.36.208:7271";
//   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
//   EXPECT_EQ(small_len,Ret);
//   
//   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP",NULL);
//   EXPECT_GT(0,Ret);
//   
//   ret=tfsclient->rm_file(appId,uid,"/test");
//   EXPECT_EQ(0,ret);
//}
//
TEST_F(TfsInit,test_06_fetchFile_empty_fileName)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char name[1]="";
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP",name);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_fetchFile_wrong_fileName_1)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_08_fetchFile_wrong_fileName_2)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","///test///");
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_fetchFile_wrong_fileName_3)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","/");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_10_fetchFile_not_exist_fileName)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_11_fetchFile_with_Dir)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_12_fetchFile_large)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a1G,"/test");
   EXPECT_EQ(vlarge_len,Ret);
   
   Ret=tfsclient->fetch_file(ns_addr,appId,uid,"TEMP","/test");
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_fetchFile_NULL_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
    
   char*name=NULL;
   Ret=tfsclient->fetch_file(name,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_fetchFile_empty_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
    
   char name[1]="";
   Ret=tfsclient->fetch_file(name,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_15_fetchFile_wrong_ns_addr_1)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
    
   char*name="10.232.65.369:4525";
   Ret=tfsclient->fetch_file(name,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_16_fetchFile_wrong_ns_addr_2)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
    
   char*name="10.2s.6s.3o9:4s25";
   Ret=tfsclient->fetch_file(name,appId,uid,"TEMP","/test");
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
