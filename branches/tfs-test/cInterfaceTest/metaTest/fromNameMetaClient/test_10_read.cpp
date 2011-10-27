#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_read_right)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_read_with_offset)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=1024;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length-1024);
   EXPECT_EQ(small_len-1024,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_read_more_offset)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len+1024];
   const int64_t offset=small_len+1;
   const int64_t length=10;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_EQ(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_read_wrong_offset)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=-1;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_read_null_buffer)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", NULL,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

//TEST_F(TfsInit,test_06_read_null_filePath)
//{
//   int64_t Ret;
//   
//   const char* ns_addr="10.232.36.208:7271"; 
//   char buffer[small_len];
//   const int64_t offset=0;
//   const int64_t length=small_len;
//   Ret=tfsclient->read(ns_addr,appId,uid,NULL, buffer,offset,length);
//   EXPECT_GT(0,Ret);  
//}

TEST_F(TfsInit,test_07_read_empty_filePath)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   char name[1]="";
   Ret=tfsclient->read(ns_addr,appId,uid,name, buffer,offset,length);
   EXPECT_GT(0,Ret);  
}

TEST_F(TfsInit,test_08_read_wrong_filePath_1)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_read_wrong_filePath_2)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"///test///", buffer,offset,length);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_10_read_large)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a1G,"/test");
   EXPECT_EQ(vlarge_len,Ret);
   
   static char buffer[vlarge_len];
   const int64_t offset=0;
   const int64_t length=vlarge_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_11_read_not_exist)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);  
}

TEST_F(TfsInit,test_12_read_Dir)
{
   int ret;
   int64_t Ret;
    
   const char* ns_addr="10.232.36.208:7271";
   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   Ret=tfsclient->read(ns_addr,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_read_NULL_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   char*name=NULL;
   Ret=tfsclient->read(name,appId,uid,"/test", buffer,offset,length);
   EXPECT_EQ(100*(1<<10),Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_read_empty_ns_addr)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   char name[1]="";
   Ret=tfsclient->read(name,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_15_read_wrong_ns_addr_1)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   char*name="10.232.58.236:8888";
   Ret=tfsclient->read(name,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_16_read_wrong_ns_addr_2)
{
   int ret;
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   Ret=tfsclient->save_file(ns_addr,appId,uid,a100K,"/test");
   EXPECT_EQ(small_len,Ret);
   
   char buffer[small_len];
   const int64_t offset=0;
   const int64_t length=small_len;
   char*name="10.2ss.5w.23ds:8888";
   Ret=tfsclient->read(name,appId,uid,"/test", buffer,offset,length);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
