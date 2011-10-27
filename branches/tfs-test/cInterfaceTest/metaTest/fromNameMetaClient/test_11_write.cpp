#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_write_right)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_write_with_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=1024;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_write_n_1_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=-1;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_write_wrong_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=-2;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_write_empty_data)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[1]="";
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,0);
   EXPECT_EQ(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_write_null_data)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", NULL, offset,0);
   EXPECT_EQ(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_write_null_filename)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,NULL, buffer, offset,small_len);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_08_write_empty_filename)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   char name[1]="";
   Ret=tfsclient->write(ns_addr,appId,uid,name, buffer, offset,small_len);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_09_write_wrong_filename_1)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"///test//", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}


TEST_F(TfsInit,test_10_write_wrong_filename_2)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_11_write_wrong_filename_3)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_12_write_Dir)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_write_many_times)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_write_not_exist)
{
   int64_t Ret;
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_18_write_large)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   static char buffer[vlarge_len];
   data_in(a1G,buffer,0,vlarge_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,vlarge_len);
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_19_write_NULL_ns_addr)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr=NULL;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_20_write_empty_ns_addr)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char ns_addr[1]="";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_21_write_wrong_ns_addr_1)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.58.236:8888";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_22_write_wrong_ns_addr_1)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.2ss.5w.23ds:8888";
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer, offset,small_len);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_23_write_many_times_parts)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer1[10*(1<<10)];
   data_in(a10K,buffer1,0,10*(1<<10));
   int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer1, offset,10*(1<<10));
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer2[10*(1<<10)];
   data_in(a10K,buffer2,0,10*(1<<10));
   offset=10*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer2, offset,10*(1<<10));
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer3[10*(1<<10)];
   data_in(a10K,buffer3,0,10*(1<<10));
   offset=20*(1<<10)+1;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer3, offset,10*(1<<10));
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer4[1];
   data_in(a1B,buffer4,0,1);
   offset=20*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer4, offset,1);
   EXPECT_EQ(1,Ret);
   
   char buffer5[1];
   data_in(a1B,buffer5,0,1);
   offset=20*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer5, offset,1);
   EXPECT_GT(0,Ret);
   
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_24_write_large_many_times_parts)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer1[3*(1<<20)];
   data_in(a3M,buffer1,0,3*(1<<20));
   int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer1, offset,3*(1<<20));
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer2[3*(1<<20)];
   data_in(a3M,buffer2,0,3*(1<<20));
   offset=3*(1<<20);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer2, offset,3*(1<<20));
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer3[3*(1<<20)];
   data_in(a3M,buffer3,0,3*(1<<20));
   offset=6*(1<<20)+1;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer3, offset,3*(1<<20));
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer4[1];
   data_in(a1B,buffer4,0,1);
   offset=6*(1<<20);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer4, offset,1);
   EXPECT_EQ(1,Ret);
   
   char buffer5[1];
   data_in(a1B,buffer5,0,1);
   offset=6*(1<<20);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer5, offset,1);
   EXPECT_GT(0,Ret);
   
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_25_write_many_times_parts_com)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   
   const char* ns_addr="10.232.36.208:7271";
   char buffer1[10*(1<<10)];
   data_in(a10K,buffer1,0,10*(1<<10));
   int64_t offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer1, offset,10*(1<<10));
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer2[2*(1<<20)];
   data_in(a2M,buffer2,0,2*(1<<20));
   offset=20*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer2, offset,2*(1<<20));
   EXPECT_EQ(2*(1<<20),Ret);
   
   char buffer3[3*(1<<20)];
   data_in(a3M,buffer3,0,3*(1<<20));
   offset=8*(1<<20)+20*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer3, offset,3*(1<<20));
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer4[3*(1<<20)];
   data_in(a3M,buffer4,0,3*(1<<20));
   offset=3*(1<<20)+20*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer4, offset,3*(1<<20));
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer5[10*(1<<10)];
   data_in(a10K,buffer5,0,10*(1<<10));
   offset=10*(1<<10);
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer5, offset,10*(1<<10));
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer6[10*(1<<10)];
   data_in(a10K,buffer6,0,10*(1<<10));
   offset=0;
   Ret=tfsclient->write(ns_addr,appId,uid,"/test", buffer6, offset,10*(1<<10));
   EXPECT_GT(0,Ret);
   
   
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
