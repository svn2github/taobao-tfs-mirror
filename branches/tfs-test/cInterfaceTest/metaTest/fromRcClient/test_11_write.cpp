#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_write_right)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite1");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite1";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_write_with_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite2");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite2";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=1024;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_write_n_1_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite3");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite3";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=-1;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite3");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_write_wrong_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite4");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite4";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=-2;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite4");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_write_empty_data)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite5");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite5";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[1]="";
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,0,offset);
   EXPECT_EQ(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite5");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_write_null_data)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite6");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite6";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char*buffer=NULL;
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,0,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite6");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_write_wrong_fd_1)
{
   int64_t Ret;
   
   int fd =-1;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_08_write_wrong_fd_2)
{
   int64_t Ret;
   
   int fd =0;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_09_write_not_open_fd)
{
   int64_t Ret;
   
   int fd =1;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_10_write_many_times)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite10");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite10";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite10");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_11_write_large)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite11");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite11";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   static char buffer[vlarge_len];
   data_in(a1G,buffer,0,vlarge_len);
   const int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,vlarge_len,offset);
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite11");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_12_write_many_times_parts)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite12");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite12";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer1[10*(1<<10)];
   data_in(a10K,buffer1,0,10*(1<<10));
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer1,10*(1<<10),offset);
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer2[10*(1<<10)];
   data_in(a10K,buffer2,0,10*(1<<10));
   offset=10*(1<<10);
   Ret=tfsclient->pwrite(fd,buffer2,10*(1<<10), offset);
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer3[10*(1<<10)];
   data_in(a10K,buffer3,0,10*(1<<10));
   offset=20*(1<<10)+1;
   Ret=tfsclient->pwrite(fd,buffer3,10*(1<<10), offset);
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer4[1];
   data_in(a1B,buffer4,0,1);
   offset=20*(1<<10);
   Ret=tfsclient->pwrite(fd, buffer4,1, offset);
   EXPECT_EQ(1,Ret);
   
   char buffer5[1];
   data_in(a1B,buffer5,0,1);
   offset=20*(1<<10);
   Ret=tfsclient->pwrite(fd,buffer5,1, offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite12");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_write_large_many_times_parts)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite13");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite13";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer1[3*(1<<20)];
   data_in(a3M,buffer1,0,3*(1<<20));
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd, buffer1,3*(1<<20), offset);
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer2[3*(1<<20)];
   data_in(a3M,buffer2,0,3*(1<<20));
   offset=3*(1<<20);
   Ret=tfsclient->pwrite(fd, buffer2,3*(1<<20), offset);
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer3[3*(1<<20)];
   data_in(a3M,buffer3,0,3*(1<<20));
   offset=6*(1<<20)+1;
   Ret=tfsclient->pwrite(fd, buffer3,3*(1<<20), offset);
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer4[1];
   data_in(a1B,buffer4,0,1);
   offset=6*(1<<20);
   Ret=tfsclient->pwrite(fd, buffer4,1, offset);
   EXPECT_EQ(1,Ret);
   
   char buffer5[1];
   data_in(a1B,buffer5,0,1);
   offset=6*(1<<20);
   Ret=tfsclient->pwrite(fd,buffer5,1, offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite13");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_write_many_times_parts_com)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/metarcgtestwrite14");
   EXPECT_EQ(0,ret);
   
   char*name="/metarcgtestwrite14";  
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer1[10*(1<<10)];
   data_in(a10K,buffer1,0,10*(1<<10));
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd, buffer1,10*(1<<10), offset);
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer2[2*(1<<20)];
   data_in(a2M,buffer2,0,2*(1<<20));
   offset=20*(1<<10);
   Ret=tfsclient->pwrite(fd,buffer2,2*(1<<20), offset);
   EXPECT_EQ(2*(1<<20),Ret);
   
   char buffer3[3*(1<<20)];
   data_in(a3M,buffer3,0,3*(1<<20));
   offset=8*(1<<20)+20*(1<<10);
   Ret=tfsclient->pwrite(fd, buffer3,3*(1<<20), offset);
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer4[3*(1<<20)];
   data_in(a3M,buffer4,0,3*(1<<20));
   offset=3*(1<<20)+20*(1<<10);
   Ret=tfsclient->pwrite(fd, buffer4,3*(1<<20), offset);
   EXPECT_EQ(3*(1<<20),Ret);
   
   char buffer5[10*(1<<10)];
   data_in(a10K,buffer5,0,10*(1<<10));
   offset=10*(1<<10);
   Ret=tfsclient->pwrite(fd, buffer5,10*(1<<10), offset);
   EXPECT_EQ(10*(1<<10),Ret);
   
   char buffer6[10*(1<<10)];
   data_in(a10K,buffer6,0,10*(1<<10));
   offset=0;
   Ret=tfsclient->pwrite(fd, buffer6,10*(1<<10), offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestwrite14");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
