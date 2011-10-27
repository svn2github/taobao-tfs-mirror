#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_read_right)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   char Rbuffer[small_len];
   offset=0;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_read_with_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   char Rbuffer[small_len];
   offset=1024;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_EQ(small_len-1024,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_read_more_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   char Rbuffer[small_len];
   offset=small_len+1;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_read_wrong_offset)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   char Rbuffer[small_len];
   offset=-1;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_read_null_buffer)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   char buffer[small_len];
   data_in(a100K,buffer,0,small_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,small_len,offset);
   EXPECT_EQ(small_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   offset=0;
   Ret=tfsclient->pread(fd,NULL,small_len,offset);
   EXPECT_GT(0,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_read_wrong_fd_1)
{
   int64_t Ret;  
   int fd=-1;
   char Rbuffer[small_len];
   int64_t offset=0;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_07_read_wrong_fd_2)
{
   int64_t Ret;  
   int fd=0;
   char Rbuffer[small_len];
   int64_t offset=0;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_08_read_not_open_fd)
{
   int64_t Ret;  
   int fd=1;
   char Rbuffer[small_len];
   int64_t offset=0;
   Ret=tfsclient->pread(fd,Rbuffer,small_len,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,test_09_read_large)
{
   int ret;
   int64_t Ret;
   
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   
   char*name="/test"; 
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   int fd =ret;
   static char buffer[vlarge_len];
   data_in(a1G,buffer,0,vlarge_len);
   int64_t offset=0;
   Ret=tfsclient->pwrite(fd,buffer,vlarge_len,offset);
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   fd=ret;
   offset=0;
   Ret=tfsclient->pread(fd,buffer,vlarge_len,offset);
   EXPECT_EQ(vlarge_len,Ret);
   
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
