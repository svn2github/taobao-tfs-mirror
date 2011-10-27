#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_fstat_right)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->STAT, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_EQ(0,ret);
   EXPECT_EQ(buf.size_,100*(1<<10));
   
   tfsclient->close(fd);
}

TEST_F(TfsInit,02_fstat_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(0,ret);
   
   tfsclient->close(fd);
}

TEST_F(TfsInit,03_fstat_CREATE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(0,ret);
   
   tfsclient->close(fd);
}

TEST_F(TfsInit,04_fstat_WRITE)
{ 
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;
   int ret;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,05_fstat_null_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->STAT, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   TfsFileStat*buf=NULL;
   ret=tfsclient->fstat(fd, buf);
   EXPECT_GT(ret,0);
    
   tfsclient->close(fd);
}

TEST_F(TfsInit,06_fstat_wrong_fd_1)
{ 
   const int fd=-1;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,07_fstat_wrong_fd_2)
{ 
   const int fd=0;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,08_fstat_wrong_fd_3)
{ 
   const int fd=2;
   TfsFileStat buf;
   ret=tfsclient->fstat(fd, &buf);
   EXPECT_GT(ret,0);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}














