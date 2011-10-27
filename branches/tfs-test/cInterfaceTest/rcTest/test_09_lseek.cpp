#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_lseek_right_SET)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInit,02_lseek_right_CUR)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=0;
   const int whence=T_SEEK_CUR;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInit,03_lseek_wrong_offset)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=-1;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,04_lseek_more_offset)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=100*(1<<10)+10;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,05_lseek_wrong_whence_1)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=10*(1<<10);
   const int whence=8;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,06_lseek_wrong_whence_2)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=10*(1<<10);
   const int whence=-1;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,07_lseek_wrong_fd_1)
{
   int64_t Ret;
   const int fd=2;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,08_lseek_wrong_fd_2)
{
   int64_t Ret;
   const int fd=0;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,09_lseek_wrong_fd_3)
{
   int64_t Ret;
   const int fd=-1;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_lseek_CREATE)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,11_lseek_STAT)
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
   
   ret=tfsclient->open(file_name, suffix, tfsclient->STAT, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,12_lseek_WRITE_1)
{  
   int ret; 
   const char*file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
  
   int64_t Ret; 
   const int fd=ret;
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_EQ(0,Ret);
}

TEST_F(TfsInit,13_lseek_WRITE_2)
{   
   const char*file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
   
   int64_t Ret;
   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));
   const int fd=ret;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const int64_t offset=0;
   const int whence=T_SEEK_SET;
   
   Ret=tfsclient->lseek(fd,offset,whence);
   EXPECT_EQ(0,Ret);
}
int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}







































