#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_pread_right)
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
   char buf[small_len];
   const int64_t count=small_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(small_len,Ret);
}

TEST_F(TfsInit,02_pread_less_count)
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
   char buf[small_len];
   const int64_t count=small_len-10*(1<<10);
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(small_len-10*(1<<10),Ret);
}

TEST_F(TfsInit,03_pread_null_buf)
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
   char*buf=NULL;
   const int64_t count=small_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,04_pread_less_buf)
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
   char buf[small_len-10*(1<<10)];
   const int64_t count=small_len-10*(1<<10);
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(small_len-10*(1<<10),Ret);
}

TEST_F(TfsInit,05_pread_more_buf)
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
   char buf[small_len+10*(1<<10)];
   const int64_t count=small_len+10*(1<<10);
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(small_len,Ret);
}

TEST_F(TfsInit,06_pread_wrong_count)
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
   char buf[small_len];
   const int64_t count=-1;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,07_pread_wrong_fd_1)
{
   int64_t Ret;
   const int fd=-1;
   char buf[small_len];
   const int64_t count=small_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,08_pread_wrong_fd_2)
{
   int64_t Ret;
   const int fd=0;
   char buf[small_len];
   const int64_t count=small_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,09_pread_wrong_fd_3)
{
   int64_t Ret;
   const int fd=1;
   char buf[small_len];
   const int64_t count=small_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_pread_right_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,true);
   EXPECT_EQ(100*(1<<20),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = true;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   static char buf[large_len];
   const int64_t count=large_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(large_len,Ret);
}

TEST_F(TfsInit,11_pread_wrong_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,true);
   EXPECT_EQ(100*(1<<20),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;
   
   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   
   const int fd=ret;
   static char buf[large_len];
   const int64_t count=large_len;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(large_len,Ret);
}

TEST_F(TfsInit,12_pread_with_offset)
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
   char buf[small_len];
   const int64_t count=small_len-10*(1<<10);
   const int64_t offset=10*(1<<10);
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_EQ(small_len-10*(1<<10),Ret);
}

TEST_F(TfsInit,13_pread_wrong_offset)
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
   char buf[small_len];
   const int64_t count=small_len;
   const int64_t offset=-1;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,14_pread_more_offset)
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
   char buf[small_len];
   const int64_t count=5;
   const int64_t offset=small_len+1;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,15_pread_mroe_count)
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
   char buf[small_len];
   const int64_t count=small_len+1;
   const int64_t offset=0;
   
   Ret=tfsclient->pread(fd, buf,count,offset);
   EXPECT_GT(0,Ret);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















