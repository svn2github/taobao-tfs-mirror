#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_readv2_right)
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

   int fd=ret;
   char buf[small_len];
   const int64_t count=small_len;
   TfsFileStat tfs_stat_buf;

   cout<<"@@@@@@@@@@  "<<fd<<endl;
   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
   fd=ret;
   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   cout<<"@@@@@@@@@@  "<<fd<<endl;

   EXPECT_EQ(small_len,Ret);
   EXPECT_EQ(tfs_stat_buf.size_,small_len);
}

TEST_F(TfsInit,02_readv2_less_count)
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_EQ(small_len-10*(1<<10),Ret);
   EXPECT_EQ(tfs_stat_buf.size_,small_len);
}

TEST_F(TfsInit,03_readv2_null_buf)
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,04_readv2_less_buf)
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_EQ(small_len-10*(1<<10),Ret);
   EXPECT_EQ(tfs_stat_buf.size_,small_len);
}

TEST_F(TfsInit,05_readv2_more_buf)
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_EQ(small_len,Ret);
   EXPECT_EQ(tfs_stat_buf.size_,small_len);
}

TEST_F(TfsInit,06_readv2_wrong_count)
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,07_readv2_wrong_fd_1)
{
   int64_t Ret;
   const int fd=-1;
   char buf[small_len];
   const int64_t count=small_len;
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,08_readv2_wrong_fd_2)
{
   int64_t Ret;
   const int fd=0;
   char buf[small_len];
   const int64_t count=small_len;
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,09_readv2_wrong_fd_3)
{
   int64_t Ret;
   const int fd=1;
   char buf[small_len];
   const int64_t count=small_len;
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_readv2_right_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,NULL,true);
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_EQ(large_len,Ret);
   EXPECT_EQ(tfs_stat_buf.size_,large_len);
}

TEST_F(TfsInit,11_readv2_wrong_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,NULL,true);
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
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_GT(large_len,Ret);
}

TEST_F(TfsInit,12_readv2_more_count)
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
   char buf[small_len+1];
   const int64_t count=small_len+1;
   TfsFileStat tfs_stat_buf;

   Ret=tfsclient->readv2(fd, buf,count,&tfs_stat_buf);
   EXPECT_EQ(102400,Ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















