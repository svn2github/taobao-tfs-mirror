#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_write_right)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=ret;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_EQ(100*(1<<10),Ret);
	
   ret=tfsclient->close(fd,tfs_name,tfs_name_len);
   EXPECT_EQ(ret,0);
   cout<<"the file name is "<<tfs_name<<endl;
}

TEST_F(TfsInit,02_write_right_less_count)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=ret;
   const int64_t count=small_len-10*(1<<10);
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_EQ(90*(1<<10),Ret);
	
   ret=tfsclient->close(fd,tfs_name,tfs_name_len);
   EXPECT_EQ(ret,0);
   cout<<"the file name is "<<tfs_name<<endl;
}

TEST_F(TfsInit,03_write_NULL_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char *buf=NULL;	
   const int fd=ret;
   const int64_t count=0;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(0,Ret);
	
}

TEST_F(TfsInit,04_write_empty_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char buf[1]="";
   cout<<"!!"<<buf<<"!!"<<endl;	
   const int fd=ret;
   const int64_t count=0;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_EQ(0,Ret);
	
   ret=tfsclient->close(fd,tfs_name,tfs_name_len);
   EXPECT_GT(0,ret);
   cout<<"the file name is "<<tfs_name<<endl;
}

TEST_F(TfsInit,05_write_samll_large_true)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = true;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));
   const int fd=ret;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(Ret,0);
}

TEST_F(TfsInit,06_write_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = true;
   const char* local_key = "/home/chenzewei.pt/testrc/resource/100k.jpg";

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   static char buf[large_len];
   data_in(a100M,buf,0,100*(1<<20));	
   const int fd=ret;
   const int64_t count=large_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_EQ(100*(1<<20),Ret);
	
   ret=tfsclient->close(fd,tfs_name,tfs_name_len);
   EXPECT_EQ(ret,0);
   cout<<"the file name is "<<tfs_name<<endl;
}


TEST_F(TfsInit,07_write_large_large_false)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = "/home/chenzewei.pt/testrc/resource/100k.jpg";

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   static char buf[large_len];
   data_in(a100M,buf,0,100*(1<<20));	
   const int fd=ret;
   const int64_t count=large_len;
   Ret=tfsclient->write(fd, buf,count);

   EXPECT_EQ(100*(1<<20),Ret);
   ret=tfsclient->close(fd,tfs_name,tfs_name_len);
   EXPECT_GT(ret,0);
   cout<<"the file name is "<<tfs_name<<endl;	
}

TEST_F(TfsInit,08_write_wrong_count)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix, tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=ret;
   const int64_t count=-1;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(0,Ret);
	
}

TEST_F(TfsInit,09_write_wrong_fd_1)
{
   int64_t Ret;

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=0;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_write_wrong_fd_2)
{
   int64_t Ret;

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=-1;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,11_write_wrong_fd_3)
{
   int64_t Ret;

   char buf[small_len];
   data_in(a100K,buf,0,100*(1<<10));	
   const int fd=27;
   const int64_t count=small_len;
   Ret=tfsclient->write(fd, buf,count);
   EXPECT_GT(0,Ret);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
