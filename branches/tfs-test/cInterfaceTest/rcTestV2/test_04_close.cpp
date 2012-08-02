#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_close_right_CREATE_1)
{
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len = 19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,02_close_right_CREATE_2)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len = 19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,03_close_right_READ)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->READ , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char* tfs_name_buff = NULL;
   const int32_t buff_len = 0;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,04_close_right_READ_FORCE)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->READ_FORCE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char* tfs_name_buff = NULL;
   const int32_t buff_len = 0;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,05_close_with_name_WRITE)
{
   int ret;

   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len=19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
   cout<<"the tfs name is "<<tfs_name_buff<<endl;
}


TEST_F(TfsInit,06_close_with_name_CREATE)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len = 19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
   cout<<"the tfs name is "<<tfs_name_buff<<endl;
}

TEST_F(TfsInit,07_close_with_name_READ)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->READ , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len = 19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
   cout<<"the tfs name is "<<tfs_name_buff<<endl;
}

TEST_F(TfsInit,08_close_with_name_READ_FORCE)
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

   ret=tfsclient->open(file_name, suffix,tfsclient->READ_FORCE , large,local_key);
   EXPECT_GT(ret,0);

   const int fd=ret;
   char tfs_name_buff[19];
   const int32_t buff_len = 19;
   ret=tfsclient->close(fd,tfs_name_buff,buff_len);
   EXPECT_EQ(0,ret);
   cout<<"the tfs name is "<<tfs_name_buff<<endl;
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}

























