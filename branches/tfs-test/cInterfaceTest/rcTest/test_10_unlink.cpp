#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_unlink_right)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,02_unlink_wrong_file_name)
{
   const char* file_name="Tuiwjsnghaljuanghja";
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,03_unlink_NULL_file_name)
{
   const char* file_name=NULL;
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,04_unlink_empty_file_name)
{
   const char file_name[1]="";
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,05_unlink_with_wrong_suffix_1)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = ".jpg";
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,06_unlink_with_empty_suffix)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char suffix[1] = "";
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,07_unlink_with_suffix)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=".jpg";
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
   
   const char* file_name_u=tfs_name;
   const char*suffix_u = ".jpg";
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name_u,suffix_u,action);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,08_unlink_with_wrong_suffix_2)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=".jpg";
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
   
   const char* file_name_u=tfs_name;
   const char*suffix_u = ".txt";
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name_u,suffix_u,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,09_unlink_with_wrong_suffix_3)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   const char* file_name=NULL;
   const char* suffix=".jpg";
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
   
   const char* file_name_u=tfs_name;
   const char*suffix_u = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name_u,suffix_u,action);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,10_unlink_two_times)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,11_unlink_UNDELETE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=UNDELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,12_unlink_UNDELETE_two_times)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   action=UNDELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   action=UNDELETE;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,13_unlink_CONCEAL_two_times)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,14_unlink_REVEAL)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=REVEAL;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,15_unlink_REVEAL_two_times)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
   
   const char* file_name=tfs_name;
   const char* suffix = NULL;
   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   action=REVEAL;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_EQ(0,ret);
   action=REVEAL;
   ret=tfsclient->unlink(file_name,suffix,action);
   EXPECT_GT(ret,0);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}




