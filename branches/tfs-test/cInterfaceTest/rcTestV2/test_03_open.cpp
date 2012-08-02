#include"tfs_client_impl_init.h"

TEST_F(TfsInit,01_open_right_CREATE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,02_open_right_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,03_open_right_READ_FORCE)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ_FORCE, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,04_open_right_CREATE_other_way)
{
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,05_open_null_tfs_name_READ)
{
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,06_open_with_suffix_CREATE)
{
   const char* file_name=NULL;
   const char* suffix=".jpg";
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,07_open_empty_suffix_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);

   const char* file_name=tfs_name;
   char suffix[1]="";
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,08_open_samll_with_large_true_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = true;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,09_open_large_with_large_false_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(100*(1<<20),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,10_open_large_with_large_true_READ)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int64_t Ret;
   int ret;

   Ret=tfsclient->save_file(a100M, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(100*(1<<20),Ret);

   const char* file_name=tfs_name;
   const char* suffix=NULL;
   const bool  large = true;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(ret,0);
}


TEST_F(TfsInit,11_open_wrong_null_CREATE)
{
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = false;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->CREATE, large,local_key);
   EXPECT_GT(ret,0);
}



TEST_F(TfsInit,12_open_wrong_null_READ_large)
{
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = true;
   const char* local_key = NULL;

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,13_open_wrong_empty_READ_large)
{
   const char* file_name=NULL;
   const char* suffix=NULL;
   const bool  large = true;
   const char  local_key[1] = "";

   ret=tfsclient->open(file_name, suffix,tfsclient->READ, large,local_key);
   EXPECT_GT(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}































