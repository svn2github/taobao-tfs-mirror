#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_save_file_right_small)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_EQ(100*(1<<10),Ret);
}

TEST_F(TfsInit,02_save_file_right_small_wrong_flag)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(100*(1<<10),Ret);

}

TEST_F(TfsInit,03_save_file_right_large)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(a1G, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(1<<30,Ret);
}

TEST_F(TfsInit,04_save_file_right_large_wrong_flag)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(a1G, tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,05_save_file_null_local_file )
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(NULL, tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,06_save_file_empty_local_file )
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   char name[1]="";

   Ret=tfsclient->save_file(name, tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,07_save_file_not_exist_local_file )
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file("/home/sdhsajkdh", tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,08_save_file_null_tfs_name)
{	
   int32_t tfs_name_len=19;
   int64_t Ret;

   Ret=tfsclient->save_file(a100K, NULL, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,09_save_file_samll_tfs_name_buf)
{
   char tfs_name[5];	
   int32_t tfs_name_len=5;
   int64_t Ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_save_file_small_less_tfs_name_len)
{
   char tfs_name[19];	
   int32_t tfs_name_len=10;
   int64_t Ret;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,false);
   EXPECT_GT(0,Ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















