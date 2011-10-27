#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_save_file_byte_right)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_EQ(100*(1<<10),Ret);
}

TEST_F(TfsInit,02_save_file_byte_less_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int32_t data_len=50*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_EQ(50*(1<<10),Ret);
}

TEST_F(TfsInit,03_save_file_byte_0_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int32_t data_len=0;
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,04_save_file_byte_wrong_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int32_t data_len=-1;
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,05_save_file_byte_large)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   static char buf[1<<30];
   int32_t data_len=1<<30;
   int64_t Ret;

   data_in(a1G,buf,0,1<<30);
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_EQ(1<<30,Ret);
}

//TEST_F(TfsInit,06_save_file_byte_large_part)
//{
//   char tfs_name[19];
//   int32_t tfs_name_len=19;
//   static char buf[1<<20];
//   int32_t data_len=100*(1<<20);
//   int64_t Ret;
//
//   data_in(a1G,buf,0,1<<30);
//   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
//   
//   EXPECT_EQ(100*(1<<20),Ret);
//}

TEST_F(TfsInit,07_save_file_byte_empty_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   char buf[1]="";
   int32_t data_len=1;
   int64_t Ret;
   
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_EQ(1,Ret);
}

TEST_F(TfsInit,08_save_file_byte_null_buf)
{
   char tfs_name[19];
   int32_t tfs_name_len=19;
   int32_t data_len=0;
   int64_t Ret;
   
   Ret=tfsclient->save_file(NULL,data_len,tfs_name,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,09_save_file_byte_null_tfs_name)
{
   int32_t tfs_name_len=19;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,NULL,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,10_save_file_byte_lese_name)
{
   char tfs_name[5];
   int32_t tfs_name_len=5;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,11_save_file_byte_lese_tfs_name_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=5;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,12_save_file_byte_wrong_tfs_name_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=-1;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);

   EXPECT_GT(0,Ret);
}

TEST_F(TfsInit,13_save_file_byte_mroe_tfs_name_len)
{
   char tfs_name[19];
   int32_t tfs_name_len=20;
   char buf[100*(1<<10)];
   int32_t data_len=100*(1<<10);
   int64_t Ret;

   data_in(a100K,buf,0,100*(1<<10));
   Ret=tfsclient->save_file(buf,data_len,tfs_name,tfs_name_len);
   
   EXPECT_EQ(100*(1<<10),Ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
















