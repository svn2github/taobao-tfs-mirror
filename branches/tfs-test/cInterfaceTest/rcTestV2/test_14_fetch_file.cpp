#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_fetch_file_right_small)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(iRet,0);
}

TEST_F(TfsInit,02_fetch_file_with_suffix)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, ".jpg",false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = ".jpg";

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(iRet,0);
}

TEST_F(TfsInit,03_fetch_file_wrong_suffix_1)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, ".jpg",false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(0,iRet);
}

TEST_F(TfsInit,04_fetch_file_wrong_suffix_2)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = ".jpg";

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_NE(iRet,0);
}

TEST_F(TfsInit,05_fetch_file_empty_suffix)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = "";

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(0,iRet);
}

TEST_F(TfsInit,06_fetch_file_wrong_file_name)
{
   int iRet = 9;

   char * local_file = "TEMP";
   char * file_name = "T2hgt7uioplkjhnmbg";
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   printf("!!!!!%d",iRet);
   EXPECT_EQ(1,iRet);
}

TEST_F(TfsInit,07_fetch_file_empty_file_name)
{
   int iRet;

   char * local_file = "TEMP";
   char * file_name = "";
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_NE(0,iRet);
}

TEST_F(TfsInit,08_fetch_file_NULL_file_name)
{
   int iRet;

   char * local_file = "TEMP";
   char * file_name = NULL;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_NE(0,iRet);
}

TEST_F(TfsInit,09_fetch_file_empty_local_file)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = "";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(1,iRet);
}

TEST_F(TfsInit,10_fetch_file_wrong_local_file)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * local_file = NULL;
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_NE(0,iRet);
}


TEST_F(TfsInit,11_fetch_file_right_large)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a1G, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(1<<30,Ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(0,iRet);
}

TEST_F(TfsInit,12_fetch_file_with_DELETE)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(1,iRet);
}

TEST_F(TfsInit,13_fetch_file_with_UNDELETE)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   action=UNDELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(iRet,0);
}

TEST_F(TfsInit,14_fetch_file_with_CONCEAL)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(1,iRet);
}

TEST_F(TfsInit,15_fetch_file_with_REVEAL)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   action=REVEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char * local_file = "TEMP";
   char * file_name = tfs_name;
   char * suffix = NULL;

   iRet =tfsclient->fetch_file(local_file, file_name, suffix);
   EXPECT_EQ(iRet,0);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















