#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_fetch_buf_right_small)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 100*(1<<10));
}

TEST_F(TfsInit,02_fetch_buf_with_suffix)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,".jpg",false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = ".jpg";
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 100*(1<<10));
}

TEST_F(TfsInit,03_fetch_buf_wrong_suffix_1)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,".jpg",false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(0,iRet);
}

TEST_F(TfsInit,04_fetch_buf_wrong_suffix_2)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, NULL, false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = ".jpg";
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,05_fetch_buf_empty_suffix)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len, NULL, false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = "";
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(0,iRet);
}

TEST_F(TfsInit,06_fetch_buf_wrong_file_name)
{
   int iRet;
   int64_t ret_count = 0;
	
   char buf[100*(1<<10)];
   char * file_name = "T1oiujhgbnmklopiuh";
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(iRet,0);
}

TEST_F(TfsInit,07_fetch_buf_empty_file_name)
{
   int iRet;
   int64_t ret_count = 0;
	
   char buf[100*(1<<10)];
   char * file_name = "";
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,08_fetch_buf_NULL_file_name)
{
   int iRet;
   int64_t ret_count = 0;
	
   char buf[100*(1<<10)];
   char * file_name = NULL;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,09_fetch_buf_half_count)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 50*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 50*(1<<10));
}

TEST_F(TfsInit,10_fetch_buf_wrong_count)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = -1;

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,11_fetch_buf_NULL_buf)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * buf = NULL;
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 0;

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,12_fetch_buf_empty_buf)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   char * buf = "";
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 0;

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,13_fetch_buf_right_large)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a1G, tfs_name, tfs_name_len,NULL,true);
   EXPECT_EQ(1<<30,Ret);

   static char buf[1<<30];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 1<<30;

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 1<<30);
}

TEST_F(TfsInit,14_fetch_buf_with_DELETE)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int64_t ret_count = 0;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,15_fetch_buf_with_UNDELETE)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=DELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   action=UNDELETE;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 100*(1<<10));
}

TEST_F(TfsInit,16_fetch_buf_with_CONCEAL)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int64_t ret_count = 0;
   int iRet;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_GT(0,iRet);
}

TEST_F(TfsInit,17_fetch_buf_with_REVEAL)
{
   char tfs_name[19];	
   int32_t tfs_name_len=19;
   int64_t Ret;
   int iRet;
   int64_t ret_count = 0;

   Ret=tfsclient->save_file(a100K, tfs_name, tfs_name_len,NULL,false);
   EXPECT_EQ(100*(1<<10),Ret);

   TfsUnlinkType action=CONCEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   action=REVEAL;
   ret=tfsclient->unlink(tfs_name,NULL,action);
   EXPECT_EQ(0,ret);

   char buf[100*(1<<10)];
   char * file_name = tfs_name;
   char * suffix = NULL;
   int64_t count = 100*(1<<10);

   iRet = tfsclient->fetch_buf(ret_count, buf, count, file_name, suffix);
   EXPECT_EQ(iRet,0);
   EXPECT_EQ(ret_count, 100*(1<<10));
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















