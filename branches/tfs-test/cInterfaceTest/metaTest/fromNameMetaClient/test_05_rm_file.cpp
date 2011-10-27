#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_rmFile_right_filePath)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_rmFile_double_times)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_rmFile_not_exist)
{
   int ret;

   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_GT(0,ret);
}

//TEST_F(TfsInit,test_04_rmFile_null_filePath)
//{
//   int ret;
//
//   ret=tfsclient->rm_file(appId,uid,NULL);
//   EXPECT_GT(0,ret);
//}

TEST_F(TfsInit,test_05_rmFile_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->rm_file(appId,uid,name);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_06_rmFile_wrong_filePath_1)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"test");
   EXPECT_GT(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_rmFile_wrong_filePath_2)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"////test/////");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_08_rmFile_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->rm_file(appId,uid,"/");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_09_rmFile_small_File)
{
   int64_t Ret;
   int ret;
   
   Ret=tfsclient->save_file("10.232.36.208:7271",appId,uid,a100K, "/test");
   EXPECT_EQ(100*(1<<10),Ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_10_rmFile_large_File)
{
   int64_t Ret;
   int ret;
   Ret=tfsclient->save_file("10.232.36.208:7271",appId,uid,a1G, "/test");
   EXPECT_EQ(1<<30,Ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
