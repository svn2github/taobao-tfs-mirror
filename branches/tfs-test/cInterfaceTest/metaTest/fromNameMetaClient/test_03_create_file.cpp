#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_createFile_right_filePath)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_createFile_null_filePath)
{
   int ret;
   ret=tfsclient->create_file(appId,uid,NULL);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_03_createFile_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->create_file(appId,uid,name);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_04_createFile_wrong_filePath_1)
{
   int ret;
   //ret=tfsclient->create_file(appId,uid,"/");
   //EXPECT_GT(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/");
}

TEST_F(TfsInit,test_05_createFile_wrong_filePath_2)
{
   int ret;
   ret=tfsclient->create_file(appId,uid,"test");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_createFile_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"///test///");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_createFile_leap_filePath)
{
   int ret;
   ret=tfsclient->create_file(appId,uid,"/test/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_08_createFile_same_File_name)
{
   int ret;

   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_GT(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_createFile_with_same_Dir)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}












