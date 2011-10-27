#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_rmDir_right_filePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_rmDir_double_times)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_rmDir_not_exist_filePath)
{
   int ret;
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_rmDir_null_filePath)
{
   int ret;
   ret=tfsclient->rm_dir(appId,uid,NULL);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_05_rmDir_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->rm_dir(appId,uid,name);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_rmDir_wrong_filePath_1)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"test");
   EXPECT_GT(ret,0);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_rmDir_wrong_filePath_2)
{
   int ret;
   ret=tfsclient->rm_dir(appId,uid,"/");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_08_rmDir_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"///test////");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_rmDir_filePath_with_File)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_GT(0,ret);
   tfsclient->rm_file(appId,uid,"/test/test");
   tfsclient->rm_dir(appId,uid,"/test");
}

TEST_F(TfsInit,test_10_rmDir_filePath_with_Dir)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test");
   EXPECT_GT(0,ret);
   tfsclient->rm_dir(appId,uid,"/test/test");
   tfsclient->rm_dir(appId,uid,"/test");
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}



































