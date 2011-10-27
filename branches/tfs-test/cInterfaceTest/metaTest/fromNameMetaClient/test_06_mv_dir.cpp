#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_mvDir_right)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_02_mvDir_srcFilePath_with_File)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test1/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test2/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_mvDir_srcFilePath_with_File_and_Dir)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test1/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test1/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test2/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test2/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_mvDir_exit_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_mvDir_empty_srcFilePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->mv_dir(appId,uid,name,"/test2");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_mvDir_null_srcFilePath)
{
   int ret;
   ret=tfsclient->mv_dir(appId,uid,NULL,"/test2");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_07_mvDir_wrong_srcFilePath_1)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"test1","/test2");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_08_mvDir_wrong_srcFilePath_2)
{
   int ret;

   ret=tfsclient->mv_dir(appId,uid,"/","/test2");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_09_mvDir_empty_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   char name[1]="";
   ret=tfsclient->mv_dir(appId,uid,"/test1",name);
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_10_mvDir_null_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1",NULL);
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_11_mvDir_wrong_destFilePath_1)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1","/");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_12_mvDir_wrong_destFilePath_2)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1","test2");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_mvDir_wrong_destFilePath_3)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1","///test2///");
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_14_mvDir_with_leap_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2/text");
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_15_mvDir_with_same_File_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test2");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_16_mvDir_srcFilePath_same_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test1");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_17_mvDir_complex)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test1/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test1/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test1/test1/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test1/test1/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test2/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(appId,uid,"/test2/test3");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1/test1","/test2/test4");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(appId,uid,"/test1/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test2/test4/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2/test4/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2/test4");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(appId,uid,"/test2/test3");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_19_mvDir_circle)
{
   int ret;

   ret=tfsclient->create_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test1/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(appId,uid,"/test1/test2/test3");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(appId,uid,"/test1","/test1/test2/test3");
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_dir(appId,uid,"/test1/test2/test3");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(appId,uid,"/test1");
   EXPECT_EQ(0,ret);
}
int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
