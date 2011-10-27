#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_mvFile_right)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","/test2");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_02_mvFile_null_destFilePath)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1",NULL);
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_mvFile_empty_destFilePath)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   char name[1]="";
   ret=tfsclient->mv_file(uid,"/test1",name);
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_mvFile_wrong_destFilePath_1)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","/");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_mvFile_wrong_destFilePath_2)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","test2");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_mvFile_wrong_destFilePath_3)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","////test2////");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_07_mvFile_leap_Filename)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","/test2/test");
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_08_mvFile_exist_Filename)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/test2");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","/test2");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/test2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_mvFile_null_srcFilePath)
{
   int ret;
   
   ret=tfsclient->mv_file(uid,NULL,"/test2");
   EXPECT_GT(0,ret);   
}

TEST_F(TfsInit,test_10_mvFile_empty_srcFilePath)
{
   int ret;
   
   char name[1]="";
   ret=tfsclient->mv_file(uid,name,"/test2");
   EXPECT_GT(ret,0);   
}

TEST_F(TfsInit,test_11_mvFile_wrong_srcFilePath_1)
{
   int ret;
   
   ret=tfsclient->mv_file(uid,"/","/test2");
   EXPECT_GT(ret,0);   
}

TEST_F(TfsInit,test_12_mvFile_wrong_srcFilePath_2)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"test1","/test2");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_mvFile_with_same_name)
{
   int ret;
   
   ret=tfsclient->create_file(uid,"/test1");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_file(uid,"/test1","/test1");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/test1");
   EXPECT_EQ(0,ret);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
