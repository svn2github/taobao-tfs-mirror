#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_createFile_right_filePath)
{
   int ret;

   ret=tfsclient->create_file(uid,"/metarcgtestcreateFile1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestcreateFile1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_createFile_null_filePath)
{
   int ret;
   ret=tfsclient->create_file(uid,NULL);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_createFile_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->create_file(uid,name);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_04_createFile_wrong_filePath_1)
{
   int ret;
   ret=tfsclient->create_file(uid,"/");
   EXPECT_GT(0,ret);
   //ret=tfsclient->rm_file(uid,"/");
}

TEST_F(TfsInit,test_05_createFile_wrong_filePath_2)
{
   int ret;
   ret=tfsclient->create_file(uid,"metarcgtestcreateFile5");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_createFile_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_file(uid,"///metarcgtestcreateFile6///");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestcreateFile6");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_createFile_leap_filePath)
{
   int ret;
   ret=tfsclient->create_file(uid,"/metarcgtestcreateFile7/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_08_createFile_same_File_name)
{
   int ret;

   ret=tfsclient->create_file(uid,"/metarcgtestcreateFile8");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestcreateFile8");
   EXPECT_GT(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestcreateFile8");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_createFile_with_same_Dir)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestcreateFile9");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestcreateFile9");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestcreateFile9");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestcreateFile9");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}












