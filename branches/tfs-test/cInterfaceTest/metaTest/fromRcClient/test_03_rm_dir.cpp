#include"tfs_client_impl_init.h"

TEST_F(TfsInit,test_01_rmDir_right_filePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir1");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_rmDir_double_times)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir2");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir2");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_rmDir_not_exist_filePath)
{
   int ret;
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir3");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_rmDir_null_filePath)
{
   int ret;
   ret=tfsclient->rm_dir(uid,NULL);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_05_rmDir_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->rm_dir(uid,name);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_rmDir_wrong_filePath_1)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir6");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"metarcgtestrmDir6");
   EXPECT_GT(ret,0);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir6");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_rmDir_wrong_filePath_2)
{
   int ret;
   ret=tfsclient->rm_dir(uid,"/");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_08_rmDir_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir8");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"///metarcgtestrmDir8////");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_rmDir_filePath_with_File)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir9");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestrmDir9/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir9");
   EXPECT_GT(0,ret);
   tfsclient->rm_file(uid,"/metarcgtestrmDir9/testfile");
   tfsclient->rm_dir(uid,"/metarcgtestrmDir9");
}

TEST_F(TfsInit,test_10_rmDir_filePath_with_Dir)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir10");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestrmDir10/testdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestrmDir10");
   EXPECT_GT(0,ret);
   tfsclient->rm_dir(uid,"/metarcgtestrmDir10/testdir");
   tfsclient->rm_dir(uid,"/metarcgtestrmDir10");
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}



































