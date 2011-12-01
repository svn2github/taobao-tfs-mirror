#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_mvDir_right)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir1src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir1src","/metarcgtestmvDir1tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir1tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir1src");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_02_mvDir_srcFilePath_with_File)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir2src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir2src/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir2src","/metarcgtestmvDir2tar");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir2tar/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir2tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir2src");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_mvDir_srcFilePath_with_File_and_Dir)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir3src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir3src/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir3src/testdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir3src","/metarcgtestmvDir3tar");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir3tar/testdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir3tar/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir3tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir3src");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_mvDir_exit_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir4src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir4tar");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir4src","/metarcgtestmvDir4tar");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir4tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir4src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_mvDir_empty_srcFilePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->mv_dir(uid,name,"/metarcgtestmvDir5tar");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_mvDir_null_srcFilePath)
{
   int ret;
   ret=tfsclient->mv_dir(uid,NULL,"/metarcgtestmvDir6tar");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_07_mvDir_wrong_srcFilePath_1)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir7src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"metarcgtestmvDir7src","/metarcgtestmvDir7tar");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir7src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir7tar");
   EXPECT_GT(0,ret);   
}

TEST_F(TfsInit,test_08_mvDir_wrong_srcFilePath_2)
{
   int ret;

   ret=tfsclient->mv_dir(uid,"/","/metarcgtestmvDir8tar");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_09_mvDir_empty_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir9src");
   EXPECT_EQ(0,ret);
   char name[1]="";
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir9src",name);
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir9src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_10_mvDir_null_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir10src");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir10src",NULL);
   EXPECT_GT(0,ret);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir10src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_11_mvDir_wrong_destFilePath_1)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir11src");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir11src","/");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir11src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_12_mvDir_wrong_destFilePath_2)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir12src");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir12src","metarcgtestmvDir12tar");
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir12src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_13_mvDir_wrong_destFilePath_3)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir13src");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir13src", "///metarcgtestmvDir13tar///");
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir13tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir13src");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_14_mvDir_with_leap_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir14src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir14src", "/metarcgtestmvDir14tar/text");
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir14src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_15_mvDir_with_same_File_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir15src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir15tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir15src","/metarcgtestmvDir15tar");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir15src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir15tar");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_16_mvDir_srcFilePath_same_destFilePath)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir16src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir16src","/metarcgtestmvDir16src");
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir16src");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_17_mvDir_complex)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir17src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir17src/testdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir17src/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir17src/testdir/testd");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir17src/testdir/testf");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir17tar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir17tar/testdirt");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestmvDir17tar/testfilet");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir17src/testdir","/metarcgtestmvDir17tar/testdirtar");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir17src/testfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir17src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir17tar/testdirtar/testf");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir17tar/testdirtar/testd");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir17tar/testdirtar");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir17tar/testdirt");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/metarcgtestmvDir17tar/testfilet");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir17tar");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_19_mvDir_circle)
{
   int ret;

   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir18src");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir18src/metarcgtestdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/metarcgtestmvDir18src/metarcgtestdir/testdir");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->mv_dir(uid,"/metarcgtestmvDir18src","/metarcgtestmvDir18src/metarcgtestdir/testdir");
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir18src/metarcgtestdir/testdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir18src/metarcgtestdir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestmvDir18src");
   EXPECT_EQ(0,ret);
}
int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
