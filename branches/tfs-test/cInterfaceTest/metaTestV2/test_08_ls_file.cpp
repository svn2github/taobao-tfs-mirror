#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_lsFile_right_filePath)
{
   int ret;

   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/test",file_meta_info);
   EXPECT_EQ(0,file_meta_info.size_);
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_lsFile_null_filePath)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,NULL,file_meta_info);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_lsFile_empty_filePath)
{
   int ret;
   FileMetaInfo file_meta_info;
   char name[1]="";
   ret=tfsclient->ls_file(appId, uid, name, file_meta_info);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_lsFile_wrong_filePath_1)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/",file_meta_info);
   EXPECT_EQ(1,ret);
}

TEST_F(TfsInit,test_05_lsFile_wrong_filePath_2)
{
   int ret;

   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"test",file_meta_info);
   EXPECT_GT(0,ret);

 ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_lsFile_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"////test///",file_meta_info);
   EXPECT_EQ(0,file_meta_info.size_);
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(ret,0);
}

TEST_F(TfsInit,test_07_lsFile_not_exist_filePath)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/test",file_meta_info);
   EXPECT_GT(0,ret);
}










int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
