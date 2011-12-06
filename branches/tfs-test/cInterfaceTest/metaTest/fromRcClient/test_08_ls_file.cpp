#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_lsFile_right_filePath)
{
   int ret;

   ret=tfsclient->create_file(uid,"/metarcgtestlsFile1");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/metarcgtestlsFile1",file_meta_info);
   EXPECT_EQ(0,file_meta_info.size_);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestlsFile1");
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
   ret=tfsclient->ls_file(appId,uid,NULL,file_meta_info);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_lsFile_wrong_filePath_1)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/",file_meta_info);
   EXPECT_LT(0,ret);
}

TEST_F(TfsInit,test_05_lsFile_wrong_filePath_2)
{
   int ret;

   ret=tfsclient->create_file(uid,"/metarcgtestlsFile5");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"metarcgtestlsFile5",file_meta_info);
   EXPECT_GT(0,ret);
  
 ret=tfsclient->rm_file(uid,"/metarcgtestlsFile5");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_lsFile_wrong_filePath_3)
{
   int ret;

   ret=tfsclient->create_file(uid,"/metarcgtestlsFile6");
   EXPECT_EQ(0,ret);
   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"////metarcgtestlsFile6///",file_meta_info);
   EXPECT_EQ(0,file_meta_info.size_);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestlsFile6");
   EXPECT_EQ(ret,0);
}

TEST_F(TfsInit,test_07_lsFile_not_exist_filePath)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->ls_file(appId,uid,"/metarcgtestlsFile7",file_meta_info);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_08_lsFile_with_dir)
{
   int ret;

   FileMetaInfo file_meta_info;
   ret=tfsclient->create_dir(uid,"/metarcgtestlsFile8dir");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_file(uid,"/metarcgtestlsFile8dir/metarcgtestfile");
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->ls_file(appId,uid,"/metarcgtestlsFile8dir/metarcgtestfile",file_meta_info);
   EXPECT_EQ(0,file_meta_info.size_);
   EXPECT_EQ(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestlsFile8dir/metarcgtestfile");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/metarcgtestlsFile8dir");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
