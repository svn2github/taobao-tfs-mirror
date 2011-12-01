#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_open_right_read)
{
   int ret;
   ret=tfsclient->create_file(uid,"/metarcgtestopen1");
   EXPECT_EQ(0,ret);
   
   const char*name="/metarcgtestopen1";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   cout<<"!!!!!!!!!!!!  "<<appId<<endl;
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestopen1");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_open_right_write)
{
   int ret;
   ret=tfsclient->create_file(uid,"/metarcgtestopen2");
   EXPECT_EQ(0,ret);
   
   const char*name="/metarcgtestopen2";
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestopen2");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_open_NULL_name)
{  
   ret=tfsclient->open(appId, uid,NULL,tfsclient->READ);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_04_open_empty_name)
{
   int ret;
   
   const char*name="";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_05_open_not_exist)
{
   int ret;
   
   const char*name="/metarcgtestopen5";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_06_open_wrong_name_1)
{
   int ret;
   ret=tfsclient->create_file(uid,"/metarcgtestopen6");
   EXPECT_EQ(0,ret);
   
   const char*name="metarcgtestopen6";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestopen6");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_07_open_wrong_name_2)
{
   int ret;
   
   const char*name="/";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_08_open_wrong_name_3)
{
   int ret;
   ret=tfsclient->create_file(uid,"/metarcgtestopen8");
   EXPECT_EQ(0,ret);
   
   const char*name="////metarcgtestopen8////";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);
   
   ret=tfsclient->rm_file(uid,"/metarcgtestopen8");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_09_open_dir)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/metarcgtestopen9");
   EXPECT_EQ(0,ret);
   
   const char*name="/metarcgtestopen9";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(0,ret);
   
   ret=tfsclient->rm_dir(uid,"/metarcgtestopen9");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
