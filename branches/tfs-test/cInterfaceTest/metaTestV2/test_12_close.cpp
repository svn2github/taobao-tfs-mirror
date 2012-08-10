#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_close_open_right_read)
{
   int ret;
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);

   const char*name="/test";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);

   int fd=ret;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_close_open_right_write)
{
   int ret;
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);

   const char*name="/test";
   ret=tfsclient->open(appId, uid,name,tfsclient->WRITE);
   EXPECT_GT(ret,0);

   int fd=ret;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);

   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_03_rm_and_close)
{
   int ret;
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);

   const char*name="/test";
   ret=tfsclient->open(appId, uid,name,tfsclient->READ);
   EXPECT_GT(ret,0);

   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);

   int fd=ret;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_close_wrong_fd_1)
{
   int ret;

   int fd=-1;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_05_close_wrong_fd_2)
{
   int ret;

   int fd=0;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_06_close_not_exist_fd)
{
   int ret;

   int fd=1;
   ret=tfsclient->close(fd);
   EXPECT_EQ(0,ret);
}


int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
