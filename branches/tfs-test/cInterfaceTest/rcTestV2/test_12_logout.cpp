#include"tfs_init_for_test.h"


TEST_F(TfsInitTest,01_logout_right)
{
    const   char* str_rc_ip = "10.232.36.203:6100";
    const   char* app_key="tfscom";
    const   char* str_app_ip="10.13.116.135";
    int ret;
    ret = tfsclient->initialize(str_rc_ip,app_key,str_app_ip);
    EXPECT_EQ(0,ret);
    ret=tfsclient->logout();
    EXPECT_EQ(0,ret);

}

TEST_F(TfsInitTest,02_logout_twice)
{
    const   char* str_rc_ip = "10.232.36.203:6100";
    const   char* app_key="tfscom";
    const   char* str_app_ip="10.13.116.135";
    int ret;
    ret = tfsclient->initialize(str_rc_ip,app_key,str_app_ip);
    EXPECT_EQ(0,ret);
    ret=tfsclient->logout();
    EXPECT_EQ(0,ret);
   ret=tfsclient->logout();
    EXPECT_GT(ret,0);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
