#include"tfs_client_impl_init.h"


TEST_F(TfsInit,test_01_createDir_right_filePath)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_02_createDir_leap_filePath)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/test/test");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_createDir_double_time)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_GT(0,ret);
   ret=tfsclient->rm_dir(uid,"/test");
   EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_createDir_null_filePath)
{
   int ret;
   ret=tfsclient->create_dir(uid,NULL);
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_05_createDir_empty_filePath)
{
   int ret;
   char name[1]="";
   ret=tfsclient->create_dir(uid,name);
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_06_createDir_with_same_fileName)
{
   int ret;
   ret=tfsclient->create_file(uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_file(uid,"/test");
   EXPECT_EQ(0,ret);
   ret = tfsclient->rm_dir(uid, "/test");
   EXPECT_EQ(0, ret);
}

TEST_F(TfsInit,test_07_createDir_wrong_filePath_1)
{
   int ret;
   ret=tfsclient->create_dir(uid,"test");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_08_createDir_wrong_filePath_2)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/////test//////");
   EXPECT_EQ(0,ret);
   ret=tfsclient->rm_dir(uid,"/test");
}

TEST_F(TfsInit,test_09_createDir_wrong_filePath_3)
{
   int ret = 0;
   ret = tfsclient->create_dir(uid,"/");
   EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_10_createDir_width_100)
{
   int ret;
   int i;
   char* name=new char[512];
   int p;
   char* test = "/test";
   char num[4];
   for (i = 1; i <= 100; i++)
   {
     p=change(i,num);
     num[p]='\0';
     name[0]='\0';
     strcat(name, test);
     strcat(name, num);
     ret=tfsclient->create_dir(uid,name);
     EXPECT_EQ(0,ret);
     cout<<name<<endl;
     delete [] name;
     name=NULL;
     name=new char [512];
   }
	
   for(i=1;i<=100;i++)
   {
        p=change(i,num);
        num[p]='\0';
        name[0]='\0';
        strcat(name, test);
        strcat(name, num);
	ret=tfsclient->rm_dir(uid,name);
        EXPECT_EQ(0,ret);
        cout<<name<<endl;
        delete [] name;
        name=NULL;
        name=new char [512];
   }
}


TEST_F(TfsInit,test_11_createDir_deep_50)
{
   int ret;
   int i;
   char name[512];
   char* test = "/test";
   name[0] = '\0';

   for (i = 1; i <= 50; i++)
   {	
     strcat(name, test);
     ret = tfsclient->create_dir(uid, name);
     EXPECT_EQ(0,ret);
     cout << name << endl;
   }

   for (i = 1; i <= 50; i++)
   {
     ret = tfsclient->rm_dir(uid,name);
     EXPECT_EQ(0,ret);
     cout << name << endl;
     name[250-i*5] = '\0';
   }
}
//
//TEST_F(TfsInit,test_12_createDir_width_100_deep_50)
//{
//   int ret;
//   int i;
//   char*name=new char[512];
//   int p;
//   char*test="/test"
//   char num[4];
//   for(i=1;i<=100;i++)
//   {     p=change(i,num);
//        num[p]='\0';
//        name[0]='\0';:
//        strcat(name, test);
//        strcat(name, num);
//		ret=tfsclient->create_dir(uid,name);
//        EXPECT_EQ(0,ret);
//        cout<<name_dir<<endl;
//        delete [] name_dir;
//        name_dir=NULL;
//        name_dir=new char [512];
//    }
//	
//	
//}

TEST_F(TfsInit,test_13_createDir_length_more_512)
{
   int ret;
   int i;
   char name[513];

   char temp;
   srand((unsigned)time(NULL));
   for(i=0;i<512;i++)
   {
        temp=char(33+rand()%127);
        name[i]=temp;
   }
   name[512]='\0';
   ret=tfsclient->create_dir(uid,name);
   EXPECT_GT(ret,0);
}


TEST_F(TfsInit,test_14_createDir_empty)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/    ");
   EXPECT_GT(ret,0);
}

TEST_F(TfsInit,test_15_createDir_the_same)
{
   int ret;
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_EQ(0,ret);
   ret=tfsclient->create_dir(uid,"/test");
   EXPECT_GT(0,ret);
   ret=tfsclient->rm_dir(uid,"/test");
   EXPECT_EQ(0,ret);
}

int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}





















