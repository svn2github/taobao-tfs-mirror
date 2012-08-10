#include"tfs_client_impl_init.h"


TEST_F(TfsInit,01_ls_dir_right)
{
    int ret;
    vector<FileMetaInfo> v_file_meta_info;
    char num1[4];
    char num2[4];
    char num3[4];
    char*name_dir=new char [512];
    char*name_file=new char[512];
    char*test="/test";
    char*file="/file";

    int i,j,k;
    int n=0;
    int p;

    for(i=1;i<=5;i++)
    {
        p=change(i,num1);
        num1[p]='\0';
        name_dir[0]='\0';
        strcat(name_dir, test);
        strcat(name_dir, num1);
        if(tfsclient->create_dir(uid,name_dir)<0)
        {
              cout<<name_dir<<endl;
        }
        delete [] name_dir;
        name_dir=NULL;
        name_dir=new char [512];


        name_file[0]='\0';
        strcat(name_file, file);
        strcat(name_file, num1);
        if(ret=tfsclient->create_file(uid,name_file)<0)
        {
             cout<<name_file<<"    "<<ret<<endl;
        }
        delete [] name_file;
        name_file=NULL;
        name_file=new char [512];

    }

        for(i=1;i<=5;i++)
        for(j=1;j<=5;j++)
        {
            p=change(i,num1);
                change(j,num2);
                num1[p]='\0';
                num2[p]='\0';
                name_dir[0]='\0';
                strcat(name_dir, test);
                strcat(name_dir, num1);
                strcat(name_dir, test);
                strcat(name_dir, num2);
                if(tfsclient->create_dir(uid,name_dir)<0)
                {
                  cout<<name_dir<<endl;
                }
                delete [] name_dir;
                name_dir=NULL;
                name_dir=new char [512];

                name_file[0]='\0';
                strcat(name_file, test);
                strcat(name_file, num1);
                strcat(name_file, file);
                strcat(name_file, num2);
                if(tfsclient->create_file(uid,name_file)<0)
               {
                cout<<name_file<<endl;
               }
                delete [] name_file;
                name_file=NULL;
                name_file=new char [512];
                ++n;
        }

        for(i=1;i<=5;i++)
        for(j=1;j<=5;j++)
        for(k=1;k<=5;k++)
        {
                p=change(i,num1);
                change(j,num2);
                change(k,num3);
                num1[p]='\0';
                num2[p]='\0';
                num3[p]='\0';
                name_dir[0]='\0';

                strcat(name_dir, test);
                strcat(name_dir, num1);
                strcat(name_dir, test);
                strcat(name_dir, num2);
                strcat(name_dir, test);
                strcat(name_dir, num3);
                if(tfsclient->create_dir(uid,name_dir)<0)
                {
                   cout<<name_dir<<endl;
                }
                delete [] name_dir;
                name_dir=NULL;
                name_dir=new char [512];

                name_file[0]='\0';
                strcat(name_file, test);
                strcat(name_file, num1);
                strcat(name_file, test);
                strcat(name_file, num2);
                strcat(name_file, file);
                strcat(name_file, num3);
                if(tfsclient->create_file(uid,name_file)<0)
                {
                 cout<<name_file<<endl;
                }
                delete [] name_file;
                name_file=NULL;
                name_file=new char [512];
                ++n;
        }
		
    ret=tfsclient->ls_dir(appId, uid,"/test1", v_file_meta_info);
    EXPECT_EQ(0,ret);
    EXPECT_EQ(10,v_file_meta_info.size());
	
	    for(i=1;i<=5;i++)
        for(j=1;j<=5;j++)
        for(k=1;k<=5;k++)
        {
                p=change(i,num1);
                change(j,num2);
                change(k,num3);
                num1[p]='\0';
                num2[p]='\0';
                num3[p]='\0';
                name_dir[0]='\0';

                strcat(name_dir, test);
                strcat(name_dir, num1);
                strcat(name_dir, test);
                strcat(name_dir, num2);
                strcat(name_dir, test);
                strcat(name_dir, num3);
                if(tfsclient->rm_dir(uid,name_dir)<0)
                {
                   cout<<name_dir<<endl;
                }
                delete [] name_dir;
                name_dir=NULL;
                name_dir=new char [512];

                name_file[0]='\0';
                strcat(name_file, test);
                strcat(name_file, num1);
                strcat(name_file, test);
                strcat(name_file, num2);
                strcat(name_file, file);
                strcat(name_file, num3);
                if(tfsclient->rm_file(uid,name_file)<0)
                {
                 cout<<name_file<<endl;
                }
                delete [] name_file;
                name_file=NULL;
                name_file=new char [512];
                ++n;
        }
		
        for(i=1;i<=5;i++)
        for(j=1;j<=5;j++)
        {
                p=change(i,num1);
                change(j,num2);
                num1[p]='\0';
                num2[p]='\0';
                name_dir[0]='\0';
                strcat(name_dir, test);
                strcat(name_dir, num1);
                strcat(name_dir, test);
                strcat(name_dir, num2);
                if(tfsclient->rm_dir(uid,name_dir)<0)
                {
                  cout<<name_dir<<endl;
                }
                delete [] name_dir;
                name_dir=NULL;
                name_dir=new char [512];

                name_file[0]='\0';
                strcat(name_file, test);
                strcat(name_file, num1);
                strcat(name_file, file);
                strcat(name_file, num2);
                if(tfsclient->rm_file(uid,name_file)<0)
               {
                cout<<name_file<<endl;
               }
                delete [] name_file;
                name_file=NULL;
                name_file=new char [512];
                ++n;
        }	

	for(i=1;i<=5;i++)
    {
        p=change(i,num1);
        num1[p]='\0';
        name_dir[0]='\0';
        strcat(name_dir, test);
        strcat(name_dir, num1);
        if(tfsclient->rm_dir(uid,name_dir)<0)
        {
              cout<<name_dir<<endl;
        }
        delete [] name_dir;
        name_dir=NULL;
        name_dir=new char [512];


        name_file[0]='\0';
        strcat(name_file, file);
        strcat(name_file, num1);
        if(ret=tfsclient->rm_file(uid,name_file)<0)
        {
             cout<<name_file<<"    "<<ret<<endl;
        }
        delete [] name_file;
        name_file=NULL;
        name_file=new char [512];

    }		
}
TEST_F(TfsInit,test_01_lsDir_null_filePath)
{
    int ret;
    vector<FileMetaInfo> v_file_meta_info;
    ret=tfsclient->ls_dir(appId,uid,NULL,v_file_meta_info);
    EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_02_lsDir_empty_filePath)
{
    int ret;
    vector<FileMetaInfo> v_file_meta_info;
    char name[1]="";	
    ret=tfsclient->ls_dir(appId,uid,name,v_file_meta_info);
    EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_03_lsDir_wrong_filePath_1)
{
    int ret;
	
    ret=tfsclient->create_dir(uid,"/test");
    EXPECT_EQ(0,ret);

    vector<FileMetaInfo> v_file_meta_info;
   	
    ret=tfsclient->ls_dir(appId,uid,"test",v_file_meta_info);
    EXPECT_GT(0,ret);

    ret=tfsclient->rm_dir(uid,"/test");
    EXPECT_EQ(0,ret);
}

TEST_F(TfsInit,test_04_lsDir_wrong_filePath_2)
{
    int ret;
	
    ret=tfsclient->create_dir(uid,"/test");
    EXPECT_EQ(0,ret);

    vector<FileMetaInfo> v_file_meta_info;
   	
    ret=tfsclient->ls_dir(appId,uid,"///test////",v_file_meta_info);
    EXPECT_EQ(0,ret);
    EXPECT_EQ(0,v_file_meta_info.size());

    ret=tfsclient->rm_dir(uid,"/test");
    EXPECT_EQ(0,ret);
}


TEST_F(TfsInit,test_05_lsDir_not_exist_filePath)
{
    int ret;

    vector<FileMetaInfo> v_file_meta_info;
   	
    ret=tfsclient->ls_dir(appId,uid,"/test",v_file_meta_info);
    EXPECT_GT(0,ret);
}

TEST_F(TfsInit,test_06_lsDir_file)
{
    int ret;
    ret=tfsclient->create_file(uid,"/test");
    EXPECT_EQ(0,ret);

    vector<FileMetaInfo> v_file_meta_info;
   	
    ret=tfsclient->ls_dir(appId,uid,"/test",v_file_meta_info);
    EXPECT_GT(0,ret);

    ret=tfsclient->rm_file(uid,"/test");
    EXPECT_EQ(0,ret);
}



int main(int argc,char**argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
