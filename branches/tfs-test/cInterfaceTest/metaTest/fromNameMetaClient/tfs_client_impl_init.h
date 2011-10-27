#ifndef TFS_CLIENT_INTT_H_
#define TFS_CLIENT_INTT_H_

#include<tfs_meta_client_api.h>
#include<string>
#include "common/define.h"
#include "common/func.h"

#include<gtest/gtest.h>
#include<limits.h>
#include <time.h>
#include <stdio.h>
#include <string.h>




#define a1G       "/home/chenzewei.pt/testmeta/resource/1g.jpg"
#define a6M       "/home/chenzewei.pt/testmeta/resource/6m.jpg"
#define a3M       "/home/chenzewei.pt/testmeta/resource/3m.jpg"
#define a2M       "/home/chenzewei.pt/testmeta/resource/2m.jpg"
#define a100M     "/home/chenzewei.pt/testmeta/resource/100m.jpg"
#define a10K      "/home/chenzewei.pt/testmeta/resource/10k.jpg"
#define a100K     "/home/chenzewei.pt/testmeta/resource/100k.jpg"
#define a1B       "/home/chenzewei.pt/testmeta/resource/1b.jpg"


using namespace std;
using namespace tfs::client;
using namespace tfs::common;

static const int64_t small_len=100*(1<<10);
static const int64_t large_len=100*(1<<20);
static const int64_t vlarge_len=1<<30;


class TfsInit: public testing::Test
{
     
     protected:
     int ret;
     NameMetaClient* tfsclient;
     public:
     static const int64_t appId = 6060 ;
     static const int64_t uid = 8080 ; 

     virtual void SetUp()
     {
         
         const   char*root_server_str = "10.232.36.206:8765";
         int Ret;
         
         tfsclient=new NameMetaClient();
         
         Ret=tfsclient->initialize(root_server_str);
         if(Ret!=0)
         cout<<"initialize fail!!!!"<<endl;
       
     }
     virtual void TearDown()
     {
             
     }
     
     virtual void data_in(const char *file_dir,char buffer[],long offset,long length)
     {
        FILE *fp=fopen(file_dir,"r+t");
        
        long num=0;
        if(fp==NULL)
        {   
            cout<<"open file error!"<<endl;
            return;
        } 
          
        num=fread(buffer,sizeof(char),length,fp);        
        if(num<=0)
        {
            cout<<"read file error!"<<endl;
        }
        cout<<"the all length is "<<num<<" !!!!!!"<<endl;

        fclose(fp);
     }
     
	 
	 virtual char chint(int i)
     {
	    switch (i)
		{
		    case 0 : return '0';break;
			case 1 : return '1';break;
			case 2 : return '2';break;
			case 3 : return '3';break;
			case 4 : return '4';break;
			case 5 : return '5';break;
			case 6 : return '6';break;
			case 7 : return '7';break;
			case 8 : return '8';break;
			case 9 : return '9';break;
		}
	 }	 
       
	 virtual int change(int i,char*num)
	 {
	          
		  
		  int n1=i/100;
		  int n2=(i%100)/10;
		  int n3=i%10;
		  if(n1!=0)
		  {
		     num[0]=chint(n1);
			 num[1]=chint(n2);
			 num[2]=chint(n3);
			 return 3;
		  }
		  else
		  {
		     if(n2!=0)
			 {
			    num[0]=chint(n2);
				num[1]=chint(n3);
				return 2;
			 }
			 
			 else
			 {
			    num[0]=chint(n3);
                            return 1;
			 }
		  }
	 }
	 
	 
};


#endif



