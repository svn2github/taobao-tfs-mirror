#ifndef TFS_CLIENT_INTT_H_
#define TFS_CLIENT_INTT_H_

#include<tfs_rc_client_api.h>
#include<string>
#include "common/define.h"
#include "common/func.h"

#include<gtest/gtest.h>
#include<limits.h>




#define a1G    "/home/chenzewei.pt/testrc/resource/1g.jpg"
#define a100M    "/home/chenzewei.pt/testrc/resource/100m.jpg"
#define a100K      "/home/chenzewei.pt/testrc/resource/100k.jpg"




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
     RcClient * tfsclient;
     int32_t       cache_time  ; 
     int32_t       cache_items ;
     
          

     virtual void SetUp()
     {
         cache_time=1800;
         
         cache_items=500000;
         
         const   char*str_rc_ip = "10.232.36.206:6261";
         const   char*app_key="foobarRcAA";
         const   char*str_app_ip="10.13.116.135";
         
         tfsclient=new RcClient();
         
         ret = tfsclient->initialize(str_rc_ip,app_key,str_app_ip,cache_time,cache_items);
         
         if(ret!=0)
         {
           cout<<"tfsclient initialize fail!"<<endl; 
           return;
         }
     }
     virtual void TearDown()
     {
         ret = tfsclient->logout();
         
         if(ret!=0)
         {
           cout<<"tfsclient destroy fail!"<<endl;
           return;
         } 
             
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
	  
  
};


#endif


































