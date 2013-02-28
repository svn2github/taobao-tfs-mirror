#ifndef TFS_CLIENT_INTT_H_
#define TFS_CLIENT_INTT_H_

#include "tfs_rc_client_api.h"
#include "common/define.h"
#include "common/func.h"

#include <string>
#include <gtest/gtest.h>
#include <limits.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <tbsys.h>



#define a1G       "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/1g.jpg"
#define a100M     "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/100m.jpg"
#define a6M       "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/6m.jpg"
#define a3M       "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/3m.jpg"
#define a2M       "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/2m.jpg"
#define a100K     "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/100k.jpg"
#define a10K      "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/10k.jpg"
#define a1B       "/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/metaTestV2/resource/1b.jpg"


using namespace std;
using namespace tfs::client;
using namespace tfs::common;

static const int64_t small_len = 100*(1<<10);   //100K
static const int64_t large_len = 100*(1<<20);   //100M
static const int64_t vlarge_len = 1<<30;        //1G


class TfsInit: public testing::Test
{
  protected:

     int ret;
     RcClient* tfsclient;
     int32_t cache_time;
     int32_t cache_items;
      	
     static const int64_t appId = 1;
     static const int64_t uid = 103;

     
     virtual void SetUp()
     {

        std::string config_file_="/home/admin/hudson/workspace/TFS_C++_CLIENT_Meta_TEST/conf/test.conf";
        TBSYS_CONFIG.load(config_file_.c_str());
        cache_time = 1800;
        cache_items = 50000;
        const char* str_rc_ip = TBSYS_CONFIG.getString("public","Rc_Ip","");
        const char* app_key = TBSYS_CONFIG.getString("public","App_Key","");
       cout<<app_key<<endl;
        const char* str_app_ip = "10.232.36.206";
        
        tfsclient = new RcClient();

        ret = tfsclient->initialize(str_rc_ip, app_key, str_app_ip, cache_time, cache_items);

        if (ret != 0)
        {
           cout << "tfsclient initialize fail!" << endl;
        }
     }

     virtual void TearDown()
     {
       if (tfsclient != NULL)
       {
         delete tfsclient;
         tfsclient = NULL;
       }
     }

     virtual void data_in(const char *file_dir, char buffer[], long offset, long length)
     {
        FILE *fp = fopen(file_dir, "r+t");

        long num = 0;
        if (NULL == fp)
        {
          cout<<"open file error!"<<endl;
        }
        else
        {
          num = fread(buffer, sizeof(char), length, fp);
          if (num <= 0)
          {
            cout << "read file error!" << endl;
          }
          cout << "the all length is " << num << " !!!!!!" << endl;

          fclose(fp);
          fp = NULL;
        }
     }

	
	 virtual char chint(int i)
   {
     if (i >=0 && i <= 9)
     {
       return static_cast<char>(i+'0');
     }
     else
     {
       return 'x';
     }
	 }	

	 virtual int change(int i, char*num)
	 {
     int n1 = i/100;
     int n2 = (i%100)/10;
     int n3 = i%10;

     if (n1 != 0)
     {
		  num[0] = chint(n1);
			num[1] = chint(n2);
			num[2] = chint(n3);
			return 3;
     }
     else
     {
       if (n2 != 0)
       {
         num[0] = chint(n2);
         num[1] = chint(n3);
         return 2;
       }
       else
       {
         num[0] = chint(n3);
         return 1;
       }
     }
	}
	
	
};


#endif



