#ifndef KV_META_TEST_INIT_H_
#define KV_META_TEST_INIT_H_

#include "tfs_rc_client_api.h"
#include "tfs_client_api.h"
#include "kv_meta_define.h"
#include "func.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <string>
#include <vector.h>
#include <set.h>
#include <sys/stat.h>
#include "tfs_rc_client_api_impl.h"

using namespace std;
using namespace tfs::client;
class TMP
{
  public:
    TMP()
    {
      TfsClient* tt = tfs::client::TfsClient::Instance();
      tt->get_cache_items();
    }
};
static TMP tmp; //this is make tfsclient init first
static RcClient kv_meta_client ;
//static RcClientImpl kv_meta_client ;

#define RC_2M    "/home/yiming.czw/kv_meta_test/resouce/2m"
#define RC_100M  "/home/yiming.czw/kv_meta_test/resouce/100m"
#define RC_5M    "/home/yiming.czw/kv_meta_test/resouce/5m"
#define RC_2K    "/home/yiming.czw/kv_meta_test/resouce/2k"
#define RC_00    "/home/yiming.czw/kv_meta_test/resouce/empty"
#define RC_20M   "/home/yiming.czw/kv_meta_test/resouce/20m"

class TFS_Init : public testing::Test
{
  protected:

    static void SetUpTestCase()
    {
      const char*kms_addr="10.232.36.210:7201";
      //const char*kms_addr="10.232.35.41:5977";
      const char*rc_addr="10.232.36.202:9202";
      //const char*rc_addr="10.232.36.200:5755";
      //const char*app_key="tfsNginxA01";
      const char*app_key="Test_11";
      int Ret ;

      //		kv_meta_client = new KvMetaClient();
      //		Ret = kv_meta_client->initialize(kms_addr,ns_addr);


      kv_meta_client.set_kv_rs_addr(kms_addr);
      Ret = kv_meta_client.initialize(rc_addr,app_key);

      if(Ret<0)
        cout<<"Client initalize fail!"<<endl;
    }

    static void TearDownTestCase()
    {
      //	if(NULL != kv_meta_client)
      //  {
      //    delete kv_meta_client;
      //    kv_meta_client=NULL;
      //  }
    }

};

class TFS_Init_Data : public testing::Test
{
  protected:
    //		static KvMetaClient* kv_meta_client ;

    static void SetUpTestCase()
    {
      //const char*kms_addr="10.232.35.41:5977";
      const char*kms_addr="10.232.36.210:7201";
      const char*rc_addr="10.232.36.202:9202";
      const char*app_key="Test_11";

      tfs::common::UserInfo user_info;
      user_info.owner_id_=1;
      const char*bucket_name="AAA";
      const char* local_file=RC_2M;

      int Ret ;

      //kv_meta_client = new KvMetaClient();
      kv_meta_client.set_kv_rs_addr(kms_addr);
      Ret = kv_meta_client.initialize(rc_addr,app_key);
      if(Ret<0)
        cout<<"Client initalize fail!"<<endl;

      kv_meta_client.put_bucket(bucket_name,user_info);

      Ret = kv_meta_client.put_object(bucket_name,"abc/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 1 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"ab/c",local_file,user_info);
      if(Ret<0) cout<<"put_objct 2 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/a",local_file,user_info);
      if(Ret<0) cout<<"put_objct 3 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/b",local_file,user_info);
      if(Ret<0) cout<<"put_objct 4 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/c",local_file,user_info);
      if(Ret<0) cout<<"put_objct 5 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abca/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 6 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcb/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 7 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcc/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 8 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcaa/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 9 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcab/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 10 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcac/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 11 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcba",local_file,user_info);
      if(Ret<0) cout<<"put_objct 12 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcbb",local_file,user_info);
      if(Ret<0) cout<<"put_objct 13 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcbc",local_file,user_info);
      if(Ret<0) cout<<"put_objct 14 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcca/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 15 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abccb/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 16 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abccc/",local_file,user_info);
      if(Ret<0) cout<<"put_objct 17 fail !"<<endl;
    }

    static void TearDownTestCase()
    {
      const char*bucket_name="AAA";
      tfs::common::UserInfo user_info;
      user_info.owner_id_=1;

      kv_meta_client.del_object(bucket_name,"abc/",user_info);
      kv_meta_client.del_object(bucket_name,"ab/c",user_info);
      kv_meta_client.del_object(bucket_name,"abc/a",user_info);
      kv_meta_client.del_object(bucket_name,"abc/b",user_info);
      kv_meta_client.del_object(bucket_name,"abc/c",user_info);
      kv_meta_client.del_object(bucket_name,"abca/",user_info);
      kv_meta_client.del_object(bucket_name,"abcb/",user_info);
      kv_meta_client.del_object(bucket_name,"abcc/",user_info);
      kv_meta_client.del_object(bucket_name,"abcaa/",user_info);
      kv_meta_client.del_object(bucket_name,"abcab/",user_info);
      kv_meta_client.del_object(bucket_name,"abcac/",user_info);
      kv_meta_client.del_object(bucket_name,"abcba",user_info);
      kv_meta_client.del_object(bucket_name,"abcbb",user_info);
      kv_meta_client.del_object(bucket_name,"abcbc",user_info);
      kv_meta_client.del_object(bucket_name,"abcca/",user_info);
      kv_meta_client.del_object(bucket_name,"abccb/",user_info);
      kv_meta_client.del_object(bucket_name,"abccc/",user_info);

      kv_meta_client.del_bucket(bucket_name,user_info);

      //if(NULL != kv_meta_client)
      //{	
      //  delete kv_meta_client;
      //  kv_meta_client = NULL;
      //}
    }
};

class TFS_Without_Init : public testing::Test
{

};

int ReadData(const char*file, void*buf, long int length, long int offset)
{
  int Ret = -1;
  size_t Len;
  FILE*fp;
  if( (fp = fopen(file,"rb")) != NULL)
  {
    fseek(fp,offset,SEEK_SET);
    Len = fread(buf,length,1,fp);
    cout<<"The read length is :"<<Len<<endl;
    if(Len!=1)
      cout<<"Read data fail!"<<endl;
    else
      Ret = 0;
  }
  return Ret;
}

int Is_Object_Name_EQ(std::vector<std::string>Expect, std::vector<std::string>Actual)
{
  int Ret = -1;
  std::vector<std::string>::iterator Eit = Expect.begin();
  std::vector<std::string>::iterator Ait = Actual.begin();

  cout<<"OB Expect.size:"<<Expect.size()<<endl;
  cout<<"OB Actual.size:"<<Actual.size()<<endl;
  if(Expect.size()==Actual.size())
  {
    for(;Eit!=Expect.end();Eit++,Ait++)
    {
      if(*Eit != *Ait)
        return Ret;
    }
    Ret = 0;
  }
  return Ret;
}

void Show_Object_Name(std::vector<std::string>object_name)
{
  std::vector<std::string>::iterator it = object_name.begin();

  cout<<object_name.size()<<endl;
  for(;it!=object_name.end();it++)
  {
    cout<<*it<<endl;
  }
}

int Is_Common_Prefix_EQ(std::set<std::string>Expect, std::set<std::string>Actual)
{
  int Ret = -1;
  std::set<std::string>::iterator Eit = Expect.begin();
  std::set<std::string>::iterator Ait = Actual.begin();

  cout<<"Co Expect.size:"<<Expect.size()<<endl;
  cout<<"Co Actual.size:"<<Actual.size()<<endl;
  if(Expect.size()==Actual.size())
  {
    for(;Eit!=Expect.end();Eit++,Ait++)
    {
      if(*Eit != *Ait)
      {
        cout<<*Eit<<endl;
        cout<<*Ait<<endl;
        return Ret;
      }
    }
    Ret = 0;
  }
  return Ret;
}

void Show_Prefix_Name(std::set<std::string>prefix_name)
{
  std::set<std::string>::iterator it = prefix_name.begin();

  for(;it!=prefix_name.end();it++)
  {
    cout<<*it<<endl;
  }
}

int Is_Object_Meta_Info_EQ(std::vector<tfs::common::ObjectMetaInfo>Expect, std::vector<tfs::common::ObjectMetaInfo>Actual, int64_t owner_id_, int64_t big_file_size_)
{
  int Ret = -1;
  std::vector<tfs::common::ObjectMetaInfo>::iterator Eit = Expect.begin();
  std::vector<tfs::common::ObjectMetaInfo>::iterator Ait = Actual.begin();
  cout<<Actual.size()<<endl;

  if(Expect.size()==Actual.size())
  {
    for(;Eit!=Expect.end();Eit++,Ait++)
    {
      if(Eit->owner_id_!=Ait->owner_id_||Eit->big_file_size_!=Ait->big_file_size_)
      {
        cout<<Eit->owner_id_<<"  "<<Ait->owner_id_<<endl;
        cout<<Eit->big_file_size_<<"  "<<Ait->big_file_size_<<endl;
        return Ret;
      }
    }
    Ret = 0;
  }
  return Ret;
}

uint32_t Get_crc(const char*local_file , int32_t len , int32_t offset = 0)
{
  char*buf=new char[len];
  if(ReadData(local_file,buf,len,offset)!=0)
  {
    cout<<"Read data form local file fail!"<<endl;
    return -1;
  }
  return tfs::common::Func::crc(0,buf,len);
}

bool Is_File_Exist(const char* file , int & File_length)
{
  bool Ret = false ;
  struct stat buf;
  if(stat(file,&buf)==0)
  {
    File_length = buf.st_size;
    return true;
  }
  return Ret ;
}
#endif
