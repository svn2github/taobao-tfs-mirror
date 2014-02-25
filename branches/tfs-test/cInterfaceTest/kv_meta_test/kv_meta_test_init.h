#ifndef KV_META_TEST_INIT_H_
#define KV_META_TEST_INIT_H_
#include <tfs_rc_client_api.h>
#include <kv_meta_define.h>
#include "func.h"
#include <gtest/gtest.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <set>
#include <sys/stat.h>
#include "tfs_rc_client_api_impl.h"

using namespace std;
using namespace tfs::clientv2;
class TMP
{
  public:
    TMP()
    {
     // TfsClient* tt = tfs::clientv2::TfsClient::Instance();
     // tt->get_cache_items();
    }
};
static TMP tmp; //this is make tfsclient init first
static RcClient kv_meta_client ;
//static RcClientImpl kv_meta_client ;

#define RC_2M    "../../resource/2m.jpg"
#define RC_100M  "../../resource/100m.jpg"
#define RC_5M    "../../resource/5m.jpg"
#define RC_2K    "../../resource/2k.jpg"
#define RC_00    "../../resource/empty.jpg"
#define RC_20M   "../../resource/20m.jpg"

class TFS_Init : public testing::Test
{
  protected:

    static void SetUpTestCase()
    {
      const char*kv_root_server_addr="10.232.4.12:4567";
      // const char*kv_root_server_addr="10.232.35.40:5977";
      const char*rc_addr="10.232.4.7:6267";
      //const char*rc_addr="10.232.36.200:5755";
      const char*app_key="tappkey";
      int Ret ;

      //		kv_meta_client = new KvMetaClient();
      //		Ret = kv_meta_client->initialize(kv_root_server_addr,ns_addr);


      kv_meta_client.set_kv_rs_addr(kv_root_server_addr);
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
      const char*kv_root_server_addr="10.232.4.12:4567";
      const char*rc_addr="10.232.4.7:6267";
      const char*app_key="tappkey";

       tfs::common::UserInfo user_info;
       user_info.owner_id_=1;
       tfs::common::CustomizeInfo customize_info;
      
      const char*bucket_name="a1a";
      const char* local_file=RC_2M;

      int Ret ;

      //kv_meta_client = new KvMetaClient();
      kv_meta_client.set_kv_rs_addr(kv_root_server_addr);
      Ret = kv_meta_client.initialize(rc_addr,app_key);
      if(Ret<0)
        cout<<"Client initalize fail!"<<endl;

      Ret =kv_meta_client.put_bucket(bucket_name,user_info);
      EXPECT_EQ(Ret,0);

      Ret = kv_meta_client.put_object(bucket_name,"abc/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 1 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"ab/c",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 2 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/a",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 3 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/b",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 4 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abc/c",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 5 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abca/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 6 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcb/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 7 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcc/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 8 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcaa/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 9 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcab/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 10 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcac/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 11 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcba",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 12 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcbb",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 13 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcbc",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 14 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abcca/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 15 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abccb/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 16 fail !"<<endl;
      Ret = kv_meta_client.put_object(bucket_name,"abccc/",local_file,user_info,customize_info);
      if(Ret<0) cout<<"put_objct 17 fail !"<<endl;
    }

    static void TearDownTestCase()
    {
       const char*bucket_name="a1a";
       tfs::common::UserInfo user_info;
       user_info.owner_id_=1;
       tfs::common::CustomizeInfo customize_info;


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
