/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/

#include"kv_meta_test_init.h"
unsigned int int0=0;
TEST_F(TFS_Init_Data,01_get_bucket_prefix_abc)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<16;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);
  cout<<"#########"<<endl;
  Show_Object_Name(v_object_name);
  cout<<"#########"<<endl;
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,02_get_bucket_prefix_1)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "1";
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;


  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,03_get_bucket_prefix_abc_limit)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 5;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<5;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");

  std::set<std::string> e_common_prefix;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  cout<<(int)(is_truncated)<<endl;
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,04_get_bucket_start_key_abc)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abc";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<16;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,05_get_bucket_start_key_abcb)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abcb";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<8;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,06_get_bucket_start_key_1)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "1";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<17;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("ab/c");
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  std::set<std::string> e_common_prefix;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,07_get_bucket_start_key_limit)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abc";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit =10;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<10;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,08_get_bucket_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (int i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("ab/");
  e_common_prefix.insert("abc/");
  e_common_prefix.insert("abca/");
  e_common_prefix.insert("abcaa/");
  e_common_prefix.insert("abcab/");
  e_common_prefix.insert("abcac/");
  e_common_prefix.insert("abcb/");
  e_common_prefix.insert("abcc/");
  e_common_prefix.insert("abcca/");
  e_common_prefix.insert("abccb/");
  e_common_prefix.insert("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,09_get_bucket_delimiter_limit)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = '/';
  const int32_t limit = 5;


  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("ab/");
  e_common_prefix.insert("abc/");
  e_common_prefix.insert("abca/");
  e_common_prefix.insert("abcaa/");
  e_common_prefix.insert("abcab/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);  
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,10_get_bucket_prefix_abc_delimiter)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abc/");
  e_common_prefix.insert("abca/");
  e_common_prefix.insert("abcaa/");
  e_common_prefix.insert("abcab/");
  e_common_prefix.insert("abcac/");
  e_common_prefix.insert("abcb/");
  e_common_prefix.insert("abcc/");
  e_common_prefix.insert("abcca/");
  e_common_prefix.insert("abccb/");
  e_common_prefix.insert("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(e_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,11_get_bucket_prefix_abc_delimiter_1)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = '1';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<16;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,12_get_bucket_prefix_1_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "1";
  const char* start_key = NULL;
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,13_get_bucket_prefix_abc_delimiter_limit)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = '/';
  const int32_t limit = 2;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abc/");
  e_common_prefix.insert("abca/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,14_get_bucket_start_key_abcb_delimiter)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abcb";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abcb/");
  e_common_prefix.insert("abcc/");
  e_common_prefix.insert("abcca/");
  e_common_prefix.insert("abccb/");
  e_common_prefix.insert("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,15_get_bucket_start_key_abcb_delimiter_1)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abcb";
  const char delimiter = '1';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<8;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");
  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,16_get_bucket_start_key_s_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "s";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,17_get_bucket_start_key_abcb_delimiter_limit)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abcb";
  const char delimiter = '/';
  const int32_t limit = 2;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (int i=0;i<1;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abcb/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,18_get_bucket_prefix_abc_start_key_abc)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abc";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<16;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,19_get_bucket_prefix_abc_start_key_abcb)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abcb";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<8;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,20_get_bucket_prefix_abcb_start_key_abc)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abcb";
  const char* start_key = "abc";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<4;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;  
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,21_get_bucket_prefix_1_start_key_abc)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "1";
  const char* start_key = "abc";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  unsigned int int0=0;
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,22_get_bucket_prefix_abc_start_key_s)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "s";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,23_get_bucket_prefix_abc_start_key_abcb_limit)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abcb";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 5;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<5;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  unsigned int int0=0;
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,24_get_bucket_prefix_abc_start_key_abc_delimiter)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abc";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abc/");
  e_common_prefix.insert("abca/");
  e_common_prefix.insert("abcaa/");
  e_common_prefix.insert("abcab/");
  e_common_prefix.insert("abcac/");
  e_common_prefix.insert("abcb/");
  e_common_prefix.insert("abcc/");
  e_common_prefix.insert("abcca/");
  e_common_prefix.insert("abccb/");
  e_common_prefix.insert("abccc/");


  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,25_get_bucket_prefix_abc_start_key_abcb_delimiter)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abcb";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abcb/");
  e_common_prefix.insert("abcc/");
  e_common_prefix.insert("abcca/");
  e_common_prefix.insert("abccb/");
  e_common_prefix.insert("abccc/");


  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,26_get_bucket_prefix_abcb_start_key_abc_delimiter)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abcb";
  const char* start_key = "abc";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<3;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abcb/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,27_get_bucket_prefix_1_start_key_abc_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "1";
  const char* start_key = "abc";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,28_get_bucket_prefix_abc_start_key_s_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "s";
  const char delimiter = '/';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,29_get_bucket_prefix_abc_start_key_abcb_delimiter_limit)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = "abcb";
  const char delimiter = '/';
  const int32_t limit = 2;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<1;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abcba");

  std::set<std::string> e_common_prefix;
  e_common_prefix.insert("abcb/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);


  Show_Object_Name(v_object_name);
  cout<<""<<endl;
  Show_Prefix_Name(s_common_prefix);

  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(0,Is_Common_Prefix_EQ(e_common_prefix,s_common_prefix));
}

TEST_F(TFS_Init_Data,30_get_bucket_non_exist_bucket)
{
  int Ret ;
  const char* bucket_name = "WWW";
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_NE(Ret,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,31_get_bucket_null_bucket)
{
  int Ret ;
  const char* bucket_name = NULL;
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_NE(Ret,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,32_get_bucket_empty_bucket)
{
  int Ret ;
  const char* bucket_name = "";
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  EXPECT_NE(Ret,0);
  EXPECT_EQ(int0,v_object_name.size());
  EXPECT_EQ(int0,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,33_get_bucket_empty_prefix)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = "";
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  unsigned int uint17=17;
  EXPECT_EQ(uint17,v_object_name.size());
  EXPECT_EQ(uint17,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,34_get_bucket_empty_start_key)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  unsigned int uint17=17;
  EXPECT_EQ(uint17,v_object_name.size());
  EXPECT_EQ(uint17,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,35_get_bucket_delimiter)
{
  int Ret ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = NULL;
  const char delimiter = '\0';
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);
  Show_Object_Name(v_object_name);

  EXPECT_EQ(Ret,0);
  unsigned int uint17=17;
  EXPECT_EQ(uint17,v_object_name.size());
  EXPECT_EQ(uint17,v_object_meta_info.size());
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,36_get_bucket_prefix_abc_limit_EQ)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = "abc";
  const char* start_key = NULL;
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 16;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<16;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/");
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,37_get_bucket_start_key_abc_add)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abc/";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 1000;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<15;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,0);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(int0,s_common_prefix.size());
}

TEST_F(TFS_Init_Data,38_get_bucket_start_key_abc_add_limit)
{
  int Ret ;
  int i ;
  const char* bucket_name = "a1a";
  const char* prefix = NULL;
  const char* start_key = "abc/";
  const char delimiter = tfs::common::DEFAULT_CHAR;
  const int32_t limit = 15;

  tfs::common::UserInfo user_info;
  user_info.owner_id_ = 1;

  std::vector<tfs::common::ObjectMetaInfo> v_object_meta_info;
  std::vector<std::string> v_object_name;
  std::set<std::string> s_common_prefix;
  int8_t is_truncated;

  std::vector<tfs::common::ObjectMetaInfo> e_object_meta_info;
  tfs::common::ObjectMetaInfo object_meta_info;
  object_meta_info.owner_id_ = 1;
  object_meta_info.big_file_size_ = 2*(1<<20);
  for (i=0;i<15;i++)
  {
    e_object_meta_info.push_back(object_meta_info);
  }

  std::vector<std::string> e_object_name;
  e_object_name.push_back("abc/a");
  e_object_name.push_back("abc/b");
  e_object_name.push_back("abc/c");
  e_object_name.push_back("abca/");
  e_object_name.push_back("abcaa/");
  e_object_name.push_back("abcab/");
  e_object_name.push_back("abcac/");
  e_object_name.push_back("abcb/");
  e_object_name.push_back("abcba");
  e_object_name.push_back("abcbb");
  e_object_name.push_back("abcbc");
  e_object_name.push_back("abcc/");
  e_object_name.push_back("abcca/");
  e_object_name.push_back("abccb/");
  e_object_name.push_back("abccc/");

  Ret = kv_meta_client.get_bucket(bucket_name, prefix, start_key, delimiter, limit, &v_object_meta_info, &v_object_name, &s_common_prefix, &is_truncated, user_info);

  Show_Object_Name(v_object_name);
  EXPECT_EQ(Ret,0);
  EXPECT_EQ(is_truncated,1);
  EXPECT_EQ(0,Is_Object_Name_EQ(e_object_name,v_object_name));
  EXPECT_EQ(0,Is_Object_Meta_Info_EQ(e_object_meta_info,v_object_meta_info,1,2*(2<<20)));
  EXPECT_EQ(int0,s_common_prefix.size());
}


int main (int argc , char**argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
