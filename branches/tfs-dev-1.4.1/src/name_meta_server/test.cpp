/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "mysql_database_helper.h"
using namespace tfs;
using namespace tfs::namemetaserver;
void dump_meta_info(const MetaInfo& metainfo)
{
  int size = metainfo.size_;
  int nlen = metainfo.name_.length();

  TBSYS_LOG(INFO, "size = %d, name_len = %d", size, nlen);
}

int main()
{
  MysqlDatabaseHelper tt;
  tt.set_conn_param("10.232.35.41:3306:tfs_name_db","root","root");
  int64_t mysql_proc_ret = -1;
  char pname[512];
  char name[512];
  int ret;
  ret = tt.get_nex_val(mysql_proc_ret);
  printf("ret = %d get_nex_val = %ld-----------\n", ret, mysql_proc_ret);

  ret = tt.get_nex_val(mysql_proc_ret);
  printf("ret = %d get_nex_val = %ld-----------\n", ret, mysql_proc_ret);

  std::vector<MetaInfo> out_v_meta_info;
  ret = tt.ls_meta_info(out_v_meta_info, 2, 2);
  printf("ls_meta_info ret = %d size = %lu\n", ret , out_v_meta_info.size());
  for (size_t i = 0; i < out_v_meta_info.size(); i++)
  {
    dump_meta_info(out_v_meta_info[i]);
  }

  sprintf(pname,"%c",0);
  sprintf(name, "%cR", 1);

  ret = tt.create_dir(2, 2, 0, pname, 1, 0, 30, name, 2, mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld-----------\n", ret, mysql_proc_ret);

  sprintf(pname, "%cR", 1);
  sprintf(name,"%c%s",4,"1234");

  printf("will create dir 4_1234\n");
  ret = tt.create_dir(2, 2, 0, pname, 2, 30, 31, name, 5, mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);
  printf("will rm dir 4_1234 input\n");
  //scanf("%d", &mysql_proc_ret);

  ret = tt.rm_dir(2, 2, 0, pname, 2, 30, 31, name, 5, mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will create 4_1234 dir\n");
  //scanf("%d", &mysql_proc_ret);
  ret = tt.create_dir(2, 2, 0, pname, 2, 30, 31, name, 5, mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will create 4_1234->3_123 dir\n");
  sprintf(pname, "%c%s", 4,"1234");
  sprintf(name,"%c%s",3,"123");
  ret = tt.create_dir(2, 2, 30, pname, 5, 31, 32, name, 4, mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  char s_pname[256];
  char d_pname[256];
  char s_name[256];
  char d_name[256];
  sprintf(s_pname, "%c%s", 4, "1234");
  sprintf(d_pname, "%cR", 1);
  sprintf(s_name, "%c%s", 3, "123");
  sprintf(d_name, "%c%s", 2, "12");
  printf("will mv dir 4_1234->3_123  R->2_12\n");
  //scanf("%d", &mysql_proc_ret);
  ret = tt.mv_dir(2, 2,
      30, 31, s_pname, 5,
      0, 30, d_pname, 2,
      s_name, 4, d_name, 3,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will create file 4_1234->4_file \n");
  sprintf(pname,"%c%s", 4, "1234");
  sprintf(name, "%c%s", 4, "file");
  ret = tt.create_file(2, 2,
      30, 31, pname, 5,
      name, 5,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  ret = tt.ls_meta_info(out_v_meta_info, 2, 2);
  printf("ls_meta_info ret = %d size = %lu\n", ret , out_v_meta_info.size());
  for (size_t i = 0; i < out_v_meta_info.size(); i++)
  {
    dump_meta_info(out_v_meta_info[i]);
  }
  ret = tt.ls_meta_info(out_v_meta_info, 2, 2);
  printf("ls_meta_info ret = %d \n", ret);
  for (size_t i = 0; i < out_v_meta_info.size(); i++)
  {
    dump_meta_info(out_v_meta_info[i]);
  }

  printf("will rm file 4_1234->4_file \n");
  ret = tt.rm_file(2, 2,
      30, 31, pname, 5,
      name, 5,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will create file 4_1234->4_file \n");
  sprintf(pname,"%c%s", 4, "1234");
  sprintf(name, "%c%s", 4, "file");
  ret = tt.create_file(2, 2,
      30, 31, pname, 5,
      name, 5,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will pwrite file 4_1234->4_file \n");
  ret = tt.pwrite_file(2, 2,
      31, name, 5,
      100, 1,
      name,  100,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  printf("will pwrite file 4_1234->4_file[100] \n");
  name[5]=name[6]=name[7]=0;
  name[8]=100;
  ret = tt.pwrite_file(2, 2,
      31, name, 9,
      100, 0,
      name,  100,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);

  sprintf(s_pname, "%c%s", 4, "1234");
  sprintf(d_pname, "%cR", 1);
  sprintf(s_name, "%c%s", 4, "file");
  sprintf(d_name, "%c%s", 4, "elif");
  printf("will mv file 4_1234->4_file  R->4_elif\n");
  //scanf("%d", &mysql_proc_ret);
  ret = tt.mv_file(2, 2,
      30, 31, s_pname, 5,
      0, 30, d_pname, 2,
      s_name, 5, d_name, 5,
      mysql_proc_ret);
  printf("ret = %d mysql_proc_ret = %ld\n", ret, mysql_proc_ret);
  ret = tt.ls_meta_info(out_v_meta_info, 2, 2);
  printf("ls_meta_info ret = %d \n", ret);
  for (size_t i = 0; i < out_v_meta_info.size(); i++)
  {
    dump_meta_info(out_v_meta_info[i]);
  }
  ret = tt.ls_meta_info(out_v_meta_info, 2, 1);
  printf("ls_meta_info ret = %d size = %lu\n", ret , out_v_meta_info.size());
  for (size_t i = 0; i < out_v_meta_info.size(); i++)
  {
    dump_meta_info(out_v_meta_info[i]);
  }

  ret = tt.get_nex_val(mysql_proc_ret);
  printf("ret = %d get_nex_val= %ld-----------\n", ret, mysql_proc_ret);
  return 0;
}
