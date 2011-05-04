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
using namespace tfs::rcserver;
int main()
{
  MysqlDatabaseHelper hl;
  hl.set_conn_param("10.232.31.33:3306:tfs_stat", "tfs", "tfs_stat#2012");
  int ret;
  ret = hl.connect();
  printf("ret = %d\n", ret);
  VResourceServerInfo outparam;
  ret = hl.scan(outparam);
  printf("ret = %d\n", ret);
  for (int i = 0; i < outparam.size(); i++)
  {
    printf("%s %d %s\n", outparam[i].addr_info_, outparam[i].stat_, outparam[i].rem_);
  }

  return 0;
}
