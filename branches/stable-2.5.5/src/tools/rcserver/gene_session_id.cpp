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
*   xueya.yy <xueya.yy@taobao.com>
*      - initial release
*
*/

#include "common/session_util.h"
#include "common/func.h"
#include "common/internal.h"
#include <string>
#include <iostream>

using namespace std;
using namespace tfs::common;


int main(int argc, char** argv)
{
  string app_id;
  int64_t session_ip = 0;
  string session_id;
  string app_ip;

  if (argc > 2)
  {
    app_id = argv[1];
    app_ip = argv[2];
  }
  else
  {
    printf("usage: %s app_id app_ip\n", argv[0]);
    return 0;
  }


  session_ip = Func::get_host_ip(app_ip.c_str());

  cout << session_ip << endl;


  SessionUtil::gene_session_id(atoi(app_id.c_str()), session_ip, session_id);

  cout << "session_id: " << session_id << ", app_ip: " << app_ip << endl;

  return 0;
}
