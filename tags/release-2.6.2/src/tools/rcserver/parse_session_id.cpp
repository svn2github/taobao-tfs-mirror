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
#include <iostream>

using namespace std;
using namespace tfs::common;


int main(int argc, char** argv)
{
  int32_t app_id = 0;
  int64_t session_ip = 0;
  string session_id;
  string app_ip;

  if (argc > 1)
  {
    session_id = argv[1];
  }
  else
  {
    printf("usage: %s session_id\n", argv[0]);
    return 0;
  }


 int ret =  SessionUtil::parse_session_id(session_id, app_id, session_ip);

  if (TFS_SUCCESS == ret)
  {
    cout << session_ip << endl;
    app_ip = Func::addr_to_str(-session_ip, true);
    cout << "session_id: " << session_id << ", app_ip: " << app_ip << endl;
  }
  else
  {
    cout << "parse from session_id to app_ip failed" << endl;
    return 1;
  }

  return 0;
}
