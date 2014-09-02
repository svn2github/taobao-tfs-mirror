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
#ifndef KV_META_H_
#define KV_META_H_

#include <iostream>
#include "tfs_client_api.h"

using namespace std;
using namespace tfs::client;

class Rest
{
  public:
  Rest(const char *rs_addr, const char *app_key)
  {
    int ret ;
    ret = client.initialize(rs_addr, app_key);

    if (TFS_SUCCESS != ret)
    {
      cout << "Client initalize fail!" << endl;
    }
    else
    {
      cout << "Client initalize success!" << endl;
    }
  }

  RestClient& get_client()
  {
    return client;
  }
  private:
    RestClient client;
};
#endif
