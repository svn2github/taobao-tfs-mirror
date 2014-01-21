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
#include "tfs_rc_client_api.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::client;

class KVMeta
{
  public:
  KVMeta(const char *kms_addr, const char *rc_addr, const char *nginx_rs_addr, const char *app_key)
  {
    int ret = -1;
    rc_client_.set_kv_rs_addr(kms_addr);
    ret = rc_client_.initialize(rc_addr, app_key);
    if (TFS_SUCCESS != ret)
    {
      cout << "Client initalize fail!" << endl;
    }
    else
    {
      ret = rest_client_.initialize(nginx_rs_addr, app_key);
      if (TFS_SUCCESS == ret)
      {
        cout << "Client initalize success!" << endl;
      }
    }
  }

  public:
    RcClient rc_client_;
    RestClient rest_client_;
};
#endif
