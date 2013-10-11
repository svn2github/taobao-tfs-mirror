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
#include "tfs_rc_client_api.h"

using namespace std;
using namespace tfs::client;

class KVMeta
{
  public:
  KVMeta(const char * kms_addr, const char * rc_addr, const char * app_key)
  {
    int Ret ;
    client.set_kv_rs_addr(kms_addr);
    Ret = client.initialize(rc_addr,app_key);
    if(Ret<0)
      cout<<"Client initalize fail!"<<endl;
    else
      cout<<"Client initalize success!"<<endl;
  }
  public:
    RcClient client;

};
#endif
