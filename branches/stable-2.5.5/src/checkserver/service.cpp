/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <exception>
#include <tbsys.h>
#include <Memory.hpp>
#include "checkserver.h"

int main(int argc, char* argv[])
{
  tfs::checkserver::CheckServer* service = new tfs::checkserver::CheckServer();
  int32_t ret = service->main(argc, argv);
  tbsys::gDelete(service);
  return ret;
}

