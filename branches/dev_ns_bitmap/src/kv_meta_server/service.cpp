/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: service.cpp  $
 *
 * Authors:
 *   qixiao <qixiao.zs@alibaba-inc.com>
 *      - initial release
 *
 */
#include <exception>
#include <tbsys.h>
#include <Memory.hpp>
#include "kv_root_server.h"

int main(int argc, char* argv[])
{
  tfs::kvrootserver::KvRootServer service;
  return service.main(argc, argv);
}

