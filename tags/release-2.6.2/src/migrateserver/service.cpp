/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: service.cpp 344 2013-08-29 10:17:38Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei<duanfei@taobao.com>
 *      - initial release
 */
#include <exception>
#include <tbsys.h>
#include <Memory.hpp>
#include "migrateserver.h"

int main(int argc, char* argv[])
{
  tfs::migrateserver::MigrateService* service = new tfs::migrateserver::MigrateService();
  int32_t ret = service->main(argc, argv);
  tbsys::gDelete(service);
  return ret;
}

