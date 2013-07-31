/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: main.cpp 495 2011-06-14 08:47:12Z nayan@taobao.com $
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *      - initial release
 *
 */
#include "kv_meta_service.h"

int main(int argc, char* argv[])
{
  tfs::kvmetaserver::KvMetaService service;
  return service.main(argc, argv);
}
