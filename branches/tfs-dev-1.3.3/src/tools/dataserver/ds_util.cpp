/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "ds_util.h"
#include "tbsys.h"
#include "common/func.h"

using namespace tfs::common;

uint64_t get_ip_addr(const char *ip)
{
  char* port_str = strchr(const_cast<char*>(ip), ':');
  if (port_str == NULL) 
  {
    TBSYS_LOG(ERROR, "ip format is wrong %s, ip:port", ip);
    return 0;
  }

  *port_str = '\0';
  int port = atoi(port_str + 1);
  return Func::str_to_addr(ip, port);
}
