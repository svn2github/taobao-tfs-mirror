/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   chuyu <chuyu@taobao.com>
 *      - initial release
 *
 */
#include "meta_server_define.h"

namespace tfs
{
  namespace namemetaserver
  {
    std::string ConnStr::mysql_conn_str_ = "10.232.35.41:3306:tfs_name_db";
    std::string ConnStr::mysql_user_ = "root";
    std::string ConnStr::mysql_password_ = "root";

  }
}

