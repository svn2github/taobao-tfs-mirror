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
#ifndef TFS_NAMEMETASERVER_META_SERVER_DEFINE_H_
#define TFS_NAMEMETASERVER_META_SERVER_DEFINE_H_

#include <string>
namespace tfs
{
  namespace namemetaserver
  {
    const int32_t MAX_FILE_PATH_LEN = 512;
    const int32_t SOFT_MAX_FRAG_INFO_COUNT = 5;
    const int32_t MAX_FRAG_INFO_SIZE = 65535;
    const int32_t MAX_OUT_FRAG_INFO = 256;
    const int32_t ROW_LIMIT = 500;

    struct ConnStr
    {
      static std::string mysql_conn_str_;
      static std::string mysql_user_;
      static std::string mysql_password_;
    };

    enum FileType
    {
      NORMAL_FILE = 1,
      DIRECTORY = 2,
      PWRITE_FILE = 3
    };
  }
}

#endif
