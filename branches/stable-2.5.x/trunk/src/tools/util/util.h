/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: util.h 413 2013-04-18 16:52:46Z duanfei@taobao.com $
 *
 * Authors:
 *   duanfei@taobao.com
 *      - initial release
 */
#ifndef TFS_TOOL_UTIL_H_
#define TFS_TOOL_UTIL_H_

#include "common/internal.h"

namespace tfs
{
  namespace tools
  {
    static const int32_t META_FLAG_ABNORMAL = -9800;
    static const int32_t FILE_STATUS_ERROR  = -9801;
    struct CompareFileInfoByFileId
    {
      bool operator()(const common::FileInfo& left, const common::FileInfo& right) const
      {
        return left.id_ < right.id_;
      }
    };

    class Util
    {
      public:
      static int read_file_infos(const std::string& ns_addr, const uint64_t block, std::multiset<std::string>& files, const int32_t version);
      static int read_file_infos(const std::string& ns_addr, const uint64_t block, std::set<common::FileInfo, CompareFileInfoByFileId>& files, const int32_t version);
      static int read_file_info(const std::string& ns_addr, const std::string& filename, common::FileInfo& info);
      static int read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc);
    };
  }
}
#endif
