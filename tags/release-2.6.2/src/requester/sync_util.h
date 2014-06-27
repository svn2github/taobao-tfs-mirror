/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *  linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_REQUESTER_SYNCUTIL_H_
#define TFS_REQUESTER_SYNCUTIL_H_

#include "common/internal.h"
#include "ns_requester.h"

namespace tfs
{
  namespace requester
  {
    static const int32_t META_FLAG_ABNORMAL = -9800;
    static const int32_t FILE_STATUS_ERROR  = -9801;
    static const int32_t SYNC_SUCCESS = 0;
    static const int32_t SYNC_FAILED  = -99999;
    static const int32_t SYNC_NOTHING = -100000;

    // all requests are sent to nameserver
    class SyncUtil
    {
      public:
        static int read_file_infos(const std::string& ns_addr, const uint64_t block, std::multiset<std::string>& files, const int32_t version);
        static int read_file_info(const std::string& ns_addr, const std::string& filename, common::FileInfo& info);
        static int read_file_info_v2(const std::string& ns_addr, const std::string& filename, common::FileInfoV2& info);
        static int read_file_stat(const std::string& ns_addr, const std::string& filename, common::TfsFileStat& info);
        static int read_file_real_crc(const std::string& ns_addr, const std::string& filename, uint32_t& crc);
        static int read_file_real_crc_v2(const std::string& ns_addr, const std::string& filename, common::FileInfoV2& info, const bool force);
        static int write_file(const std::string& ns_addr, const std::string& filename, const char* data, const int32_t size, const int32_t status);
        static int copy_file(const std::string& src_ns_addr, const std::string& dest_ns_addr, const std::string& file_name, const int32_t status);
        static int cmp_and_sync_file(const std::string& src_ns_addr, const std::string& dest_ns_addr, const std::string& file_name,
        const int64_t timestamp, const bool force, common::FileInfoV2& left, common::FileInfoV2& right, const bool confirm = true);
        static int sync_file(const std::string& saddr, const std::string& daddr, const std::string& filename,
            const common::FileInfoV2& sfinfo, const common::FileInfoV2& dfinfo, const int64_t timestamp, const bool force);
    };

  }
}

#endif
