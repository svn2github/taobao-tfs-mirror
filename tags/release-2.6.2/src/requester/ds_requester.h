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
#ifndef TFS_REQUESTER_DSREQUESTER_H_
#define TFS_REQUESTER_DSREQUESTER_H_

#include "common/internal.h"

namespace tfs
{
  namespace requester
  {
    // all requests are sent to dataserver
    class DsRequester
    {
      public:
        // get block's index header and all file info list
        static int read_block_index(const uint64_t ds_id,
            const uint64_t block_id, const uint64_t attach_block_id,
            common::IndexDataV2& index_data);
        static int stat_file(const uint64_t ds_id,
            const uint64_t block_id, const uint64_t attach_block_id,
            const uint64_t file_id, common::TfsFileStat& file_stat, const int32_t flag);
        static int read_file(const uint64_t ds_id,
            const uint64_t block_id, const uint64_t attach_block_id, const uint64_t file_id,
            char* data, const int32_t offset, const int32_t length, const int32_t flag);
        static int list_block(const uint64_t ds_id, std::vector<common::BlockInfoV2>& block_infos);

        static int read_raw_data(const uint64_t server, const uint64_t block,
            const int64_t traffic, const int64_t total_size, tbnet::DataBuffer& data);
        static int recombine_raw_data(const common::IndexDataV2& sindex, tbnet::DataBuffer& sbuf,
        common::IndexDataV2& dindex, tbnet::DataBuffer& dbuf, std::vector<common::FileInfoV2>& nosync_files);
        static int write_raw_data(tbnet::DataBuffer& buf, const uint64_t block, const uint64_t server, const int32_t traffic = 4 * 1024 * 1024);
        static int write_raw_index(const common::IndexDataV2& index_data, const uint64_t block, const uint64_t server);
        static int remove_block(const uint64_t block, const std::string& addr, const bool tmp);
    };
  }
}

#endif
