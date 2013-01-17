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
#ifndef TFS_DATASERVER_DATAHELPER_H_
#define TFS_DATASERVER_DATAHELPER_H_

#include "common/error_msg.h"
#include "common/internal.h"
#include "common/array_helper.h"

namespace tfs
{
  namespace dataserver
  {
    class BlockManager;
    class DataService;
    class DataHelper
    {
      public:
        explicit DataHelper(DataService& service);
        ~DataHelper();

        inline BlockManager& block_manager();

        int new_remote_block(const uint64_t server_id, const uint64_t block_id,
            const bool tmp, const uint64_t family_id, const int32_t index_num);

        /**
         * use following interface to do flow control(shor for fc)
         */
        int read_raw_data(const uint64_t server_id, const uint64_t block_id,
            char* data, int32_t& length, const int32_t offset);
        int write_raw_data(const uint64_t server_id, const uint64_t block_id,
            const char* data, const int32_t length, const int32_t offset);
        int read_index(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);
        int write_index(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data,
            const int32_t switch_flag = common::SWITCH_BLOCK_NO);

        int query_ec_meta(const uint64_t server_id, const uint64_t block_id,
            common::ECMeta& ec_meta);
        int commit_ec_meta(const uint64_t server_id, const uint64_t block_id,
            const common::ECMeta& ec_meta);
        int query_file_info(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id, common::FileInfoV2& finfo);

      private:
        int read_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
            char* data, int32_t& length, const int32_t offset);
        int write_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
            const char* data, const int32_t length, const int32_t offset);
        int read_index_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);
        int write_index_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data,
            const int32_t switch_flag = common::SWITCH_BLOCK_NO);

      private:
        DataService& service_;
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGER_H_
