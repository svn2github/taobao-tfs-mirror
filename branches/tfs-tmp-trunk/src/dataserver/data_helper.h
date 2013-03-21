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
    class TrafficControl;
    class BlockManager;
    class DataService;
    class DataHelper
    {
      public:
        explicit DataHelper(DataService& service);
        ~DataHelper();

        inline BlockManager& get_block_manager();
        inline TrafficControl& get_traffic_control();

        /**
        * @brief send a request whose response is a StatusMessage
        *
        * @return TFS_SUCCESS only if server return STATUS_MESSAGE_OK
        */
        int send_simple_request(uint64_t server_id, common::BasePacket* message);

        int new_remote_block(const uint64_t server_id, const uint64_t block_id,
            const bool tmp = false, const uint64_t family_id = common::INVALID_FAMILY_ID ,
            const int32_t index_num = 0);
        int delete_remote_block(const uint64_t server_id, const uint64_t block_id,
            const bool tmp = false);

        int read_raw_data(const uint64_t server_id, const uint64_t block_id,
            char* data, int32_t length, const int32_t offset);
        int write_raw_data(const uint64_t server_id, const uint64_t block_id,
            const char* data, const int32_t length, const int32_t offset);
        int read_index(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);
        int write_index(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);

        int query_ec_meta(const uint64_t server_id, const uint64_t block_id,
            common::ECMeta& ec_meta);
        int commit_ec_meta(const uint64_t server_id, const uint64_t block_id,
            const common::ECMeta& ec_meta, const int8_t switch_flag = common::SWITCH_BLOCK_NO);

        // we should know file's real length first
        int read_file(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id,
            char* data, const int32_t len, const int32_t off, const int8_t flag);
        int write_file(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id,
            const char*data, const int32_t len, const bool tmp);

        int stat_file_degrade(const uint64_t block_id, const uint64_t file_id, const int32_t flag,
            const common::FamilyInfoExt& family_info, common::FileInfoV2& finfo);
        int read_file_degrade(const uint64_t block_id, const common::FileInfoV2& finfo, char* data,
            const int32_t length, const int32_t offset, const int8_t flag,
            const common::FamilyInfoExt& family_info);

      private:
        int new_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
            const bool tmp = false, const uint64_t family_id = common::INVALID_FAMILY_ID ,
            const int32_t index_num = 0);
        int delete_remote_block_ex(const uint64_t server_id, const uint64_t block_id,
            const bool tmp = false);

        int read_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
            char* data, int32_t& length, const int32_t offset);
        int write_raw_data_ex(const uint64_t server_id, const uint64_t block_id,
            const char* data, const int32_t length, const int32_t offset);
        int read_index_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);
        int write_index_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, common::IndexDataV2& index_data);

        int query_ec_meta_ex(const uint64_t server_id, const uint64_t block_id,
            common::ECMeta& ec_meta);
        int commit_ec_meta_ex(const uint64_t server_id, const uint64_t block_id,
            const common::ECMeta& ec_meta, const int8_t switch_flag = common::SWITCH_BLOCK_NO);

        int stat_file_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id, const int8_t flag,
            common::FileInfoV2& finfo);
        int read_file_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id,
            char* data, int32_t& length, const int32_t offset, const int8_t flag);
        int write_file_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id,
            const char* data, const int32_t length, const int32_t offset, uint64_t& lease_id);
        int close_file_ex(const uint64_t server_id, const uint64_t block_id,
            const uint64_t attach_block_id, const uint64_t file_id, const uint64_t lease_id, const bool tmp);

        int prepare_read_degrade(const common::FamilyInfoExt& family_info, int* erased);
        int read_file_degrade_ex(const uint64_t block_id, const common::FileInfoV2& finfo,
            char* buffer, const int32_t length, const int32_t offset,
            const common::FamilyInfoExt& family_info, int* erased);

      private:
        DataService& service_;
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGER_H_
