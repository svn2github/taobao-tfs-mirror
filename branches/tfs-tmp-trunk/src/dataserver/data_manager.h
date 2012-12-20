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
#ifndef TFS_DATASERVER_DATAMANAGER_H_
#define TFS_DATASERVER_DATAMANAGER_H_

#include <vector>
#include <map>
#include <list>
#include "common/parameter.h"
#include "data_file.h"
#include "lease_manager.h"
#include "block_manager.h"
#include "Mutex.h"

namespace tfs
{
  namespace dataserver
  {
    class DataManager
    {
      public:
        DataManager();
        ~DataManager();

        /**
         * DataLayer is a middle layer between upper logic and lower storage.
         * many class will use DataManager, so i made it a singleton for
         * simplicity, after we upgraded ds, we can change it back.
         */
        static DataManager& instance()
        {
          static DataManager data_manager_;
          return data_manager_;
        }

      public:
        /** bootstrap, heartbeat, block report */
        int initialize(const std::string& super_block_path);
        int init_block_files(const common::FileSystemParameter& fs_param);
        void get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity);
        int get_all_block_info(std::set<common::BlockInfoV2>& blocks);
        int get_all_block_info(std::set<common::BlockInfo>& blocks);
        int64_t get_all_logic_block_size();

        /** file operation */
        int create_file_id(uint64_t block_id, uint64_t &file_id);
        int stat_file(const uint64_t block_id, const uint64_t file_id, const int32_t flag,
            common::FileInfoV2& file_info);
        int read_file(const uint64_t block_id, const uint64_t file_id,
            char* buffer, int32_t& length, const int32_t offset, const int8_t flag);
        int write_data(WriteLease* lease, const char* buffer,
            const int32_t length, const int32_t offset, const int32_t remote_version,
            common::BlockInfoV2& local);
        int close_file(const uint64_t block_id, uint64_t &file_id, DataFile& data_file);
        int unlink_file(const uint64_t block_id, const uint64_t file_id, const int32_t action,
            const int32_t remote_version, int64_t& size, common::BlockInfoV2& local);

        /** block operations */
        int check_block_version(const uint64_t block_id, const int32_t remote_version,
            common::BlockInfoV2& block_info);
        int get_block_info(const uint64_t block_id, common::BlockInfoV2& block_info);
        int new_block(const uint64_t block_id, const bool tmp = false,
            const int64_t family_id = common::INVALID_FAMILY_ID, const int8_t index_num = 0);
        int remove_block(const uint64_t block_id, const bool tmp = false);
        int read_raw_data(char* buffer, const uint32_t block_id, int32_t& length, const int32_t offset);
        int write_raw_data(const char* buffer, const uint32_t block_id, const int32_t length, const int32_t offset);

        /** inner tools support */
        // TODO

      private:
        BlockManager *block_manager_;
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGER_H_
