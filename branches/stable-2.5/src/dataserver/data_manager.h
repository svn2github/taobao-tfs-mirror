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
#include <sstream>
#include "common/parameter.h"
#include "data_file.h"
#include "lease_manager.h"
#include "Mutex.h"

namespace tfs
{
  namespace dataserver
  {
    class Dataservice;
    class DataManager
    {
      public:
        explicit DataManager(DataService& service);
        ~DataManager();

      public:
        /** lease management */
        inline BlockManager& get_block_manager();
        int prepare_lease(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id,
            const LeaseType type, const common::VUINT64& servers, const bool alloc);
        int update_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            tbnet::Packet* packet);
        int update_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            const int32_t error_code, const common::BlockInfoV2& info);

        /*
         * check_lease will return true if all server finish
         * status is the final status to be taken back
         * if all successful file_size will be set, or err_msg will be set
         *
         * if check_lease return false, nothing should be done.
         */
        bool check_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            int32_t& status, int64_t& req_cost_time, int64_t& file_size, std::stringstream& err_msg);
        int remove_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id);

        /** update & unlink operations */
        int write_file(const uint64_t block_id, const uint64_t attach_block_id,
            const uint64_t file_id, const uint64_t lease_id,
            const char* buffer, const int32_t length, const int32_t offset,
            const int32_t remote_version, common::BlockInfoV2& local);
        int close_file(const uint64_t block_id, const uint64_t attach_block_id,
            uint64_t& file_id, const uint64_t lease_id, const uint32_t crc, const int32_t status,
            const bool tmp, common::BlockInfoV2& local);
        int prepare_unlink_file(const uint64_t block_id, const uint64_t attach_block_id,
            const uint64_t file_id, const int64_t lease_id, const int32_t action,
            const int32_t remote_version, common::BlockInfoV2& local);
        int unlink_file(const uint64_t block_id, const uint64_t attach_block_id, const uint64_t file_id,
            const int64_t lease_id, const int32_t action, common::BlockInfoV2& local);

        /** helper functions */
        int update_block_info(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            const common::UpdateBlockInfoType type);
        int update_block_info(const common::BlockInfoV2& block_info, const common::UpdateBlockInfoType type);
        int resolve_block_version_conflict(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id);
        int timeout(const time_t now_us);

      private:
        DataService& service_;
        LeaseManager lease_manager_;
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGER_H_
