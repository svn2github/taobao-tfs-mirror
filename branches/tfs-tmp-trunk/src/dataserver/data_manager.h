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
        inline BlockManager& block_manager();
        int prepare_lease(const uint64_t block_id, uint64_t& file_id, uint64_t& lease_id,
            const LeaseType type, const common::VUINT64& servers);
        int update_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            tbnet::Packet* packet);
        int check_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            std::stringstream& err_msg);
        int remove_lease(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id);

        /** update & unlink operations */
        int write_data(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id,
            const char* buffer, const int32_t length, const int32_t offset,
            const int32_t remote_version, common::BlockInfoV2& local);
        int close_file(const uint64_t block_id, uint64_t& file_id, const uint64_t lease_id);
        int unlink_file(const uint64_t block_id, const uint64_t file_id, const int64_t lease_id,
            const int32_t action, const int32_t remote_version, int64_t& size, common::BlockInfoV2& local);

        /** helper functions */
        int update_block_info(const uint64_t block_id, const common::UnlinkFlag unlink_flag);
        int resolve_block_version_conflict(const uint64_t block_id, const uint64_t file_id, const uint64_t lease_id);

      private:
        DataService& service_;
        LeaseManager lease_manager_;
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGER_H_
