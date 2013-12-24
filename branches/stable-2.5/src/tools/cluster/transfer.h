/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: trasnfer.h 2312 2013-11-26 08:46:08Z duanfei $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#include <sys/types.h>
#include <sys/syscall.h>
#include "tools/util/base_worker.h"
#include "clientv2/tfs_client_impl_v2.h"
#include "clientv2/fsname.h"
#include "common/func.h"
#include "common/internal.h"

namespace tfs
{
  namespace transfer
  {
    enum TransferType
    {
      TRANSFER_TYPE_TRANSFER = 0,
      TRANSFER_TYPE_SYNC_BLOCK = 1,
      TRANSFER_TYPE_SYNC_FILE = 2,
      TRANSFER_TYPE_COMPARE_BLOCK = 3
    };
    class SyncByBlockManger;
    class SyncByBlockWorker: public tfs::tools::BaseWorker
    {
      public:
        SyncByBlockWorker(SyncByBlockManger& manager) : manager_(manager){}
        virtual ~SyncByBlockWorker(){}

        virtual int process(std::string& line);
        virtual void destroy();
      private:
        int transfer_block_by_raw(const uint64_t block, const uint64_t server);
        int transfer_block_by_file(const uint64_t block);
        int transfer_file(const std::string& filename);
        int compare_crc_by_block(const uint64_t block);
        int compare_crc_by_finfo(const std::string& name, const common::FileInfoV2& left, const common::FileInfoV2& right);
        int check_block_integrity(const uint64_t block, const uint64_t server, const common::IndexDataV2& dindex_data);
        int check_file_integrity(const std::string& filename, const common::FileInfoV2& left, common::FileInfoV2& right);
        int check_dest_block_copies(const uint64_t block, const bool force_remove);

      private:
        SyncByBlockManger& manager_;
    };

    class SyncByBlockManger: public tfs::tools::BaseWorkerManager
    {
      public:
        SyncByBlockManger()
        {
          index_ = 0;
        }
        virtual ~SyncByBlockManger()
        {
        }
        virtual int begin();
        virtual void end();
        virtual tools::BaseWorker* create_worker();
        virtual void usage(const char* app_name);
        uint64_t choose_target_server() ;
      private:
        int32_t index_;
        std::vector<uint64_t> dest_server_addr_;
    };
  } /** end namespace transfer **/
} /** end namespace tfs **/


