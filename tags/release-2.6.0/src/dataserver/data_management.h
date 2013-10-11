/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: data_management.h 515 2011-06-17 01:50:58Z duanfei@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *   qushan<qushan@taobao.com>
 *      - modify 2009-03-27
 *   zongdai <zongdai@taobao.com>
 *      - modify 2010-04-23
 *   linqing <linqing.zyd@taobao.com>
 *      - modify 2012-12-12
 *
 *
 */
#ifndef TFS_DATASERVER_DATAMANAGEMENT_H_
#define TFS_DATASERVER_DATAMANAGEMENT_H_

#include <vector>
#include <map>
#include <list>
#include "ds_define.h"
#include "data_file.h"
#include "common/parameter.h"
#include <Mutex.h>
#include "block_manager.h"

namespace tfs
{
  namespace dataserver
  {

    class DataService;
    class DataManagement
    {
      public:
        explicit DataManagement(DataService& service);
        ~DataManagement();

      public:
        inline BlockManager& get_block_manager();
        void set_file_number(const uint64_t file_number);

        int create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number);
        int write_data(const common::WriteDataInfo& write_info, const int32_t lease_id, int32_t& version,
            const char* data_buffer, common::UpdateBlockType& repair);
        int erase_data_file(const uint64_t file_number);
        int close_write_file(const common::CloseFileInfo& close_file_info, const int32_t force_status, int32_t& write_file_size);
        int read_data(const uint32_t block_id, const uint64_t file_id, const int32_t read_offset, const int8_t flag,
            int32_t& real_read_len, char* tmpDataBuffer);

        int read_file_info(const uint32_t block_id,
            const uint64_t file_id, const int32_t mode, common::FileInfo& finfo);
        int unlink_file(const uint32_t block_id, const uint64_t file_id, const int32_t action, int64_t& file_size);

        int get_block_info(const uint32_t block_id, common::BlockInfo& blk, int32_t& visit_count);
        int del_single_block(const uint32_t block_id, const bool tmp = false);

        //gc thread
        int gc_data_file();

      private:
        DISALLOW_COPY_AND_ASSIGN(DataManagement);

        DataService& service_;
        uint64_t file_number_;          // file id
        tbutil::Mutex data_file_mutex_; // datafile mutex
        DataFileMap data_file_map_; // datafile map

        //gc datafile
        int32_t last_gc_data_file_time_; // last datafile gc time
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGEMENT_H_
