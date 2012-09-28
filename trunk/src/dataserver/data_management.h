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
 *
 */
#ifndef TFS_DATASERVER_DATAMANAGEMENT_H_
#define TFS_DATASERVER_DATAMANAGEMENT_H_

#include <vector>
#include <map>
#include <list>
#include "logic_block.h"
#include "dataserver_define.h"
#include "lease_manager.h"
#include "common/parameter.h"
#include <Mutex.h>

namespace tfs
{
  namespace dataserver
  {

    class DataManagement
    {
      public:
        DataManagement();
        ~DataManagement();

      public:
        void set_file_number(const uint64_t file_number);
        int init_block_files(const common::FileSystemParameter& fs_param);
        void get_ds_filesystem_info(int32_t& block_count, int64_t& use_capacity, int64_t& total_capacity);
        int get_all_logic_block(std::list<LogicBlock*>& logic_block_list);
        int get_all_block_info(std::set<common::BlockInfo>& blocks);
        int get_all_block_info(std::set<common::BlockInfoExt>& blocks_ext);
        int64_t get_all_logic_block_size();

        int create_file(const uint32_t block_id, uint64_t& file_id, uint64_t& file_number);
        int write_data(common::BlockInfo& block_info, WriteLease* lease, const int32_t remote_version, const common::WriteDataInfo& write_info,
            const char* data_buffer);
        int close_write_file(const common::CloseFileInfo& close_file_info, int32_t& write_file_size);
        int read_data(const uint32_t block_id, const uint64_t file_id, const int32_t read_offset, const int8_t flag,
            int32_t& real_read_len, char* tmpDataBuffer);
        int read_data_degrade(const uint32_t block_id, const uint64_t file_id,
            const int32_t read_offset, const int8_t flag, int32_t& real_read_len,
            char* tmpDataBuffer, const common::FamilyMemberInfoExt& family_info);
        int read_raw_data(uint32_t block_id, int32_t read_offset, int32_t& real_read_len,
            char* tmpDataBuffer, int32_t& data_file_size);

        int read_file_info(const uint32_t block_id,
            const uint64_t file_id, const int32_t mode, common::FileInfo& finfo);
        int rename_file(const uint32_t block_id, const uint64_t file_id, const uint64_t new_file_id);
        int unlink_file(common::BlockInfo& info, int64_t& file_size, const uint32_t block_id, const uint64_t file_id, const int32_t action, const int32_t remote_version);

        int batch_new_block(const common::VUINT32* new_blocks);
        int batch_remove_block(const common::VUINT32* remove_blocks);

        int query_bit_map(const int32_t query_type, char** tmp_data_buffer, int32_t& bit_map_len, int32_t& set_count);

        int query_block_status(const int32_t query_type, common::VUINT& block_ids, std::map<uint32_t, std::vector<
            uint32_t> >& logic_2_physic_blocks, std::map<uint32_t, common::BlockInfo*>& block_2_info);
        int get_block_info(const uint32_t block_id, common::BlockInfo*& blk, int32_t& visit_count);

        int get_visit_sorted_blockids(std::vector<LogicBlock*>& block_ptrs);
        int get_block_file_list(const uint32_t block_id, std::vector<common::FileInfo>& fileinfos);
        int get_block_meta_info(const uint32_t block_id, common::RawMetaVec& meta_list);
        int reset_block_version(const uint32_t block_id);

        int new_single_block(const uint32_t block_id, const BlockType type = C_MAIN_BLOCK);
        int del_single_block(const uint32_t block_id);
        int get_block_curr_size(const uint32_t block_id, int32_t& size);
        int write_raw_data(const uint32_t block_id, const int32_t data_offset, const int32_t msg_len,
            const char* data_buffer);
        int batch_write_meta(const uint32_t block_id, const common::BlockInfo* blk,
            const common::RawMetaVec* meta_list);
        int write_raw_index(const uint32_t block_id, const int64_t family_id,
            const common::RawIndexOp index_op, const common::RawIndexVec* index_vec);
        int read_raw_index(const uint32_t block_id, const common::RawIndexOp index_op, const uint32_t index_id,
            char* & buf, uint32_t& size);

        int add_new_expire_block(const common::VUINT32* expire_block_ids, const common::VUINT32* remove_block_ids,
            const common::VUINT32* new_block_ids);

        int expire_blocks(const common::VUINT32& expire_blocks,
            std::set<common::BlockInfoExt>& clear_blocks, std::set<common::BlockInfoExt>& delete_blocks);

        inline LeaseManager& get_lease_manager() { return lease_manager_;}
      private:
        int create_file_id_(uint64_t& file_id, const uint32_t block_id);

      private:
        DISALLOW_COPY_AND_ASSIGN(DataManagement);
        LeaseManager lease_manager_;
    };

    struct visit_count_sort
    {
        bool operator()(const LogicBlock *x, const LogicBlock *y) const
        {
          return (x->get_visit_count() > y->get_visit_count());
        }
    };
  }
}
#endif //TFS_DATASERVER_DATAMANAGEMENT_H_
