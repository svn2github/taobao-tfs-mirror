/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_DATASERVER_LOGIC_BLOCK_H_
#define TFS_DATASERVER_LOGIC_BLOCK_H_

#include "common/lock.h"
#include "common/internal.h"

#include "ds_define.h"
#include "data_file.h"
#include "data_handlev2.h"
#include "index_handlev2.h"
#include "physical_blockv2.h"

namespace tfs
{
  namespace dataserver
  {
    class LogicBlockIterator;
    class LogicBlock
    {
      friend class LogicBlockIterator;
      typedef std::list<PhysicalBlock*> PHYSICAL_BLOCK_LIST;
      typedef PHYSICAL_BLOCK_LIST::iterator PHYSICAL_BLOCK_LIST_ITER;
      typedef PHYSICAL_BLOCK_LIST::const_iterator PHYSICAL_BLOCK_LIST_CONST_ITER;
      public:
        LogicBlock(const uint64_t logic_logic_block_id, const std::string& index_path);
        virtual ~LogicBlock();
        inline uint64_t id() const { return logic_block_id_;}
        int create_index(const int32_t bucket_size, const common::MMapOption mmap_option);
        int load_index(const common::MMapOption mmap_option);
        int remove_self_file();
        int rename_index_filename();
        int generation_file_id(uint64_t& fileid, const double threshold);
        int add_physical_block(PhysicalBlock* block);
        int choose_physic_block(PhysicalBlock*& block, int32_t& length, int32_t& inner_offset, const int32_t offset) const;
        int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const int8_t index, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int check_block_intact();
        int update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int write(const uint64_t fileid, DataFile& datafile, const uint32_t crc, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int pwrite(const char* buf, const int32_t nbytes, const int32_t offset, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int write_file_infos(std::vector<common::FileInfoV2>& infos, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid, const int8_t flag, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int pread(char* buf, int32_t& nbytes, const int32_t offset,const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int stat(common::FileInfoV2& info, const uint64_t fileid, const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int scan(std::vector<common::FileInfo>& finfos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int scan(std::vector<common::FileInfoV2>& finfos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int get_family_id(int64_t& family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int set_family_id(const int64_t family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int get_used_size(int32_t& size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;//data file length
        int get_avail_size(int32_t& size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int statistic_visit(common::ThroughputV2& throughput, const bool reset = false,
            const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      private:
        int extend_block_(const int32_t size, const int32_t offset);
      private:
        uint64_t logic_block_id_;//logic block id
        DataHandle data_handle_;  //data operation handle
        IndexHandle* index_handle_;//index operation handle
        common::RWLock rwmutex_;  //mutex
        PHYSICAL_BLOCK_LIST physical_block_list_;//the physical block list of this logic block
    };

    class LogicBlockIterator
    {
      public:
        LogicBlockIterator(LogicBlock& logic_block);
        virtual ~LogicBlockIterator();
        bool empty() const;
        int next(int32_t& mem_offset, common::FileInfoV2*& info);
        const common::FileInfoV2& get_file_info() const;
        const char* get_data(const int32_t mem_offset) const;
      private:
        int32_t transfer_offet_disk_to_mem_(const int32_t disk_offset) const;
        bool verify_length_(const int32_t mem_offset, const int32_t length) const;
      private:
        LogicBlock& logic_block_;
        static const int32_t MAX_DATA_SIZE = 2 * 1024 * 1024;
        char data_[MAX_DATA_SIZE];//从磁盘上读出的数据缓存
        int32_t read_disk_offset_;//当前缓冲区在磁盘数据中的起始位置x
        BaseIndexHandle::iterator iter_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* LOGIC_BLOCK_H_ */
