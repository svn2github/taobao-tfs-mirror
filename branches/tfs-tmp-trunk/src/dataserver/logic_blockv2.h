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
    class BlockManager;
    class LogicBlockIterator;
    class BaseLogicBlock
    {
      public:
      typedef std::vector<PhysicalBlock*> PHYSICAL_BLOCK_LIST;
      typedef PHYSICAL_BLOCK_LIST::iterator PHYSICAL_BLOCK_LIST_ITER;
      typedef PHYSICAL_BLOCK_LIST::const_iterator PHYSICAL_BLOCK_LIST_CONST_ITER;
     public:
      BaseLogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path);
      explicit BaseLogicBlock(const uint64_t logic_block_id); //for query
      virtual ~BaseLogicBlock();
      inline uint64_t id() const { return logic_block_id_;}
      int remove_self_file();
      int rename_index_filename();
      int add_physical_block(PhysicalBlock* block);
      int get_all_physical_blocks(std::vector<int32_t>& physical_blocks) const;
      int choose_physic_block(PhysicalBlock*& block, int32_t& length, int32_t& inner_offset, const int32_t offset) const;
      int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const int8_t index, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      int update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      int get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      virtual int check_block_intact() { return common::TFS_SUCCESS;}
      int load_index(const common::MMapOption mmap_option);
      int traverse(std::vector<common::FileInfoV2>& finfos, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      int get_family_id(int64_t& family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      int set_family_id(const int64_t family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      int get_used_size(int32_t& size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;//data file length
      int get_avail_size(int32_t& size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      int write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      virtual int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      int read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid, const int8_t flag, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      int pwrite(const char* buf, const int32_t nbytes, const int32_t offset);
      int pread(char* buf, int32_t& nbytes, const int32_t offset);
      int stat(common::FileInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
      virtual int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);

      int64_t get_last_update_time() const { return 0;}//TODO

      BlockManager& get_block_manager_() { return *manager_;}

     protected:
      int transfer_file_status_(int32_t& oper_type, common::FileInfoV2& info, const int32_t action,
          const uint64_t logic_block_id, const uint64_t fileid) const;
      int extend_block_(const int32_t size, const int32_t offset);
      int write_(common::FileInfoV2& new_finfo, DataFile& data_file, const common::FileInfoV2& old_finfo, const bool update);
     protected:
      BlockManager* manager_;
      uint64_t logic_block_id_;
      DataHandle data_handle_;  //data operation handle
      BaseIndexHandle* index_handle_;//index operation handle
      PHYSICAL_BLOCK_LIST physical_block_list_;//the physical block list of this logic block
    };

    class LogicBlock : public BaseLogicBlock
    {
      friend class LogicBlockIterator;
      public:
        LogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path);
        virtual ~LogicBlock();
        int create_index(const int32_t bucket_size, const common::MMapOption mmap_option);
        int generation_file_id(uint64_t& fileid, const double threshold);
        int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int traverse(std::vector<common::FileInfo>& finfos, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int check_block_intact();
        int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0);
        int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0);
        int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
        int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
        int statistic_visit(common::ThroughputV2& throughput, const bool reset = false);
      private:
        IndexHandle* get_index_handle_() const { return dynamic_cast<IndexHandle*>(index_handle_);}
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
        int32_t used_offset_;//磁盘文件大小(这里的offset是最后一个文件的起始位置)
        int32_t mem_valid_size_;
        BaseIndexHandle::iterator iter_;
    };

    class VerifyLogicBlock : public BaseLogicBlock
    {
      public:
        VerifyLogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path);
        virtual ~VerifyLogicBlock();
        int create_index(const uint64_t logic_block_id, const int64_t family_id, const int16_t index_num);
        int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      private:
        VerifyIndexHandle* get_index_handle_() const { return dynamic_cast<VerifyIndexHandle*>(index_handle_);}
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* LOGIC_BLOCK_H_ */
