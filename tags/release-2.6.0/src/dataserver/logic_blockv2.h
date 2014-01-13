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

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    class BlockManager;
    class BaseLogicBlock : public GCObject
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
      void set_expire_time(const int32_t expire_time);
      int32_t get_expire_time() const;
      int remove_self_file();
      int rename_index_filename();
      int add_physical_block(PhysicalBlock* block);
      int get_all_physical_blocks(std::vector<int32_t>& physical_blocks) const;
      int choose_physic_block(PhysicalBlock*& block, int32_t& length, int32_t& inner_offset, const int32_t offset) const;
      virtual int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const;
      int update_block_info(const common::BlockInfoV2& info) const;
      int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT);
      int get_block_info(common::BlockInfoV2& info) const;
      int get_index_header(common::IndexHeaderV2& header) const;
      int set_index_header(const common::IndexHeaderV2& header);
      int flush();
      virtual int check_block_intact() { return common::TFS_SUCCESS;}
      int load_index(const common::MMapOption mmap_option);
      int traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& finfos, const uint64_t logic_block_id) const;
      int get_attach_blocks(common::ArrayHelper<uint64_t>& blocks) const;
      int get_index_num(int32_t& index_num) const;
      int get_family_id(int64_t& family_id) const;
      int set_family_id(const int64_t family_id);
      int get_used_offset(int32_t& size) const;//data file length
      int set_used_offset(int32_t size);  // used after encoded
      int get_avail_offset(int32_t& size) const;
      int get_marshalling_offset(int32_t& offset) const;
      int set_marshalling_offset(const int32_t size);
      int write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id, const bool partial = false);
      virtual int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp);
      virtual int read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid,
          const int8_t flag, const uint64_t logic_block_id);
      int pwrite(const char* buf, const int32_t nbytes, const int32_t offset, const bool tmp);
      int pread(char* buf, int32_t& nbytes, const int32_t offset);
      int stat(common::FileInfoV2& info, const int8_t flag, const uint64_t logic_block_id) const;
      virtual int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      virtual void callback();
      int statistic_visit(common::ThroughputV2& throughput, const bool reset = false);

      BlockManager& get_block_manager_() { return *manager_;}

     protected:
      int transfer_file_status_(int32_t& oper_type, common::FileInfoV2& info, const int32_t action,
          const uint64_t logic_block_id, const uint64_t fileid) const;
      int extend_block_(const int32_t size, const int32_t offset, const bool tmp);
      int write_(common::FileInfoV2& new_finfo, DataFile& data_file, const common::FileInfoV2& old_finfo, const bool update);
     protected:
      BlockManager* manager_;
      uint64_t logic_block_id_;
      DataHandle data_handle_;  //data operation handle
      BaseIndexHandle* index_handle_;//index operation handle
      PHYSICAL_BLOCK_LIST physical_block_list_;//the physical block list of this logic block
      mutable common::RWLock mutex_;
      int32_t expire_time_;
    };

    class LogicBlock : public BaseLogicBlock
    {
      #ifdef TFS_GTEST
        friend class TestLogicBlock;
        FRIEND_TEST(TestLogicBlock, extend_block_);
        FRIEND_TEST(TestLogicBlock, choose_physic_block);
        FRIEND_TEST(TestLogicBlock, write_);
        FRIEND_TEST(TestLogicBlock, write);
        FRIEND_TEST(TestLogicBlock, unlink);
        FRIEND_TEST(TestLogicBlock, scan_file);
      #endif
      public:
        LogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path);
        virtual ~LogicBlock();
        int create_index(const int32_t bucket_size, const common::MMapOption mmap_option);
        int generation_file_id(uint64_t& fileid);
        int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp);
        int read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid,
          const int8_t flag, const uint64_t logic_block_id);
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int traverse(std::vector<common::FileInfo>& finfos, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int check_block_intact();
        int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0);
        int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0);
        int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
        int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
      private:
        IndexHandle* get_index_handle_() const { return dynamic_cast<IndexHandle*>(index_handle_);}

      public:
        class Iterator
        {
         public:
            Iterator(LogicBlock* logic_block);
            virtual ~Iterator() {}
            bool empty() const;
            bool check_offset_range(const int32_t offset, const int32_t size) const;
            int next(common::FileInfoV2*& info);
            const common::FileInfoV2& get_file_info() const;
            const char* get_data(const int32_t offset, const int32_t size) const;
          private:
            std::vector<common::FileInfoV2> finfos_;
            LogicBlock* logic_block_;
            std::vector<common::FileInfoV2>::iterator iter_;
            int32_t used_offset_;//磁盘文件大小(这里的offset是最后一个文件的起始位置)
            int32_t mem_valid_size_;
            int32_t last_read_disk_offset_;
            char data_[common::MAX_SINGLE_FILE_SIZE];//从磁盘上读出的数据缓存
        };
    };

    class VerifyLogicBlock : public BaseLogicBlock
    {
      #ifdef TFS_GTEST
        friend class TestLogicBlock;
        FRIEND_TEST(TestLogicBlock, verify_write);
        FRIEND_TEST(TestLogicBlock, verify_unlink);
      #endif
      public:
        VerifyLogicBlock(BlockManager* manager, const uint64_t logic_block_id, const std::string& index_path);
        virtual ~VerifyLogicBlock();
        int create_index(const uint64_t logic_block_id, const int64_t family_id, const int16_t index_num);
        int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id, const bool tmp);
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
      private:
        VerifyIndexHandle* get_index_handle_() const { return dynamic_cast<VerifyIndexHandle*>(index_handle_);}
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* LOGIC_BLOCK_H_ */
