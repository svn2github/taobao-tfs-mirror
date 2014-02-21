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

#ifndef TFS_DATASERVER_INDEX_HEADER_H_
#define TFS_DATASERVER_INDEX_HEADER_H_

#include "common/internal.h"
#include "common/error_msg.h"
#include "common/mmap_file_op.h"
#include "common/array_helper.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    class VerifyLogicBlock;
    class BaseIndexHandle
    {
      public:
        typedef common::FileInfoV2* iterator;
      public:
        explicit BaseIndexHandle(const std::string& path): is_load_(false), file_op_(path, O_RDWR | O_LARGEFILE | O_CREAT) {}
        virtual ~BaseIndexHandle() {}
        virtual int read_file_info(common::FileInfoV2& info, const double threshold,
            const int32_t max_hash_bucket, const uint64_t logic_block_id) const  = 0;
        virtual int write_file_info(common::FileInfoV2& info,const double threshold,
            const int32_t max_hash_bucket, const uint64_t logic_block_id, const bool update) = 0;
        virtual int write_file_infos(const common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos,
            const double threshold, const int32_t max_hash_bucket, const uint64_t logic_block_id, const bool partial, const int32_t reserved_space_ratio) = 0;
        virtual int traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id) const = 0;
        virtual int get_attach_blocks(common::ArrayHelper<uint64_t>& blocks) const = 0;
        virtual int get_index_num(int32_t& index_num) const = 0;
        virtual int mmap(const uint64_t logic_block_id, const common::MMapOption& map_options) = 0;
        int flush();
        int update_block_info(const common::BlockInfoV2& info) const;
        int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT);
        int get_block_info(common::BlockInfoV2& info) const;
        int get_index_header(common::IndexHeaderV2& header) const;
        int set_index_header(const common::IndexHeaderV2& header);
        virtual int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const;
        int update_used_offset(const int32_t size);
        int get_used_offset(int32_t& offset) const;
        int set_used_offset(const int32_t size);
        int update_avail_offset(const int32_t size);
        int get_avail_offset(int32_t& offset) const;
        int get_marshalling_offset(int32_t& offset) const;
        int set_marshalling_offset(const int32_t size);
        int get_family_id(int64_t& family_id) const;
        int set_family_id(const int64_t family_id);
        int rename_filename(const uint64_t logic_block_id);
        int remove_self(const uint64_t logic_block_id);
        int check_load() const;
        int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0);
        int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0);
        int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
        int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0);
        int statistic_visit(common::ThroughputV2& throughput, const bool reset = false);
      protected:
        virtual int remmap_(const double threshold, const int32_t max_hash_bucket, const int32_t advise_per_mmap_size = 0) const = 0;
        virtual common::FileInfoV2*  get_file_infos_array_() const = 0;
        common::IndexHeaderV2* get_index_header_() const;
        int update_block_statistic_info_(common::IndexHeaderV2* header, const int32_t oper_type,
              const int32_t new_size, const int32_t old_size, const bool rollback = false);
        void get_prev_(common::FileInfoV2* prev, common::FileInfoV2* current, bool complete) const;
        int get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
              common::FileInfoV2*& prev, common::FileInfoV2* finfo, common::IndexHeaderV2* header, const int8_t type) const;
        int get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
              common::FileInfoV2*& prev, const char* buf, const int32_t nbytes, const int8_t type) const;
        int get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
              common::FileInfoV2*& prev, const double threshold, const int32_t max_hash_bucket,const int8_t type) const ;
        int insert_file_info_(common::FileInfoV2& info, const double threshold, const int32_t max_hash_bucket,const bool update);
        int insert_file_info_(common::FileInfoV2& info, char* buf, const int32_t nbytes, const bool update) const;
        bool is_load_;
        mutable common::MMapFileOperation file_op_;
      private:
        DISALLOW_COPY_AND_ASSIGN(BaseIndexHandle);
    };
    // create index file. inner format:
    // -----------------------------------------------------
    // | index header|   common::FileInfoV2s                       |
    // ----------------------------------------------------
    // | IndexHeader | common::FileInfoV2|common::FileInfoV2|...|common::FileInfoV2|
    // -----------------------------------------------------

    class IndexHandle : public BaseIndexHandle
    {
        #ifdef TFS_GTEST
        friend class TestIndexHandle;
        FRIEND_TEST(TestIndexHandle, get_slot_v1);
        FRIEND_TEST(TestIndexHandle, insert_file_info);
        FRIEND_TEST(TestIndexHandle, remmap);
        FRIEND_TEST(TestIndexHandle, write_read_file_info);
        #endif
      public:
        explicit IndexHandle(const std::string& path);
        virtual ~IndexHandle();
        int create(const uint64_t logic_block_id, const int32_t max_bucket_size, const common::MMapOption& map_options);
        int mmap(const uint64_t logic_block_id, const common::MMapOption& map_options);
        int generation_file_id(uint64_t& file_id, const double threshold, const int32_t max_hash_bucket);
        int read_file_info(common::FileInfoV2& info,  const double threshold, const int32_t max_hash_bucket,const uint64_t logic_block_id) const;
        int write_file_info(common::FileInfoV2& info, const double threshold, const int32_t max_hash_bucket,const uint64_t logic_block_id, const bool update);
        int write_file_infos(const common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const double threshold,
            const int32_t max_hash_bucket, const uint64_t logic_block_id,const bool partial, const int32_t reserved_space_ratio);
        int traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id) const;
        int traverse(std::vector<common::FileInfo>& infos, const uint64_t logic_block_id ) const;
        int get_attach_blocks(common::ArrayHelper<uint64_t>& blocks) const;
        int get_index_num(int32_t& index_num) const;
        int update_block_statistic_info(const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback = false);
        iterator begin();
        iterator end();
      protected:
        int remmap_(const double threshold, const int32_t max_hash_bucket, const int32_t advise_per_mmap_size = 0) const;
        common::FileInfoV2*  get_file_infos_array_() const;
      private:
        DISALLOW_COPY_AND_ASSIGN(IndexHandle);
    };

    // create verify index file. inner format:
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | index header|   inner index              |           single block index                                                                 |
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | IndexHeader | InnerIndex |...|InnerIndex | |{IndexHeader|common::FileInfoV2| ... | FileInofV2|}|{...}|{IndexHeader|common::FileInfoV2| ... | FileInofV2|}
    // -------------------------------------------------------------------------------------------------------------------------------------------
    class VerifyIndexHandle: public BaseIndexHandle
    {
      #ifdef TFS_GTEST
      friend class TestIndexHandle;
      FRIEND_TEST(TestIndexHandle, verify_write_read_infos);
      #endif
      friend class VerifyLogicBlock;
      struct InnerIndex
      {
        uint64_t logic_block_id_;
        int32_t offset_;
        int32_t size_;
      };
      public:
      VerifyIndexHandle(const std::string& path);
      virtual ~VerifyIndexHandle();
      int create(const uint64_t logic_block_id, const int64_t family_id, const int16_t index_num);
      int mmap(const uint64_t logic_block_id, const common::MMapOption& map_options);
      int write_file_info(common::FileInfoV2& info, const double threshold, const int32_t max_hash_bucket,
          const uint64_t logic_block_id, const bool update);
      int write_file_infos(const common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const double threshold,
          const int32_t max_hash_bucket,const uint64_t logic_block_id, const bool partial, const int32_t reserved_space_ratio);
      int read_file_info(common::FileInfoV2& info, const double threshold,
          const int32_t max_hash_bucket,const uint64_t logic_block_id) const;
      int traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id) const;
      int get_attach_blocks(common::ArrayHelper<uint64_t>& blocks) const;
      int get_index_num(int32_t& index_num) const;
      int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const;

      private:
      static const int32_t INDEX_DATA_START_OFFSET = sizeof(common::IndexHeaderV2) + common::MAX_MARSHALLING_NUM * sizeof(InnerIndex);
      InnerIndex* get_inner_index_array_() const;
      InnerIndex* get_inner_index_(const uint64_t logic_block_id) const;
      int read_file_info_(common::FileInfoV2*& info, const uint64_t fileid, char* buf, const int32_t nbytes, const int8_t type) const;
      int update_block_statistic_info_(char* data, const int32_t nbytes, const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback = false);

      int malloc_index_mem_(char*& data, InnerIndex& index) const;
      int free_index_mem_(const char* data, InnerIndex& index, const bool write_back) const;

      int remmap_(const double threshold, const int32_t max_hash_bucket, const int32_t advise_per_mmap_size = 0) const { UNUSED(max_hash_bucket);UNUSED(threshold);UNUSED(advise_per_mmap_size);return common::TFS_SUCCESS;}
      common::FileInfoV2*  get_file_infos_array_() const { return NULL;}
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif /* INDEX_HEADER_H_ */

