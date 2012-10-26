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

namespace tfs
{
  namespace dataserver
  {
    class BaseIndexHandle
    {
      public:
        typedef common::FileInfoV2* iterator;
      public:
        BaseIndexHandle() {}
        virtual ~BaseIndexHandle() {}
      public:
        virtual int flush() = 0;
        virtual int read_file_info(common::FileInfoV2& info, const uint64_t file_id, const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const  = 0;
        virtual int write_file_info(common::FileInfoV2& info, const uint64_t fileid, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int write_file_infos(std::vector<common::FileInfoV2>& infos, const double threshold, const bool override, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int del_file_info(const uint64_t fileid, const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int traverse(std::vector<common::FileInfo>& infos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int traverse(std::vector<common::FileInfoV2>& infos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int update_write_visit_count(const int8_t step = 1, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int update_read_visit_count(const int8_t step = 1, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int update_block_info(const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const int8_t index) const = 0;
        virtual int update_used_offset(const int32_t size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int get_used_offset(int32_t& offset, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int update_avail_offset(const int32_t size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int get_avail_offset(int32_t& offset, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int get_family_id(int64_t& family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int set_family_id(const int64_t family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual iterator begin(const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual iterator end(const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual int statistic_visit(common::ThroughputV2& throughput, const bool reset = false,
            const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
      protected:
        virtual int get_file_info_(common::FileInfoV2*& info, const uint64_t file_id,
                const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int insert_file_info_(const common::FileInfoV2& info, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) = 0;
        virtual common::IndexHeaderV2* get_index_header_(const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
        virtual int get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
                common::FileInfoV2*& prev, const double threshold, const bool exist = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const = 0;
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
      public:
        explicit IndexHandle(const std::string& path);
        virtual ~IndexHandle();
        int create(const uint64_t logic_block_id, const int32_t max_bucket_size, const common::MMapOption& map_options);
        int mmap(const uint64_t logic_block_id, const common::MMapOption& map_options);
        int generation_file_id(uint64_t& file_id, const double threshold);
        int flush();
        int read_file_info(common::FileInfoV2& info, const uint64_t file_id, const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int write_file_info(common::FileInfoV2& info, const uint64_t fileid, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int write_file_infos(std::vector<common::FileInfoV2>& infos, const double threshold, const bool override, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int del_file_info(const uint64_t fileid, const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int traverse(std::vector<common::FileInfoV2>& infos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int traverse(std::vector<common::FileInfo>& infos, const bool sort, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int update_write_visit_count(const int8_t step = 1, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int update_read_visit_count(const int8_t step = 1, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int update_block_info(const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const int8_t index) const ;
        int update_used_offset(const int32_t size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int get_used_offset(int32_t& offset, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int update_avail_offset(const int32_t size, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int get_avail_offset(int32_t& offset, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int get_family_id(int64_t& family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int set_family_id(const int64_t family_id, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        iterator begin(const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        iterator end(const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_write_visit_count(const int32_t step = 1, const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_read_visit_count(const int32_t step = 1,  const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_update_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int inc_unlink_visit_count(const int32_t step = 1,const int32_t nbytes = 0, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int statistic_visit(common::ThroughputV2& throughput, const bool reset = false,
            const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        int rename_filename(const uint64_t logic_block_id);
        int remove_self(const uint64_t logic_block_id);
      protected:
        int get_file_info_(common::FileInfoV2*& info, const uint64_t file_id,
                const double threshold, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const ;
        int insert_file_info_(const common::FileInfoV2& info, const double threshold, const bool override = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID);
        common::IndexHeaderV2* get_index_header_(const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        common::FileInfoV2*    get_file_infos_array_() const;
        int get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
                common::FileInfoV2*& prev, const double threshold, const bool exist = false, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const ;
        int remmap_(const double threshold) const;
      private:
        DISALLOW_COPY_AND_ASSIGN(IndexHandle);
        mutable common::MMapFileOperation file_op_;
        bool is_load_;
    };
    // create verify index file. inner format:
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | index header|   inner index              |           single block index                                                                 |
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | IndexHeader | InnerIndex |...|InnerIndex | |{IndexHeader|common::FileInfoV2| ... | FileInofV2|}|{...}|{IndexHeader|common::FileInfoV2| ... | FileInofV2|}
    // -------------------------------------------------------------------------------------------------------------------------------------------
    /*class VerifyIndexHandle: public BaseIndexHandle
    {
      struct InnerIndex
      {
        uint64_t logic_block_id_;
        int32_t offset_;
        int32_t size_;
      };
      public:
      VerifyIndexHandle(const std::string& path);
      private:
      common::FileOperation file_op_;
    };*/
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif /* INDEX_HEADER_H_ */
