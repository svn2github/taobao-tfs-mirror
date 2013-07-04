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

#include "common/error_msg.h"
#include "ds_define.h"
#include "index_handlev2.h"

using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    int BaseIndexHandle::flush()
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = file_op_.flush();
      }
      return ret;
    }

    int BaseIndexHandle::update_block_info(const common::BlockInfoV2& info) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->info_ = info;
      }
      return ret;
    }

    int BaseIndexHandle::update_block_version(const int8_t step)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = step >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          header->info_.version_ += step;
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_block_info(BlockInfoV2& info) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        info = header->info_;
      }
      return ret;
    }

    int BaseIndexHandle::get_index_header(common::IndexHeaderV2& pheader) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        pheader = *header;
      }
      return ret;
    }

    int BaseIndexHandle::set_index_header(const common::IndexHeaderV2& pheader)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        *header = pheader;
      }
      return ret;
    }

    int BaseIndexHandle::check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const
    {
      UNUSED(logic_block_id);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        info = header->info_;
        ret = (remote_version == info.version_) ? TFS_SUCCESS : EXIT_BLOCK_VERSION_CONFLICT_ERROR;
      }
      return ret;
    }

    int BaseIndexHandle::update_used_offset(const int32_t size)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          header->used_offset_ += size;
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_used_offset(int32_t& offset) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        offset = header->used_offset_;
      }
      return ret;
    }

    int BaseIndexHandle::set_used_offset(const int32_t size)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          header->used_offset_ = size;
        }
      }
      return ret;
    }

    int BaseIndexHandle::update_avail_offset(const int32_t size)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          header->avail_offset_ += size;
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_avail_offset(int32_t& offset) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        offset = header->avail_offset_;
      }
      return ret;
    }

    int BaseIndexHandle::get_marshalling_offset(int32_t& offset) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        offset = header->marshalling_offset_;
      }
      return ret;
    }

    int BaseIndexHandle::set_marshalling_offset(const int32_t size)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          header->marshalling_offset_ = size;
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_family_id(int64_t& family_id) const
    {
      family_id = INVALID_FAMILY_ID;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        family_id = header->info_.family_id_;
      }
      return ret;
    }

    int BaseIndexHandle::set_family_id(const int64_t family_id)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->info_.family_id_ = family_id;
      }
      return ret;
    }

    int BaseIndexHandle::rename_filename(const uint64_t logic_block_id)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        ret = (header->info_.block_id_ == logic_block_id) ? TFS_SUCCESS : EXIT_BLOCKID_CONFLICT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = file_op_.rename();
        }
      }
      return ret;
    }

    int BaseIndexHandle::remove_self(const uint64_t logic_block_id)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        ret = (header->info_.block_id_ == logic_block_id) ? TFS_SUCCESS : EXIT_BLOCKID_CONFLICT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = file_op_.munmap();
        }
        if (TFS_SUCCESS == ret)
        {
          ret = 0 == ::unlink(file_op_.get_path().c_str()) ? TFS_SUCCESS : -errno;
        }
      }
      return ret;
    }

    int BaseIndexHandle::check_load() const
    {
      return is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
    }

    IndexHeaderV2* BaseIndexHandle::get_index_header_() const
    {
      return TFS_SUCCESS == check_load() ? reinterpret_cast<IndexHeaderV2*>(file_op_.get_data()) : NULL;
    }

    int BaseIndexHandle::update_block_statistic_info_(common::IndexHeaderV2* header,
        const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (new_size >= 0  && NULL != header) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret && !rollback)
        {
          header->info_.version_ += VERSION_INC_STEP_DEFAULT;
        }
        if (TFS_SUCCESS == ret)
        {
          header->info_.last_update_time_  = time(NULL);
          int32_t file_count = rollback ? -1 : 1;
          int32_t real_new_size = rollback ? 0 - new_size : new_size;
          int32_t real_old_size = rollback ? 0 - old_size : old_size;
          if (OPER_INSERT == oper_type)
          {
            header->info_.file_count_ += file_count;
            header->info_.size_       += real_new_size;
            header->throughput_.write_visit_count_ += file_count;
            header->throughput_.write_bytes_ += real_new_size;
          }

          if (OPER_DELETE == oper_type)
          {
            header->info_.del_file_count_ += file_count;
            header->info_.del_size_       += real_old_size;
            header->throughput_.unlink_visit_count_ += file_count;
            header->throughput_.unlink_bytes_ += real_old_size;
          }
          if (OPER_UNDELETE == oper_type)
          {
            header->info_.del_file_count_ -= file_count;
            header->info_.del_size_       -= real_old_size;
            header->throughput_.unlink_visit_count_ -= file_count;
            header->throughput_.unlink_bytes_ -= real_old_size;
          }
          if (OPER_UPDATE == oper_type)
          {
            header->info_.del_file_count_ += file_count;
            header->info_.file_count_     += file_count;;
            header->info_.del_size_       += real_old_size;
            header->info_.size_           += real_new_size;
            header->info_.update_size_    += real_new_size;
            header->info_.update_file_count_ += file_count;
            header->throughput_.unlink_visit_count_ += file_count;
            header->throughput_.unlink_bytes_ += real_old_size;
            header->throughput_.write_visit_count_ += file_count;
            header->throughput_.write_bytes_ += real_new_size;
            header->throughput_.update_bytes_ += real_new_size;
            header->throughput_.update_visit_count_ += file_count;
          }
        }
      }
      return ret;
    }

    void BaseIndexHandle::get_prev_(common::FileInfoV2* prev, common::FileInfoV2* current, bool complete) const
    {
      assert(NULL != current);
      if (0 != current->next_ && !complete)
        prev = current;
    }

    int BaseIndexHandle::get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
        common::FileInfoV2*& prev, common::FileInfoV2* finfos, common::IndexHeaderV2* header,
        const int8_t type) const
    {
      slot = 0, current = NULL, prev = NULL;
      int32_t ret = (NULL != finfos && NULL != header) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        int32_t max_loop = header->file_info_bucket_size_;
        if (GET_SLOT_TYPE_GEN == type)
        {
          prev = NULL;
          assert(INVALID_FILE_ID == file_id);
          //slot = ++header->seq_no_ % header->file_info_bucket_size_;
          slot = (++header->seq_no_ % (header->file_info_bucket_size_ - 1) ) + 1;
          current =  (finfos + slot);
          ret = (0 == current->offset_ && INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_INSERT_INDEX_SLOT_NOT_FOUND_ERROR;
          while (TFS_SUCCESS != ret && max_loop-- > 0)
          {
            //slot = ++header->seq_no_ % header->file_info_bucket_size_;
            slot = (++header->seq_no_ % (header->file_info_bucket_size_ - 1) ) + 1;
            current =  (finfos + slot);
            ret = (0 == current->offset_ && INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_INSERT_INDEX_SLOT_NOT_FOUND_ERROR;
          }
          if (TFS_SUCCESS == ret)
          {
            file_id = header->seq_no_;
          }
        }

        if (GET_SLOT_TYPE_QUERY  == type)
        {
          prev = NULL;
          assert(INVALID_FILE_ID != file_id);
          uint32_t key = file_id & 0xFFFFFFFF;//lower 32 bit
          slot = (key % (header->file_info_bucket_size_ - 1)) + 1;
          //slot = key % header->file_info_bucket_size_;
          current =  (finfos + slot);
          ret = (file_id == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
          while (TFS_SUCCESS != ret && 0 != current->next_ && max_loop -- > 0)
          {
            slot = current->next_;
            current =  (finfos + slot);
            ret = (file_id == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
          }
        }

        if (GET_SLOT_TYPE_INSERT == type)
        {
          prev = NULL;
          assert(INVALID_FILE_ID != file_id);
          uint32_t key = file_id & 0xFFFFFFFF;//lower 32 bit
          //slot = key % header->file_info_bucket_size_;
          slot = (key % (header->file_info_bucket_size_ - 1)) + 1;
          current =  (finfos + slot);
          ret = (INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
          while (TFS_SUCCESS != ret && 0 != current->next_)
          {
            slot = current->next_;
            current =  (finfos + slot);
          }
          //ret = (INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
          if (TFS_SUCCESS != ret)
            prev = current;
          while (TFS_SUCCESS != ret && max_loop-- > 0 && header->used_file_info_bucket_size_ < header->file_info_bucket_size_)
          {
            slot = ((key + random()) % (header->file_info_bucket_size_ - 1)) + 1;
            //slot = (key + random()) % header->file_info_bucket_size_;
            current =  (finfos + slot);
            ret = (INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
          }
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
        common::FileInfoV2*& prev, const char* buf, const int32_t nbytes, const int8_t type) const
    {
      slot = 0, current = NULL, prev = NULL;
      int32_t ret = (NULL != buf && nbytes > INDEX_HEADER_V2_LENGTH) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* data = const_cast<char*>(buf);
        IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
        assert(NULL != header);
        FileInfoV2* finfos  = reinterpret_cast<FileInfoV2*>(data + INDEX_HEADER_V2_LENGTH);
        assert(NULL != finfos);
        int32_t total = header->used_file_info_bucket_size_ * FILE_INFO_V2_LENGTH + INDEX_HEADER_V2_LENGTH;
        ret = (nbytes >= total) ? TFS_SUCCESS : EXIT_INDEX_DATA_INVALID_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = get_slot_(slot, file_id, current, prev, finfos, header, type);
        }
      }
      return ret;
    }

    int BaseIndexHandle::get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
        common::FileInfoV2*& prev, const double threshold, const int32_t max_hash_bucket,const int8_t type) const
    {
      slot = 0, current = NULL, prev = NULL;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        ret = GET_SLOT_TYPE_QUERY != type ? remmap_(threshold, max_hash_bucket) : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          header  = get_index_header_();
          FileInfoV2* finfos  = get_file_infos_array_();
          assert(NULL != finfos);
          ret = get_slot_(slot, file_id, current, prev, finfos, header, type);
        }
      }
      return ret;
    }

    int BaseIndexHandle::insert_file_info_(common::FileInfoV2& info, const double threshold,
        const int32_t max_hash_bucket,const bool update)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        if (!update)
          info.next_ = 0;
        uint16_t slot = 0;
        FileInfoV2* current = NULL, *prev = NULL;
        ret = get_slot_(slot, info.id_, current, prev, threshold, max_hash_bucket, update ? GET_SLOT_TYPE_QUERY : GET_SLOT_TYPE_INSERT);
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          assert(NULL != current);
          *current = info;
          if (!update)
          {
            ++header->used_file_info_bucket_size_;
            if (NULL != prev)
              prev->next_ = slot;
          }
        }
      }
      return ret;
    }

    int BaseIndexHandle::insert_file_info_(common::FileInfoV2& info, char* buf, const int32_t nbytes, const bool update) const
    {
      int32_t ret = (NULL != buf && nbytes > 0) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        if (!update)
          info.next_ = 0;
        uint16_t slot = 0;
        FileInfoV2* current = NULL, *prev = NULL;
        ret = get_slot_(slot, info.id_, current, prev, buf, nbytes, update ? GET_SLOT_TYPE_QUERY : GET_SLOT_TYPE_INSERT);
        if (TFS_SUCCESS == ret)
        {
          char* data = const_cast<char*>(buf);
          IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
          assert(NULL != header);
          assert(NULL != current);
          *current = info;
          if (!update)
          {
            ++header->used_file_info_bucket_size_;
            if (NULL != prev)
              prev->next_ = slot;
          }
        }
      }
      return ret;
    }

    // create index file. inner format:
    // -----------------------------------------------------
    // | index header|   FileInfoV2s                       |
    // ----------------------------------------------------
    // | IndexHeader | FileInfoV2|FileInfoV2|...|FileInfoV2|
    // -----------------------------------------------------
    IndexHandle::IndexHandle(const std::string& path):
      BaseIndexHandle(path)
    {

    }

    IndexHandle:: ~IndexHandle()
    {

    }

    int IndexHandle::create(const uint64_t logic_block_id, const int32_t max_bucket_size, const common::MMapOption& options)
    {
      int64_t file_size = 0;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && max_bucket_size > 0 &&  options.check()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = is_load_ ? EXIT_INDEX_ALREADY_LOADED_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        file_size = file_op_.size();
        ret = file_size < 0 ? EXIT_FILE_OP_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (file_size > 0)
            TBSYS_LOG(WARN, "index file %s maybe reuse!", file_op_.get_path().c_str());
          if (0 == file_size)
          {
            IndexHeaderV2 header;
            memset(&header, 0, sizeof(header));
            header.info_.block_id_ = logic_block_id;
            header.info_.family_id_= INVALID_FAMILY_ID;
            header.info_.last_update_time_ = time(NULL);
            header.throughput_.last_statistics_time_ = header.info_.last_update_time_;
            header.file_info_bucket_size_ = max_bucket_size;
            file_size = INDEX_HEADER_V2_LENGTH + max_bucket_size * FILE_INFO_V2_LENGTH;
            char* data = new (std::nothrow)char[file_size];
            memcpy(data, &header, sizeof(header));
            memset((data + INDEX_HEADER_V2_LENGTH), 0, file_size - INDEX_HEADER_V2_LENGTH);
            int64_t length = file_op_.pwrite(data, file_size, 0);
            ret = (length == file_size) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
            if (TFS_SUCCESS == ret)
              ret = file_op_.fdatasync();
            tbsys::gDeleteA(data);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          ret = file_op_.mmap(options);
        }

        if (TFS_SUCCESS == ret)
        {
          is_load_ = true;
        }
      }
      TBSYS_LOG(INFO, "create index %s, ret: %d, block id : %"PRI64_PREFIX"u, max_bucket_size: %d, file_size: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "successful" : "failed", ret, logic_block_id, max_bucket_size, file_size);
      return ret;
    }

    int IndexHandle::mmap(const uint64_t logic_block_id, const common::MMapOption& options)
    {
      int64_t file_size = 0;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id &&  options.check()) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = is_load_ ? EXIT_INDEX_NOT_LOAD_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        file_size = file_op_.size();
        ret = file_size < 0 ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        MMapOption option = options;
        if (file_size > option.first_mmap_size_)
          option.first_mmap_size_ = file_size;
        if (file_size > option.max_mmap_size_)
          option.max_mmap_size_ = file_size;
        ret = file_op_.mmap(option);
      }

      if (TFS_SUCCESS == ret)
      {
        is_load_ = true;
        /*IndexHeaderV2* header = get_index_header_();
        assert(NULL !=header);
        ret = (INVALID_BLOCK_ID == header->info_.block_id_
            || logic_block_id != header->info_.block_id_
            || header->file_info_bucket_size_ <= 0) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          int64_t size = INDEX_HEADER_V2_LENGTH + header->file_info_bucket_size_ * FILE_INFO_V2_LENGTH;
          ret = (file_size < size ) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
        }*/
      }
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        header->avail_offset_ = 0;
      }
      is_load_ = TFS_SUCCESS == ret;
      TBSYS_LOG(INFO, "load index %s, ret: %d, block id : %"PRI64_PREFIX"u, file_size: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "successful" : "failed", ret, logic_block_id, file_size);
      return ret;
    }

    int IndexHandle::generation_file_id(uint64_t& file_id, const double threshold, const int32_t max_hash_bucket)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        FileInfoV2* current = NULL, *prev = NULL;
        ret =get_slot_(slot, file_id, current, prev, threshold, max_hash_bucket, GET_SLOT_TYPE_GEN);
      }
      return ret;
    }

    int IndexHandle::read_file_info(FileInfoV2& info, const double threshold, const int32_t max_hash_bucket, const uint64_t logic_block_id ) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_FILE_ID != info.id_ && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        FileInfoV2* prev = NULL, *current = NULL;
        ret = get_slot_(slot, info.id_, current, prev, threshold, max_hash_bucket, GET_SLOT_TYPE_QUERY);
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != current);
          info = (*current);
        }
      }
      return ret;
    }

    int IndexHandle::write_file_info(common::FileInfoV2& info, const double threshold,
          const int32_t max_hash_bucket, const uint64_t logic_block_id, const bool update)
    {
      UNUSED(logic_block_id);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = insert_file_info_(info, threshold, max_hash_bucket, update);
      }
      return ret;
    }

    int IndexHandle::write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos,
        const double threshold, const int32_t max_hash_bucket,const uint64_t logic_block_id, const bool partial)
    {
      UNUSED(logic_block_id);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        if (partial)
        {
          // only used in recover updated block
          // only update partial header information
          IndexHeaderV2* pheader = get_index_header_();
          assert(NULL != pheader);
          pheader->info_ = header.info_;
          pheader->throughput_ = header.throughput_;
          pheader->marshalling_offset_ = header.marshalling_offset_;
          pheader->seq_no_ = header.seq_no_;
        }
        else
        {
          IndexHeaderV2* pheader = get_index_header_();
          assert(NULL != pheader);
          const int32_t file_info_bucket_size = pheader->file_info_bucket_size_;
          pheader->info_ = header.info_;
          pheader->seq_no_ = header.seq_no_;
          pheader->marshalling_offset_ = header.marshalling_offset_;
          pheader->used_file_info_bucket_size_ = 0;
          pheader->file_info_bucket_size_ = file_info_bucket_size;
          std::vector<common::FileInfoV2>::iterator iter = infos.begin();
          for (; iter != infos.end() && TFS_SUCCESS == ret; ++iter)
          {
            FileInfoV2& finfo = (*iter);
            ret = insert_file_info_(finfo, threshold, max_hash_bucket, false);
          }
        }
      }
      return ret;
    }

    int IndexHandle::traverse(common::IndexHeaderV2& iheader, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id) const
    {
      UNUSED(logic_block_id);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        iheader = *header;
        FileInfoV2* finfos    = get_file_infos_array_();
        for (uint16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
        {
          FileInfoV2* current = finfos + bucket;
          if (INVALID_FILE_ID != current->id_)
            infos.push_back((*current));
        }
      }
      return ret;
    }

    int IndexHandle::traverse(std::vector<common::FileInfo>& infos, const uint64_t logic_block_id) const
    {
      UNUSED(logic_block_id);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        FileInfoV2* finfos    = get_file_infos_array_();
        FileInfoV2* current   = NULL;
        for (uint16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
        {
          current = finfos + bucket;
          if (INVALID_FILE_ID != current->id_)
          {
            infos.push_back(FileInfo());
            std::vector<FileInfo>::iterator iter = infos.end() - 1;
            (*iter).id_     = current->id_;
            (*iter).offset_ = current->offset_;
            (*iter).size_   = current->size_;
            (*iter).modify_time_ = current->modify_time_;
            (*iter).create_time_ = current->create_time_;
            (*iter).flag_   = current->status_;
            (*iter).crc_    = current->crc_;
            (*iter).usize_  = current->size_;
          }
        }
      }
      return ret;
    }

    int IndexHandle::get_attach_blocks(ArrayHelper<uint64_t>& blocks) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        blocks.push_back(header->info_.block_id_);
      }
      return ret;
    }

    int IndexHandle::get_index_num(int32_t& index_num) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* pheader = get_index_header_();
        assert(NULL != pheader);
        index_num = 0;
      }
      return ret;
    }

    int IndexHandle::update_block_statistic_info(const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = new_size >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          ret = update_block_statistic_info_(header, oper_type, new_size, old_size, rollback);
          if (TFS_SUCCESS == ret)
          {
            int32_t real_new_size = rollback ? 0 - new_size : new_size;
            if ((OPER_INSERT == oper_type) || (OPER_UPDATE == oper_type))
            {
              header->used_offset_ += real_new_size;
            }
          }
        }
      }
      return ret;
    }

    BaseIndexHandle::iterator IndexHandle::begin()
    {
      iterator iter = NULL;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        iter = get_file_infos_array_();
        assert(NULL != iter);
      }
      return iter;
    }

    BaseIndexHandle::iterator IndexHandle::end()
    {
      iterator iter = NULL;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        iter = get_file_infos_array_();
        assert(NULL != iter);
        iter += header->file_info_bucket_size_;
      }
      return iter;
    }

    int IndexHandle::inc_write_visit_count(const int32_t step, const int32_t nbytes)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->throughput_.write_visit_count_+= step;
        header->throughput_.write_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::inc_read_visit_count(const int32_t step, const int32_t nbytes)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->throughput_.read_visit_count_+= step;
        header->throughput_.read_bytes_ += nbytes;
      }
      return ret;

    }

    int IndexHandle::inc_update_visit_count(const int32_t step, const int32_t nbytes)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->throughput_.update_visit_count_+= step;
        header->throughput_.update_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::inc_unlink_visit_count(const int32_t step, const int32_t nbytes)
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        header->throughput_.unlink_visit_count_ += step;
        header->throughput_.unlink_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::statistic_visit(ThroughputV2& throughput, const bool reset)
    {
      memset(&throughput, 0, sizeof(throughput));
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        assert(NULL != header);
        throughput = header->throughput_;
        if (reset)
        {
          memset(&header->throughput_, 0, sizeof(header->throughput_));
          header->info_.last_update_time_ = time(NULL);
          header->throughput_.last_statistics_time_ = header->info_.last_update_time_;
        }
      }
      return ret;
    }

    int IndexHandle::remmap_(const double threshold, const int32_t max_hash_bucket) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        ret = (NULL != header) ? TFS_SUCCESS : EXIT_INDEX_HEADER_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          if (header->check_need_mremap(threshold) && header->file_info_bucket_size_ < max_hash_bucket)
          {
            int32_t old_length = file_op_.length();
            char* old_data = new (std::nothrow)char[old_length];
            memcpy(old_data, file_op_.get_data(), old_length);
            header = reinterpret_cast<IndexHeaderV2*>(old_data);
            ret = file_op_.mremap();//只扩大，不减小
            if (TFS_SUCCESS == ret)
            {
              int32_t new_length =  file_op_.length();
              char* new_data = file_op_.get_data();
              memset(new_data, 0, new_length);
              memcpy(new_data, old_data, INDEX_HEADER_V2_LENGTH);
              IndexHeaderV2* new_header = reinterpret_cast<IndexHeaderV2*>(new_data);
              new_header->used_file_info_bucket_size_ = 0;
              new_header->file_info_bucket_size_ = std::min(((new_length - INDEX_HEADER_V2_LENGTH) / FILE_INFO_V2_LENGTH), max_hash_bucket);
              FileInfoV2* old_buckets = reinterpret_cast<FileInfoV2*>(old_data + INDEX_HEADER_V2_LENGTH);
              TBSYS_LOG(INFO, "index file %s remmap, old_length: %d, bucket_size: %d, used bucket size: %d, new_length: %d, new_bucket size: %d, used bucket size: %u, block id: %"PRI64_PREFIX"u",
                  file_op_.get_path().c_str(), old_length, header->file_info_bucket_size_, header->used_file_info_bucket_size_,
                  new_length, new_header->file_info_bucket_size_, new_header->used_file_info_bucket_size_, new_header->info_.block_id_);
              for (uint16_t bucket = 0; bucket < header->file_info_bucket_size_ && TFS_SUCCESS == ret; ++bucket)
              {
                FileInfoV2* current = (old_buckets + bucket);
                if (current->id_ != INVALID_FILE_ID)
                {
                  ret = insert_file_info_(*current, new_data, new_length, false);
                }
              }
              assert(new_header->used_file_info_bucket_size_ == header->used_file_info_bucket_size_);
              ret = file_op_.flush();
            }
            tbsys::gDeleteA(old_data);
          }
        }
      }
      return ret;
    }

    FileInfoV2* IndexHandle::get_file_infos_array_() const
    {
      return (TFS_SUCCESS == check_load()) ? reinterpret_cast<FileInfoV2*>(file_op_.get_data() + INDEX_HEADER_V2_LENGTH) : NULL;
    }

    // create verify index file. inner format:
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | index header|   inner index              |           single block index                                                                 |
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | IndexHeader | InnerIndex |...|InnerIndex | |{IndexHeader|FileInfoV2| ... | FileInofV2|}|{...}|{IndexHeader|FileInfoV2| ... | FileInofV2|}
    // -------------------------------------------------------------------------------------------------------------------------------------------
    VerifyIndexHandle::VerifyIndexHandle(const std::string& path):
      BaseIndexHandle(path)
    {

    }

    VerifyIndexHandle::~VerifyIndexHandle()
    {

    }

    int VerifyIndexHandle::create(const uint64_t logic_block_id, const int64_t family_id, const int16_t index_num)
    {
      int64_t file_size = 0;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id && index_num > 0) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = is_load_ ? EXIT_INDEX_ALREADY_LOADED_ERROR : TFS_SUCCESS;
      }

      if (TFS_SUCCESS == ret)
      {
        file_size = file_op_.size();
        ret = file_size < 0 ? EXIT_FILE_OP_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          if (file_size > 0)
            TBSYS_LOG(WARN, "index file %s mybe reuse!", file_op_.get_path().c_str());
          if (0 == file_size)
          {
            IndexHeaderV2 header;
            memset(&header, 0, sizeof(header));
            header.info_.block_id_ = logic_block_id;
            header.seq_no_   = 0;
            header.info_.family_id_ = family_id;
            header.info_.last_update_time_ = time(NULL);
            header.throughput_.last_statistics_time_ = header.info_.last_update_time_;
            header.max_index_num_  = index_num;
            file_size = INDEX_HEADER_V2_LENGTH + header.max_index_num_ * sizeof(InnerIndex);
            char* data = new (std::nothrow)char[file_size];
            memcpy(data, &header, sizeof(header));
            memset((data + INDEX_HEADER_V2_LENGTH), 0, file_size - INDEX_HEADER_V2_LENGTH);
            int64_t length = file_op_.pwrite(data, file_size, 0);
            ret = (length == file_size) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
            if (TFS_SUCCESS == ret)
              ret = file_op_.fdatasync();
            tbsys::gDeleteA(data);
          }
        }

        if (TFS_SUCCESS == ret)
        {
          MMapOption option;
          option.first_mmap_size_ = INDEX_DATA_START_OFFSET;
          option.per_mmap_size_   = INDEX_DATA_START_OFFSET;
          option.max_mmap_size_   = INDEX_DATA_START_OFFSET;
          ret = file_op_.mmap(option);
        }

        if (TFS_SUCCESS == ret)
        {
          is_load_ = true;
        }
      }
      TBSYS_LOG(INFO, "create verify block index %s, ret: %d, block id : %"PRI64_PREFIX"u, file_size: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "successful" : "failed", ret, logic_block_id, file_size);
      return ret;
    }

    int VerifyIndexHandle::mmap(const uint64_t logic_block_id, const common::MMapOption& options)
    {
      UNUSED(options);
      int64_t file_size = 0;
      int32_t ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = is_load_ ? EXIT_INDEX_NOT_LOAD_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        file_size = file_op_.size();
        ret = file_size <= 0 ? EXIT_FILE_OP_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        MMapOption option;
        option.first_mmap_size_ = INDEX_DATA_START_OFFSET;
        option.per_mmap_size_   = INDEX_DATA_START_OFFSET;
        option.max_mmap_size_   = INDEX_DATA_START_OFFSET;
        ret = file_op_.mmap(option);
      }

      if (TFS_SUCCESS == ret)
      {
        is_load_ = true;
        IndexHeaderV2* header = get_index_header_();
        assert(NULL !=header);
        header->avail_offset_ = 0;
        ret = (INVALID_BLOCK_ID  == header->info_.block_id_
            || logic_block_id != header->info_.block_id_
            || header->index_num_ <= 0) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
        is_load_ = TFS_SUCCESS == ret;
      }
      TBSYS_LOG(INFO, "load verify index %s, ret: %d, block id : %"PRI64_PREFIX"u, file_size: %"PRI64_PREFIX"d",
          TFS_SUCCESS == ret ? "successful" : "failed", ret, logic_block_id, file_size);
      return ret;
    }

    int VerifyIndexHandle::write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const double threshold,
        const int32_t max_hash_bucket,const uint64_t logic_block_id, const bool partial)
    {
      UNUSED(threshold);
      UNUSED(max_hash_bucket);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        InnerIndex* index = get_inner_index_(header.info_.block_id_);
        if (NULL == index)
        {
          assert(!partial);
          assert(!infos.empty());
          IndexHeaderV2* pheader = get_index_header_();
          InnerIndex* inner_index = get_inner_index_array_();
          assert(NULL != inner_index);
          InnerIndex* index = &inner_index[pheader->index_num_++];
          InnerIndex* last_index = (1 == pheader->index_num_) ? &inner_index[0] : &inner_index[pheader->index_num_ - 2];
          index->logic_block_id_ = logic_block_id;
          index->offset_ = (1 == pheader->index_num_) ? INDEX_DATA_START_OFFSET : last_index->offset_ + last_index->size_;
          index->size_ = INDEX_HEADER_V2_LENGTH + (header.file_info_bucket_size_ * FILE_INFO_V2_LENGTH);
          header.used_file_info_bucket_size_ = 0;
          char* data  = new (std::nothrow) char[index->size_];
          memset(data , 0, index->size_);
          memcpy(data, &header, INDEX_HEADER_V2_LENGTH);
          std::vector<common::FileInfoV2>::iterator iter = infos.begin();
          for (; iter != infos.end() && TFS_SUCCESS == ret; ++iter)
          {
            FileInfoV2& info = (*iter);
            ret = insert_file_info_(info, data, index->size_, false);
          }
          if (TFS_SUCCESS == ret)
          {
            ret = file_op_.pwrite(data, index->size_, index->offset_);
            ret = (index->size_ == ret) ? TFS_SUCCESS : ret;
          }
          if (TFS_SUCCESS == ret)
          {
            ret = file_op_.flush();
          }
          tbsys::gDeleteA(data);
        }
        else if (partial)
        {
          // only update partial header information
          char* data = NULL;
          ret = malloc_index_mem_(data, *index);
          if (TFS_SUCCESS == ret)
          {
            IndexHeaderV2* pheader = reinterpret_cast<IndexHeaderV2*>(data);
            assert(NULL != pheader);
            pheader->info_ = header.info_;
            pheader->throughput_ = header.throughput_;
            pheader->marshalling_offset_ = header.marshalling_offset_;
            pheader->seq_no_ = header.seq_no_;
            if (!infos.empty())
              TBSYS_LOG(WARN, "update verify header, file infos not empty!!!!");
          }
          if (NULL != data)
            free_index_mem_(data, *index, TFS_SUCCESS == ret);
        }
      }
      return ret;
    }

    int VerifyIndexHandle::write_file_info(common::FileInfoV2& info, const double threshold, const int32_t max_hash_bucket,
        const uint64_t logic_block_id, const bool update)
    {
      UNUSED(threshold);
      UNUSED(max_hash_bucket);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        InnerIndex inner_index;
        inner_index.logic_block_id_ = logic_block_id;
        ret = malloc_index_mem_(data, inner_index);
        if (TFS_SUCCESS == ret)
          ret = insert_file_info_(info, data, inner_index.size_, update);
        if (NULL != data)
          ret = free_index_mem_(data, inner_index, TFS_SUCCESS == ret);
      }
      return ret;
    }

    int VerifyIndexHandle::read_file_info(common::FileInfoV2& info, const double threshold, const int32_t max_hash_bucket,
        const uint64_t logic_block_id) const
    {
      UNUSED(threshold);
      UNUSED(max_hash_bucket);
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_FILE_ID != info.id_ && INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        InnerIndex inner_index;
        inner_index.logic_block_id_ = logic_block_id;
        ret = malloc_index_mem_(data, inner_index);
        if (TFS_SUCCESS == ret)
        {
          uint16_t slot = 0;
          FileInfoV2* current = NULL, *prev = NULL;
          ret = get_slot_(slot, info.id_, current, prev, data, inner_index.size_,GET_SLOT_TYPE_QUERY);
          if (TFS_SUCCESS == ret)
            info = *current;
        }
        if (NULL != data)
          free_index_mem_(data, inner_index, TFS_SUCCESS == ret);
      }
      return ret;
    }

    int VerifyIndexHandle::traverse(common::IndexHeaderV2& vheader, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        InnerIndex inner_index;
        inner_index.logic_block_id_ = logic_block_id;
        ret = malloc_index_mem_(data, inner_index);
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
          assert(NULL != header);
          vheader = *header;
          FileInfoV2* finfos  = reinterpret_cast<FileInfoV2*>(data + INDEX_HEADER_V2_LENGTH);
          assert(NULL != finfos);
          int32_t total = header->file_info_bucket_size_ * FILE_INFO_V2_LENGTH;
          ret = (inner_index.size_ >= total) ? TFS_SUCCESS : EXIT_INDEX_DATA_INVALID_ERROR;
          if (TFS_SUCCESS == ret)
          {
            for (uint16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
            {
              FileInfoV2* current = finfos + bucket;
              if (INVALID_FILE_ID != current->id_)
                infos.push_back((*current));
            }
          }
        }

        if (NULL != data)
          free_index_mem_(data, inner_index, false);
      }
      return ret;
    }

    int VerifyIndexHandle::get_attach_blocks(ArrayHelper<uint64_t>& blocks) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* pheader = get_index_header_();
        InnerIndex* inner_index = get_inner_index_array_();
        assert(NULL != inner_index);

        for (int32_t i = 0; i < pheader->index_num_; i++)
        {
          blocks.push_back((*(inner_index + i)).logic_block_id_);
        }
      }
      return ret;
    }

    int VerifyIndexHandle::get_index_num(int32_t& index_num) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* pheader = get_index_header_();
        assert(NULL != pheader);
        index_num = pheader->index_num_;
      }
      return ret;
    }

    int VerifyIndexHandle::check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const uint64_t logic_block_id) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        char* data = NULL;
        InnerIndex inner_index;
        inner_index.logic_block_id_ = logic_block_id;
        ret = malloc_index_mem_(data, inner_index);
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = reinterpret_cast<IndexHeaderV2*>(data);
          assert(NULL != data);
          info = header->info_;
          ret = (remote_version == info.version_) ? TFS_SUCCESS : EXIT_BLOCK_VERSION_CONFLICT_ERROR;
        }
        if (NULL != data)
          free_index_mem_(data, inner_index, false);
      }
      return ret;
    }

    VerifyIndexHandle::InnerIndex* VerifyIndexHandle::get_inner_index_array_() const
    {
      return TFS_SUCCESS == check_load() ? reinterpret_cast<InnerIndex*>(file_op_.get_data() + INDEX_HEADER_V2_LENGTH) : NULL;
    }

    VerifyIndexHandle::InnerIndex* VerifyIndexHandle::get_inner_index_(const uint64_t logic_block_id) const
    {
      InnerIndex* result = NULL;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header  = get_index_header_();
        InnerIndex* begin = get_inner_index_array_();
        InnerIndex* end   = begin + header->max_index_num_;
        assert(NULL != header);
        assert(NULL != begin);
        for (; begin != end && NULL == result; ++begin)
        {
          if (begin->logic_block_id_ == logic_block_id)
            result = begin;
        }
      }
      return result;
    }

    int VerifyIndexHandle::read_file_info_(common::FileInfoV2*& info, const uint64_t fileid, char* buf, const int32_t nbytes, const int8_t type) const
    {
      int32_t ret = (NULL != buf && nbytes > 0 && INVALID_FILE_ID != fileid) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        uint64_t tmp  = fileid;
        FileInfoV2 *prev = NULL;
        ret = get_slot_(slot, tmp, info, prev, buf, nbytes, type);
      }
      return ret;
    }

    int VerifyIndexHandle::update_block_statistic_info_(char* data, const int32_t nbytes, const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback)
    {
      int32_t ret = (NULL != data && nbytes > INDEX_HEADER_V2_LENGTH) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* main_header = get_index_header_();
        IndexHeaderV2* header =  reinterpret_cast<IndexHeaderV2*>(data);
        assert(NULL != header);
        ret = BaseIndexHandle::update_block_statistic_info_(header, oper_type, new_size, old_size, rollback);
        if (TFS_SUCCESS == ret)
        {
          int32_t real_new_size = rollback ? 0 - new_size : new_size;
          if ((OPER_INSERT == oper_type) || (OPER_UPDATE == oper_type))
          {
            main_header->used_offset_ += real_new_size;
          }
        }
      }
      return ret;
    }

    int VerifyIndexHandle::malloc_index_mem_(char*& data, InnerIndex& index) const
    {
      data = NULL;
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != index.logic_block_id_) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }

      InnerIndex* inner_index = NULL;
      if (TFS_SUCCESS == ret)
      {
        inner_index = get_inner_index_(index.logic_block_id_);
        ret = (NULL != inner_index) ? TFS_SUCCESS : EXIT_VERIFY_INDEX_BLOCK_NOT_FOUND_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        assert(NULL != inner_index);
        index = *inner_index;
        data  = new (std::nothrow)char[index.size_];
        assert(NULL != data);
        index.size_ = file_op_.pread(data, inner_index->size_, inner_index->offset_);
        ret = (index.size_ > 0 ) ? TFS_SUCCESS : index.size_;
      }
      return ret;
    }

    int VerifyIndexHandle::free_index_mem_(const char* data, InnerIndex& index, const bool write_back) const
    {
      int32_t ret = check_load();
      if (TFS_SUCCESS == ret)
      {
        ret = (NULL != data && index.size_ > 0 && INVALID_BLOCK_ID != index.logic_block_id_) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }

      if (TFS_SUCCESS == ret && write_back)
      {
        ret = file_op_.pwrite(data, index.size_, index.offset_);
        ret = (index.size_ == ret) ? TFS_SUCCESS : ret;
      }
      tbsys::gDeleteA(data);

      if (TFS_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "free index mem fail. block id: %"PRI64_PREFIX"u, size: %d, offset: %d",
            index.logic_block_id_, index.size_, index.offset_);
      }
      return ret;
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
