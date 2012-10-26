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
    // create index file. inner format:
    // -----------------------------------------------------
    // | index header|   FileInfoV2s                       |
    // ----------------------------------------------------
    // | IndexHeader | FileInfoV2|FileInfoV2|...|FileInfoV2|
    // -----------------------------------------------------
    IndexHandle::IndexHandle(const std::string& path):
      file_op_(path),
      is_load_(false)
    {

    }

    IndexHandle:: ~IndexHandle()
    {

    }

    int IndexHandle::flush()
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = file_op_.flush();
      }
      return ret;
    }

    int IndexHandle::read_file_info(FileInfoV2& info, const uint64_t file_id, const double threshold, const uint64_t logic_block_id ) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_FILE_ID != file_id) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        FileInfoV2* current = NULL;
        ret = get_file_info_(current, file_id, threshold, logic_block_id);
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != current);
          info = *current;
        }
      }
      return ret;
    }

    int IndexHandle::write_file_info(common::FileInfoV2& info, const uint64_t fileid, const double threshold, const bool override, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_FILE_ID != fileid) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = insert_file_info_(info, threshold, override, logic_block_id);
      }
      return ret;
    }

    int IndexHandle::write_file_infos(std::vector<common::FileInfoV2>& infos, const double threshold, const bool override, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        std::vector<common::FileInfoV2>::const_iterator iter = infos.begin();
        for (; iter != infos.end() && TFS_SUCCESS == ret; ++iter)
        {
          ret = insert_file_info_((*iter), threshold, override, logic_block_id);
        }
      }
      return ret;
    }

    int IndexHandle::del_file_info(const uint64_t fileid, const double threshold, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        uint64_t tmp_file_id = fileid;
        FileInfoV2* current = NULL, *prev = NULL;
        ret = get_slot_(slot, tmp_file_id, current, prev, threshold, true, logic_block_id);
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != current);
          memset(current, 0, sizeof(FileInfoV2));
          if ( NULL != prev)
          {
            prev->next_  = 0;
          }
        }
      }
      return ret;
    }

    int IndexHandle::traverse(std::vector<common::FileInfo>& infos, const bool sort, const uint64_t logic_block_id) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        FileInfoV2* finfos    = get_file_infos_array_();
        FileInfoV2* current   = NULL;
        for (int16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
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

      if (sort)
      {
        //TODO
      }
      return ret;
    }

    int IndexHandle::traverse(std::vector<common::FileInfoV2>& infos, const bool sort, const uint64_t logic_block_id) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        FileInfoV2* finfos    = get_file_infos_array_();
        FileInfoV2* current   = NULL;
        for (int16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
        {
          current = finfos + bucket;
          if (INVALID_FILE_ID != current->id_)
          {
            infos.push_back((*current));
          }
        }
      }

      if (sort)
      {
        //TODO
      }
      return ret;
    }

    int IndexHandle::update_write_visit_count(const int8_t step, const uint64_t logic_block_id )
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = step >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          header->throughput_.write_visit_count_ += step;
        }
      }
      return ret;
    }

    int IndexHandle::update_read_visit_count(const int8_t step, const uint64_t logic_block_id )
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = step >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          header->throughput_.read_visit_count_ += step;
        }
      }
      return ret;
    }

    int IndexHandle::update_block_info(const int32_t oper_type, const int32_t new_size, const int32_t old_size, const bool rollback, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = new_size >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret && !rollback)
        {
          ret = update_block_version(VERSION_INC_STEP_DEFAULT, logic_block_id);
        }
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          int32_t file_count = rollback ? -1 : 1;
          int32_t real_new_size = rollback ? -new_size : new_size;
          int32_t real_old_size = rollback ? -old_size : old_size;

          if (OPER_INSERT == oper_type)
          {
            header->info_.file_count_ += file_count;
            header->info_.size_  -= real_new_size;
            header->used_offset_ -= real_new_size;
          }

          if (OPER_DELETE == oper_type)
          {
            header->info_.del_file_count_ += file_count;
            header->info_.del_size_ += real_old_size;
          }
          if (OPER_UNDELETE == oper_type)
          {
            header->info_.del_file_count_ -= file_count;
            header->info_.del_size_       -= real_old_size;
          }
          if (OPER_UPDATE == oper_type)
          {
            header->info_.del_file_count_ += file_count;
            header->info_.file_count_     += file_count;;
            header->info_.del_size_       += real_old_size;
            header->info_.size_           += real_new_size;
            header->used_offset_          += real_new_size;
          }
        }
      }
      return ret;
    }

    int IndexHandle::update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->info_ = info;
      }
      return ret;
    }

    int IndexHandle::update_block_version(const int8_t step, const uint64_t logic_block_id )
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = step >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          header->info_.version_ += step;
        }
      }
      return ret;
    }

    int IndexHandle::get_block_info(BlockInfoV2& info, const uint64_t logic_block_id) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = index >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          info = header->info_;
        }
      }
      return ret;
    }

    int IndexHandle::check_block_version(common::BlockInfoV2& info, const int32_t remote_version, const int8_t index) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = index >= 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_();
          assert(NULL != header);
          info = header->info_;
          ret = (remote_version == info.version_) ? TFS_SUCCESS : EXIT_VERSION_CONFLICT_ERROR;
        }
      }
      return ret;
    }

    int IndexHandle::update_used_offset(const int32_t size, const uint64_t logic_block_id )
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          header->used_offset_ += size;
        }
      }
      return ret;
    }

    int IndexHandle::get_used_offset(int32_t& offset, const uint64_t logic_block_id ) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        offset = header->used_offset_;
      }
      return ret;
    }

    int IndexHandle::update_avail_offset(const int32_t size, const uint64_t logic_block_id )
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = size > 0 ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          header->avail_offset_ += size;
        }
      }
      return ret;
    }

    int IndexHandle::get_avail_offset(int32_t& offset, const uint64_t logic_block_id ) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        offset = header->avail_offset_;
      }
      return ret;
    }

    int IndexHandle::get_family_id(int64_t& family_id, const uint64_t logic_block_id) const
    {
      family_id = INVALID_FAMILY_ID;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        family_id = header->family_id_;
      }
      return ret;
    }

    int IndexHandle::set_family_id(const int64_t family_id, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_BLOCK_ID != logic_block_id) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->family_id_ = family_id;
      }
      return ret;
    }

    BaseIndexHandle::iterator IndexHandle::begin(const uint64_t logic_block_id)
    {
      UNUSED(logic_block_id);
      iterator iter = NULL;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        iter = get_file_infos_array_();
        assert(NULL != iter);
      }
      return iter;
    }

    BaseIndexHandle::iterator IndexHandle::end(const uint64_t logic_block_id)
    {
      UNUSED(logic_block_id);
      iterator iter = NULL;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        iter = get_file_infos_array_();
        assert(NULL != iter);
        iter += header->file_info_bucket_size_;
      }
      return iter;
    }

    int IndexHandle::inc_write_visit_count(const int32_t step, const int32_t nbytes, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->throughput_.write_visit_count_+= step;
        header->throughput_.write_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::inc_read_visit_count(const int32_t step, const int32_t nbytes, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->throughput_.read_visit_count_+= step;
        header->throughput_.read_bytes_ += nbytes;
      }
      return ret;

    }

    int IndexHandle::inc_update_visit_count(const int32_t step, const int32_t nbytes, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->throughput_.update_visit_count_+= step;
        header->throughput_.update_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::inc_unlink_visit_count(const int32_t step, const int32_t nbytes, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        header->throughput_.unlink_visit_count_ += step;
        header->throughput_.unlink_bytes_ += nbytes;
      }
      return ret;
    }

    int IndexHandle::statistic_visit(ThroughputV2& throughput, const bool reset, const uint64_t logic_block_id)
    {
      memset(&throughput, 0, sizeof(throughput));
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        throughput = header->throughput_;
        if (reset)
        {
          memset(&header->throughput_, 0, sizeof(header->throughput_));
          header->throughput_.last_update_time_ = time(NULL);
          header->throughput_.last_statistics_time_ = header->throughput_.last_update_time_;
        }
      }
      return ret;
    }

    int IndexHandle::rename_filename(const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
        assert(NULL != header);
        ret = (header->info_.block_id_ == logic_block_id) ? TFS_SUCCESS : EXIT_BLOCKID_CONFLICT_ERROR;
        if (TFS_SUCCESS == ret)
        {
          ret = file_op_.rename();
        }
      }
      return ret;
    }


    int IndexHandle::remove_self(const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_(logic_block_id);
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

    int IndexHandle::get_file_info_(FileInfoV2*& info, const uint64_t file_id, const double threshold, const uint64_t logic_block_id) const
    {
      info = NULL;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        uint64_t tmp_file_id = file_id;
        FileInfoV2* prev = NULL;
        ret = get_slot_(slot, tmp_file_id, info, prev, threshold, true, logic_block_id);
      }
      return ret;
    }

    int IndexHandle::insert_file_info_(const common::FileInfoV2& info, const double threshold, const bool override, const uint64_t logic_block_id)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = (INVALID_FILE_ID != info.id_) ? TFS_SUCCESS :  EXIT_PARAMETER_ERROR;
      }
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        uint64_t tmp_file_id = info.id_;
        FileInfoV2* current = NULL, *prev = NULL;
        ret = get_slot_(slot, tmp_file_id, current, prev, threshold, override, logic_block_id);
        if (TFS_SUCCESS == ret)
        {
          assert(NULL != current);
          *current = info;
          if (NULL != prev)
          {
            prev->next_ = slot;
          }
        }
      }
      return ret;
    }

    int IndexHandle::get_slot_(uint16_t& slot, uint64_t& file_id, common::FileInfoV2*& current,
        common::FileInfoV2*& prev, const double threshold, const bool exist, const uint64_t logic_block_id) const
    {
      current = NULL;
      prev    = NULL;
      slot = 0;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        ret = exist ? INVALID_FILE_ID == file_id ? EXIT_INVALID_FILE_ID_ERROR : TFS_SUCCESS : TFS_SUCCESS;
        if (TFS_SUCCESS == ret)
        {
          IndexHeaderV2* header = get_index_header_(logic_block_id);
          assert(NULL != header);
          ret = remmap_(threshold);
          if (TFS_SUCCESS == ret)
          {
            int32_t retry_times =  exist ? 0 : 3;
            FileInfoV2* finfos  = get_file_infos_array_();
            assert(NULL != finfos);
            uint32_t key = (INVALID_FILE_ID == file_id) ? header->info_.seq_no_: file_id & 0xFFFFFFFF;//lower 32 bit
            do
            {
              slot = key % header->file_info_bucket_size_;
              prev = current =  (finfos + slot);
              if (exist)
                ret = (file_id == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
              else
                ret = (0 == current->offset_ && INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_INSERT_INDEX_SLOT_NOT_FOUND_ERROR;

              if (TFS_SUCCESS != ret)
              {
                do
                {
                  slot = (0 == current->next_) ? ++key % header->file_info_bucket_size_ : current->next_;
                  prev = current =  (finfos + slot);
                  if (exist)
                    ret = (file_id == current->id_) ? TFS_SUCCESS : EXIT_META_NOT_FOUND_ERROR;
                  else
                    ret = (0 == current->offset_ && INVALID_FILE_ID == current->id_) ? TFS_SUCCESS : EXIT_INSERT_INDEX_SLOT_NOT_FOUND_ERROR;
                }
                while (TFS_SUCCESS != ret
                      && INVALID_FILE_ID != current->id_
                      && 0 != current->next_);
                if (TFS_SUCCESS != ret && !exist)
                  ++key;
              }
            }
            while (TFS_SUCCESS != ret && retry_times-- > 0);

            if (TFS_SUCCESS == ret && INVALID_FILE_ID == file_id && !exist)
            {
              file_id <<= 32;
              file_id |= key;
            }
          }
        }
      }
      return ret;
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
          ret = file_size > 0 ? EXIT_INDEX_UNEXPECT_EXIST_ERROR : TFS_SUCCESS;
        }
        if (TFS_SUCCESS == ret)// file_size == 0
        {
          IndexHeaderV2 header;
          memset(&header, 0, sizeof(header));
          header.info_.block_id_ = logic_block_id;
          header.info_.version_  = 1;
          header.info_.seq_no_   = 1;
          header.family_id_     = INVALID_FAMILY_ID;
          header.throughput_.last_update_time_ = time(NULL);
          header.throughput_.last_statistics_time_ = header.throughput_.last_update_time_;
          header.file_info_bucket_size_ = max_bucket_size;
          file_size = sizeof(IndexHeaderV2) + max_bucket_size * sizeof(FileInfoV2);
          char* data = new char[file_size];
          memcpy(data, &header, sizeof(header));
          memset((data + sizeof(IndexHeaderV2)), 0, file_size - sizeof(IndexHeaderV2));
          int64_t length = file_op_.pwrite(data, file_size, 0);
          ret = (length == file_size) ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
          tbsys::gDeleteA(data);
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
        ret = is_load_ ? EXIT_INDEX_ALREADY_LOADED_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        file_size = file_op_.size();
        ret = file_size < 0 ? EXIT_FILE_OP_ERROR : TFS_SUCCESS;
      }
      if (TFS_SUCCESS == ret)
      {
        ret = (0 == file_size)  ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
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
        IndexHeaderV2* header = get_index_header_();
        assert(NULL !=header);
        ret = (0 == header->info_.block_id_) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;

        if (TFS_SUCCESS == ret)
        {
          ret = (logic_block_id == header->info_.block_id_) ? EXIT_BLOCKID_CONFLICT_ERROR : TFS_SUCCESS;
        }

        if (TFS_SUCCESS == ret)
        {
          ret = (header->file_info_bucket_size_ <= 0) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
        }

        if (TFS_SUCCESS == ret)
        {
          int64_t size = sizeof(IndexHeaderV2) + header->file_info_bucket_size_ * sizeof(FileInfoV2);
          ret = (file_size < size ) ? EXIT_INDEX_CORRUPT_ERROR : TFS_SUCCESS;
        }
      }
      if (TFS_SUCCESS == ret)
      {
        is_load_ = true;
      }
      TBSYS_LOG(INFO, "load index %s, ret: %d, block id : %"PRI64_PREFIX"u, file_size: %"PRI64_PREFIX"d",
        TFS_SUCCESS == ret ? "successful" : "failed", ret, logic_block_id, file_size);
      return ret;
    }

    int IndexHandle::generation_file_id(uint64_t& file_id, const double threshold)
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        uint16_t slot = 0;
        FileInfoV2* current = NULL, *prev = NULL;
        ret =get_slot_(slot, file_id, current, prev, threshold, false, INVALID_BLOCK_ID);
      }
      return ret;
    }

    FileInfoV2* IndexHandle::get_file_infos_array_() const
    {
      FileInfoV2* result = NULL;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* data = file_op_.get_data();
        if (NULL != data)
        {
          result = reinterpret_cast<FileInfoV2*>(data + sizeof(IndexHeaderV2));
        }
      }
      return result;
    }

    IndexHeaderV2* IndexHandle::get_index_header_(const uint64_t logic_block_id) const
    {
      UNUSED(logic_block_id);
      IndexHeaderV2* result = NULL;
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        char* data = file_op_.get_data();
        if (NULL != data)
          result = reinterpret_cast<IndexHeaderV2*>(data);
      }
      return result;
    }

    int IndexHandle::remmap_(const double threshold) const
    {
      int32_t ret = is_load_ ? TFS_SUCCESS : EXIT_INDEX_NOT_LOAD_ERROR;
      if (TFS_SUCCESS == ret)
      {
        IndexHeaderV2* header = get_index_header_();
        ret = (NULL != header) ? TFS_SUCCESS : EXIT_INDEX_HEADER_NOT_FOUND;
        if (TFS_SUCCESS == ret)
        {
          if (header->check_need_mremap(threshold))
          {
            int32_t old_length = file_op_.length();
            ret = file_op_.mremap();//只扩大，不减小
            if (TFS_SUCCESS == ret)
            {
              int32_t use_file_info_bucket = 0;
              int32_t new_length =  file_op_.length();
              char* new_data = new char[new_length];
              memset(new_data, 0, new_length);
              char* old_data = new char[old_length];
              memcpy(old_data, file_op_.get_data(), old_length);
              memcpy(new_data, old_data, sizeof(IndexHeaderV2));
              IndexHeaderV2* new_header = reinterpret_cast<IndexHeaderV2*>(new_data);
              new_header->file_info_bucket_size_ = ((new_length - old_length ) / sizeof(FileInfoV2)) + header->file_info_bucket_size_;
              FileInfoV2* old_buckets = reinterpret_cast<FileInfoV2*>(old_data + sizeof(IndexHeaderV2));
              FileInfoV2* new_buckets = reinterpret_cast<FileInfoV2*>(new_data + sizeof(IndexHeaderV2));
              for (int16_t bucket = 0; bucket < header->file_info_bucket_size_; ++bucket)
              {
                FileInfoV2* current = old_buckets + bucket;
                while (INVALID_FILE_ID != current->id_)
                {
                  ++use_file_info_bucket;
                  int16_t new_bucket = (current->id_ << 32) % new_header->file_info_bucket_size_;
                  int16_t next_bucket= current->next_;
                  *(new_buckets + new_bucket) = *current;
                  current->id_ = INVALID_FILE_ID;
                  current = old_buckets + next_bucket;
                }
              }
              assert(use_file_info_bucket == header->used_file_info_bucket_size_);
              int64_t length = file_op_.pwrite(new_data, new_length, 0);
              tbsys::gDeleteA(new_data);
              tbsys::gDeleteA(old_data);
              ret = length >= new_length ? TFS_SUCCESS : EXIT_WRITE_FILE_ERROR;
              if (TFS_SUCCESS == ret)
              {
                ret = file_op_.flush();
              }
            }
          }
        }
      }
      return ret;
    }
    // create verify index file. inner format:
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | index header|   inner index              |           single block index                                                                 |
    // -------------------------------------------------------------------------------------------------------------------------------------------
    // | IndexHeader | InnerIndex |...|InnerIndex | |{IndexHeader|FileInfoV2| ... | FileInofV2|}|{...}|{IndexHeader|FileInfoV2| ... | FileInofV2|}
    // -------------------------------------------------------------------------------------------------------------------------------------------
    /*VerifyIndexHandle::VerifyIndexHandle(const std::string& path)
    {

    }

    VerifyIndexHandle::~VerifyIndexHandle()
    {

    }*/
  }/** end namespace dataserver **/
}/** end namespace tfs **/
